import asyncio
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, List

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from app.settings import settings

log = logging.getLogger(__name__)
app = FastAPI(title="Playwright Wrapper (Transparent Proxy)", version="1.0.0")


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _split_args_simple(s: str) -> List[str]:
    return [a for a in (s or "").split(" ") if a]


def _load_kube() -> bool:
    if os.getenv("KUBE_ENABLED", "true").strip().lower() in ("0", "false", "no"):
        log.info("Kubernetes disabled via KUBE_ENABLED=false")
        return False

    in_cluster = bool(os.getenv("KUBERNETES_SERVICE_HOST")) and bool(os.getenv("KUBERNETES_SERVICE_PORT"))

    try:
        if in_cluster:
            config.load_incluster_config()
            log.info("Loaded in-cluster Kubernetes config")
            return True

        config.load_kube_config()
        log.info("Loaded kubeconfig (out-of-cluster)")
        return True

    except Exception as e:
        if os.getenv("KUBE_REQUIRED", "false").strip().lower() in ("1", "true", "yes", "y", "on"):
            raise
        log.warning("Kubernetes config not available; continuing without kube. Error=%r", e)
        return False


def _require_kube():
    if not getattr(app.state, "kube_enabled", False):
        raise HTTPException(
            status_code=503,
            detail="Kubernetes is disabled/unavailable (KUBE_ENABLED=false or no K8s config).",
        )


def _ns() -> str:
    if settings.target_namespace:
        return settings.target_namespace
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="target_namespace is not set and serviceaccount namespace file is missing. "
                   "Set TARGET_NAMESPACE for out-of-cluster use.",
        )


def _auth(x_api_token: Optional[str]):
    if not x_api_token or x_api_token != settings.api_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


@dataclass
class Session:
    session_id: str
    namespace: str
    pod_name: str
    service_name: str
    target_base: str
    created_at: float = field(default_factory=lambda: time.time())
    last_activity_at: float = field(default_factory=lambda: time.time())
    active_streams: int = 0


SESSIONS: Dict[str, Session] = {}
SESSIONS_LOCK = asyncio.Lock()


@app.on_event("startup")
async def startup():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    app.state.kube_enabled = _load_kube()

    app.state.http = httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(20.0, connect=5.0),
    )
    app.state.gc_task = asyncio.create_task(_gc_loop())


@app.on_event("shutdown")
async def shutdown():
    try:
        await app.state.http.aclose()
    except Exception:
        pass
    try:
        app.state.gc_task.cancel()
    except Exception:
        pass


@app.get("/health")
def health():
    return {
        "ok": True,
        "kubeEnabled": bool(getattr(app.state, "kube_enabled", False)),
        "keepFailedResources": _env_bool("KEEP_FAILED_RESOURCES", False),
        "runAsNonRoot": _env_bool("RUN_AS_NON_ROOT", False),
        "image": settings.playwright_image,
        "port": settings.playwright_port,
        "serviceType": settings.service_type,
        "nodeAddress": settings.node_address,
    }


def _hop_by_hop_header(name: str) -> bool:
    h = name.lower()
    return h in {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }


async def _touch(session_id: str):
    async with SESSIONS_LOCK:
        s = SESSIONS.get(session_id)
        if s:
            s.last_activity_at = time.time()


def _k8s_delete_session(namespace: str, session_id: str) -> None:
    _require_kube()
    core = client.CoreV1Api()

    pod_name = f"pw-{session_id}"
    svc_name = f"pw-svc-{session_id}"

    try:
        core.delete_namespaced_service(name=svc_name, namespace=namespace)
    except ApiException as e:
        if e.status != 404:
            raise

    try:
        core.delete_namespaced_pod(name=pod_name, namespace=namespace, grace_period_seconds=0)
    except ApiException as e:
        if e.status != 404:
            raise


async def _k8s_create_pod_and_service(namespace: str, session_id: str) -> tuple[str, str]:
    _require_kube()
    core = client.CoreV1Api()

    pod_name = f"pw-{session_id}"
    svc_name = f"pw-svc-{session_id}"
    labels = {"app": "playwright-ephemeral", "sessionId": session_id}

    volume_mounts = []
    volumes = []

    if settings.enable_pvc_mount:
        volumes.append(
            client.V1Volume(
                name="artifacts",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=settings.pvc_name
                ),
            )
        )
        volume_mounts.append(client.V1VolumeMount(name="artifacts", mount_path=settings.pvc_mount_path))

    command_list = _split_args_simple(settings.playwright_command) if settings.playwright_command else None
    args_list = _split_args_simple(settings.playwright_args)

    container = client.V1Container(
        name="playwright",
        image=settings.playwright_image,
        image_pull_policy="IfNotPresent",
        command=command_list,
        args=args_list,
        ports=[client.V1ContainerPort(container_port=settings.playwright_port, name="mcp")],
        volume_mounts=volume_mounts or None,
        readiness_probe=client.V1Probe(
            tcp_socket=client.V1TCPSocketAction(port=settings.playwright_port),
            initial_delay_seconds=2,
            period_seconds=2,
            timeout_seconds=1,
            failure_threshold=30,
        ),
    )

    run_as_non_root = _env_bool("RUN_AS_NON_ROOT", False)
    pod_security_ctx = client.V1PodSecurityContext(run_as_non_root=True) if run_as_non_root else None

    pod_spec = client.V1PodSpec(
        containers=[container],
        restart_policy="Never",
        volumes=volumes or None,
        security_context=pod_security_ctx,
    )

    pod = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=pod_name, labels=labels),
        spec=pod_spec,
    )

    svc_type = (settings.service_type or "ClusterIP").strip()
    if svc_type not in ("ClusterIP", "NodePort"):
        raise HTTPException(status_code=500, detail=f"Unsupported SERVICE_TYPE: {svc_type}")

    svc_port = client.V1ServicePort(
        name="mcp",
        port=settings.playwright_port,
        target_port=settings.playwright_port,
    )

    svc = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=svc_name, labels=labels),
        spec=client.V1ServiceSpec(
            selector=labels,
            ports=[svc_port],
            type=svc_type,
        ),
    )

    core.create_namespaced_pod(namespace=namespace, body=pod)
    core.create_namespaced_service(namespace=namespace, body=svc)

    return pod_name, svc_name


async def _k8s_wait_pod_ready(namespace: str, session_id: str, timeout_s: int) -> None:
    _require_kube()
    core = client.CoreV1Api()
    pod_name = f"pw-{session_id}"

    deadline = time.time() + timeout_s
    last_phase = None
    last_reason = None

    while time.time() < deadline:
        pod = core.read_namespaced_pod(name=pod_name, namespace=namespace)
        last_phase = pod.status.phase

        cs = (pod.status.container_statuses or [])
        if cs and cs[0].state and cs[0].state.waiting:
            last_reason = cs[0].state.waiting.reason
        if cs and cs[0].state and cs[0].state.terminated:
            term = cs[0].state.terminated
            raise HTTPException(
                status_code=500,
                detail=f"Pod terminated early (phase={last_phase}, exitCode={term.exit_code}, reason={term.reason})",
            )

        conds = pod.status.conditions or []
        if any(c.type == "Ready" and c.status == "True" for c in conds):
            return

        await asyncio.sleep(1)

    detail = "Pod not ready in time"
    if last_phase or last_reason:
        detail += f" (lastPhase={last_phase}, lastReason={last_reason})"
    raise HTTPException(status_code=504, detail=detail)


def _k8s_get_nodeport(namespace: str, svc_name: str) -> int:
    _require_kube()
    core = client.CoreV1Api()
    svc = core.read_namespaced_service(name=svc_name, namespace=namespace)
    ports = (svc.spec.ports or [])
    if not ports:
        raise HTTPException(status_code=502, detail="Service has no ports")
    np = ports[0].node_port
    if not np:
        raise HTTPException(status_code=502, detail="Service NodePort not allocated yet")
    return int(np)


@app.post("/sessions")
async def create_session(x_api_token: Optional[str] = Header(default=None)):
    _auth(x_api_token)
    _require_kube()

    namespace = _ns()
    session_id = secrets.token_hex(8)
    keep_failed = _env_bool("KEEP_FAILED_RESOURCES", False)

    try:
        pod_name, svc_name = await _k8s_create_pod_and_service(namespace, session_id)
        await _k8s_wait_pod_ready(namespace, session_id, settings.pod_ready_timeout_seconds)

        svc_type = (settings.service_type or "ClusterIP").strip()
        if svc_type == "NodePort":
            node_port = _k8s_get_nodeport(namespace, svc_name)
            target_base = f"http://{settings.node_address}:{node_port}"
        else:
            target_base = f"http://{svc_name}.{namespace}.svc.cluster.local:{settings.playwright_port}"

    except HTTPException:
        if not keep_failed:
            try:
                _k8s_delete_session(namespace, session_id)
            except Exception:
                pass
        raise
    except ApiException as e:
        if not keep_failed:
            try:
                _k8s_delete_session(namespace, session_id)
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"K8s error: {e.reason}")

    async with SESSIONS_LOCK:
        SESSIONS[session_id] = Session(
            session_id=session_id,
            namespace=namespace,
            pod_name=pod_name,
            service_name=svc_name,
            target_base=target_base,
        )

    return {
        "sessionId": session_id,
        "namespace": namespace,
        "podName": pod_name,
        "serviceName": svc_name,
        "targetBase": target_base,
        "baseUrl": f"/sessions/{session_id}",
    }


@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str, x_api_token: Optional[str] = Header(default=None)):
    _auth(x_api_token)
    _require_kube()

    async with SESSIONS_LOCK:
        s = SESSIONS.pop(session_id, None)

    if not s:
        return {"ok": True, "sessionId": session_id, "deleted": False}

    try:
        _k8s_delete_session(s.namespace, session_id)
    except ApiException as e:
        raise HTTPException(status_code=500, detail=f"K8s delete error: {e.reason}")

    return {"ok": True, "sessionId": session_id, "deleted": True}


@app.api_route("/sessions/{session_id}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_any(session_id: str, path: str, request: Request, x_api_token: Optional[str] = Header(default=None)):
    _auth(x_api_token)

    async with SESSIONS_LOCK:
        s = SESSIONS.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Unknown session")

    await _touch(session_id)

    upstream_url = f"{s.target_base}/{path}"
    if request.url.query:
        upstream_url = f"{upstream_url}?{request.url.query}"

    headers = {}
    for k, v in request.headers.items():
        if _hop_by_hop_header(k):
            continue
        if k.lower() == "host":
            continue
        headers[k] = v

    body = await request.body()
    method = request.method.upper()

    accept = request.headers.get("accept", "")
    is_sse = "text/event-stream" in accept.lower()
    should_stream = is_sse

    try:
        if should_stream:
            async with SESSIONS_LOCK:
                ss = SESSIONS.get(session_id)
                if ss:
                    ss.active_streams += 1

            async with app.state.http.stream(method, upstream_url, headers=headers, content=body) as r:

                async def _stream_sse_text():
                    try:
                        async for chunk in r.aiter_text():
                            await _touch(session_id)
                            # IMPORTANT: StreamingResponse accepte str/bytes; str -> encodé proprement
                            yield chunk
                    except (httpx.StreamClosed, asyncio.CancelledError):
                        return
                    except Exception as e:
                        log.info("SSE stream ended with error: %r", e)
                        return
                    finally:
                        async with SESSIONS_LOCK:
                            ss2 = SESSIONS.get(session_id)
                            if ss2 and ss2.active_streams > 0:
                                ss2.active_streams -= 1
                            if ss2:
                                ss2.last_activity_at = time.time()

                # Headers SSE propres (ne pas recopier Content-Length, et on force Content-Type)
                resp_headers = {
                    k: v for k, v in r.headers.items()
                    if (not _hop_by_hop_header(k)) and (k.lower() != "content-length")
                }
                resp_headers["content-type"] = "text/event-stream"
                resp_headers.setdefault("cache-control", "no-cache")

                return StreamingResponse(
                    _stream_sse_text(),
                    status_code=r.status_code,
                    headers=resp_headers,
                )

        r = await app.state.http.request(method, upstream_url, headers=headers, content=body)
        resp_headers = {k: v for k, v in r.headers.items() if not _hop_by_hop_header(k)}
        return Response(
            content=r.content,
            status_code=r.status_code,
            headers=resp_headers,
            media_type=r.headers.get("content-type"),
        )

    except httpx.ConnectTimeout:
        raise HTTPException(status_code=504, detail="Upstream connect timeout")
    except httpx.ReadTimeout:
        raise HTTPException(status_code=504, detail="Upstream read timeout")
    except httpx.ConnectError as e:
        raise HTTPException(status_code=502, detail=f"Upstream connect error: {e!s}")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {e!s}")


async def _gc_loop():
    while True:
        try:
            await asyncio.sleep(settings.gc_interval_seconds)
            now = time.time()
            ttl_s = settings.session_ttl_minutes * 60

            to_delete = []
            async with SESSIONS_LOCK:
                for sid, s in list(SESSIONS.items()):
                    if s.active_streams > 0:
                        continue
                    if now - s.last_activity_at > ttl_s:
                        to_delete.append(sid)

            for sid in to_delete:
                async with SESSIONS_LOCK:
                    s = SESSIONS.pop(sid, None)
                if not s:
                    continue
                try:
                    _k8s_delete_session(s.namespace, sid)
                except Exception:
                    pass

        except asyncio.CancelledError:
            return
        except Exception:
            continue
