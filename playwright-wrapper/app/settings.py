from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    api_token: str = "CHANGE_ME"
    target_namespace: str | None = None

    playwright_image: str = "mcr.microsoft.com/playwright/mcp:latest"
    playwright_port: int = 8933

    playwright_command: str = "node"
    playwright_args: str = (
        "cli.js --headless --browser chromium --no-sandbox "
        "--host 0.0.0.0 --allowed-hosts * "
        "--port 8933"
    )

    enable_pvc_mount: bool = False
    pvc_name: str = "playwright-artifacts-pvc"
    pvc_mount_path: str = "/artifacts"

    session_ttl_minutes: int = 30
    gc_interval_seconds: int = 30
    pod_ready_timeout_seconds: int = 90

    service_type: str = "NodePort"   # ClusterIP | NodePort
    node_address: str = "127.0.0.1"  # chez toi: 172.19.0.2


settings = Settings()
