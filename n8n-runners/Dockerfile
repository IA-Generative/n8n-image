FROM ghcr.io/jqlang/jq AS jq


# Image name: n8n-runners
FROM ghcr.io/ia-generative/n8n-runners:2.8.3

USER root
COPY --from=jq /jq /usr/local/bin/jq

RUN cd /opt/runners/task-runner-javascript && pnpm add moment playwright-core
RUN jq '(.["task-runners"][] | select(.["runner-type"] == "javascript") | .["env-overrides"]) += {"NODE_FUNCTION_ALLOW_EXTERNAL": "moment,playwright-core"}' /etc/n8n-task-runners.json > /tmp/json && mv /tmp/json /etc/n8n-task-runners.json

USER runner
# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-image"
