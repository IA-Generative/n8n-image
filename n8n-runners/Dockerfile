# FEATURE with playwright-core
# FROM ghcr.io/jqlang/jq AS jq

# Image name: n8n-runners
FROM n8nio/runners:2.19.2

# --- FEATURE with playwright-core 
# USER root
# COPY --from=jq /jq /usr/local/bin/jq

# RUN cd /opt/runners/task-runner-javascript && pnpm add moment playwright-core
# RUN jq '(.["task-runners"][] | select(.["runner-type"] == "javascript") | .["env-overrides"]) += {"NODE_FUNCTION_ALLOW_EXTERNAL": "moment,playwright-core"}' /etc/n8n-task-runners.json > /tmp/json && mv /tmp/json /etc/n8n-task-runners.json
# --- END FEATURE with playwright-core

# USER runner
# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-image"
