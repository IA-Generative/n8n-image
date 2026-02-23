# Image name: n8n-runners
FROM n8nio/runners:1.123.21 

USER root
RUN cd /opt/runners/task-runner-javascript && pnpm add playwright-core
# add "NODE_FUNCTION_ALLOW_EXTERNAL": "playwright-core", in env-overrides of n8n-task-runners.json, with sed, cause missing lines
RUN sed -i '/NODE_FUNCTION_ALLOW_BUILTIN/a\                          "NODE_FUNCTION_ALLOW_EXTERNAL": "playwright-core",' /etc/n8n-task-runners.json
USER runner

LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-image"