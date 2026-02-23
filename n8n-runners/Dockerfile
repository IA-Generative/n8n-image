# Image name: n8n-runners
FROM n8nio/runners:1.123.21 

USER root
RUN cd /opt/runners/task-runner-javascript && pnpm add playwright-core
# replace NODE_FUNCTION_ALLOW_EXTERNAL line with "NODE_FUNCTION_ALLOW_EXTERNAL": "playwright-core,moment", of n8n-task-runners.json, with sed and preserving the present value
RUN sed -i 's/"NODE_FUNCTION_ALLOW_EXTERNAL": "moment"/"NODE_FUNCTION_ALLOW_EXTERNAL": "playwright-core,moment"/' /etc/n8n-task-runners.json
USER runner
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-image"