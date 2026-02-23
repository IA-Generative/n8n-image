# Image name: n8n-runners
FROM n8nio/runners:1.123.21 

USER root
RUN cd /opt/runners/task-runner-javascript && pnpm add playwright-core
COPY n8n-runners/n8n-task-runners.json /etc/n8n-task-runners.json
USER runner

LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-runners"