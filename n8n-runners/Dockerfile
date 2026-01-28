FROM n8nio/runners:1.123.17

COPY ../nodes/ /home/node/.n8n/nodes
RUN [[ -d /home/node/.n8n/nodes/node_modules ]] || exit 1

# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-runners"