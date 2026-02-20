FROM n8nio/n8n:2.8.3

USER root

COPY ../nodes/ /home/node/.n8n/nodes
RUN [[ -d /home/node/.n8n/nodes/node_modules ]] || exit 1

# # Switch to n8n's installation directory
# WORKDIR /usr/local/lib/node_modules/n8n

# COPY ../global/*.json ../global/*.sh ./
# RUN sh install_global.sh

# COPY ../tracing.js ../n8n-otel-instrumentation.js /usr/local/lib/node_modules/n8n/
# WORKDIR /home/node

USER node

# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n"
