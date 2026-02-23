# Image name: n8n
FROM n8nio/n8n:1.123.20

USER root

# Install required packages
RUN echo "Installing required packages..." && \
    apk add --no-cache \
    curl \
    gettext \
    coreutils \
    openssl \
    ca-certificates \
    musl-dev && \
    echo "Curl installed successfully: $(curl --version | head -n 1)" && \
    echo "Envsubst installed successfully: $(envsubst --version | head -n 1)"

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
