FROM n8nio/runners:1.123.16

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

COPY ../nodes/ /home/node/.n8n/nodes/node_modules

# Switch to n8n's installation directory
WORKDIR /usr/local/lib/node_modules/n8n

USER node

# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/ia-generative/n8n-runners"