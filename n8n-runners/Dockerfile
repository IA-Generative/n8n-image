FROM n8nio/runners:2.5.0

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

WORKDIR /home/node/.n8n/nodes

RUN npm install \
    n8n-nodes-imap@2.14.1

# Switch to n8n's installation directory
WORKDIR /usr/local/lib/node_modules/n8n

# Install Node.js OpenTelemetry dependencies locally to n8n
RUN npm install -g \
    @opentelemetry/api \
    @opentelemetry/sdk-node \
    @opentelemetry/auto-instrumentations-node \
    @opentelemetry/exporter-trace-otlp-http \
    @opentelemetry/exporter-logs-otlp-http \
    @opentelemetry/resources \
    @opentelemetry/semantic-conventions \
    @opentelemetry/instrumentation \
    @opentelemetry/instrumentation-winston \
    @opentelemetry/winston-transport \
    winston \
    flat

COPY ../tracing.js ../n8n-otel-instrumentation.js /usr/local/lib/node_modules/n8n/

ENV NODE_OPTIONS="-r @opentelemetry/auto-instrumentations-node/register -r /usr/local/lib/node_modules/n8n/tracing.js"

USER node

# Set image repository label
LABEL org.opencontainers.image.source="https://github.com/IA-Generative/n8n-runners"