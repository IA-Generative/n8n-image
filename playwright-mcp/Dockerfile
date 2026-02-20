FROM mcr.microsoft.com/playwright:v1.58.2-jammy AS playwright

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_RUNTIME_DIR=/tmp/.chromium
ENV XDG_CACHE_HOME=/tmp/.chromium
ENV NPM_CONFIG_CACHE=/tmp/.npm

RUN npx --yes playwright@1.58.0-alpha-2026-01-07 install chromium

FROM mcr.microsoft.com/playwright/mcp:v0.0.67

ENV XDG_RUNTIME_DIR=/tmp/.chromium
ENV XDG_CACHE_HOME=/tmp/.chromium
ENV NPM_CONFIG_CACHE=/tmp/.npm
ENV HOME=/tmp
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY --from=playwright /ms-playwright /ms-playwright
