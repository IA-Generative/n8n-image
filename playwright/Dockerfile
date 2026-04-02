# Image suffix: chromium
# Image name: playwright
FROM mcr.microsoft.com/playwright:v1.59.1-jammy

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_RUNTIME_DIR=/tmp/.chromium
ENV XDG_CACHE_HOME=/tmp/.chromium
ENV NPM_CONFIG_CACHE=/tmp/.npm

RUN npx --yes playwright install chromium
