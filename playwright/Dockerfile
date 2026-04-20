# Image name: playwright
FROM mcr.microsoft.com/playwright:v1.58.2-jammy

ARG BROWSER=chromium

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_RUNTIME_DIR=/tmp/.chromium
ENV XDG_CACHE_HOME=/tmp/.chromium
ENV NPM_CONFIG_CACHE=/tmp/.npm

RUN npx --yes playwright@1.58.2 install firefox \
    && npx --yes playwright@1.58.2 install chromium

CMD ["npx", "playwright@1.58.2", "run-server", "--port", "3000"]
