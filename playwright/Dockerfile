# Image name: playwright
FROM mcr.microsoft.com/playwright:v1.59.1-jammy

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_RUNTIME_DIR=/tmp/.runtime
ENV XDG_CACHE_HOME=/tmp/.cache
ENV NPM_CONFIG_CACHE=/tmp/.npm

# ENV pour Firefox
ENV HOME=/tmp/home
ENV TMPDIR=/tmp

RUN npx --yes playwright@1.58.2 install firefox \
    && npx --yes playwright@1.58.2 install chromium

CMD ["npx", "playwright@1.58.2", "run-server", "--port", "3000"]
