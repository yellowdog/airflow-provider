FROM yellowdogco/static-site-server:1.6.0-58

COPY _build/html /static

ENV STATIC_ASSET_CACHE_CONTROL_HEADER_VALUE="no-cache"
ENV ENABLE_DOCS_HEADER="true"
