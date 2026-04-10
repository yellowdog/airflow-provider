FROM yellowdogco/static-site-server:1.7.2-83

COPY _build/html /static

ENV STATIC_ASSET_CACHE_CONTROL_HEADER_VALUE="no-cache"
ENV ENABLE_DOCS_HEADER="true"
ENV CONTENT_SECURITY_POLICY="default-src 'self'; script-src 'self' 'unsafe-inline'"
