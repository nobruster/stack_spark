FROM portainer/portainer-ce:lts

COPY --from=busybox:1.36-musl /bin/sh /bin/sh
COPY --from=busybox:1.36-musl /bin/wget /bin/wget

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9000/api/system/status || exit 1
