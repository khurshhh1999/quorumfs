FROM eclipse-temurin:21-jre-jammy@sha256:e9aaf73145bbd1f9f6ec7f6867dd75a44f34b1a6c32a813504bf4129be2d09d7
# Pin the Jammy fix for CVE-2026-84782 until the upstream JRE image includes it.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3=3.0.2-0ubuntu1.30 openssl=3.0.2-0ubuntu1.30 \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 10001 quorumfs && useradd --uid 10001 --gid 10001 --no-create-home quorumfs \
    && mkdir -p /var/lib/quorumfs && chown 10001:10001 /var/lib/quorumfs
COPY modules/server/build/install/server /opt/quorumfs/server
COPY modules/client/build/install/client /opt/quorumfs/client
USER 10001:10001
EXPOSE 9000
ENTRYPOINT ["/opt/quorumfs/server/bin/server"]
