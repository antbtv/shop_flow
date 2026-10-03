# Grafana for the dashboard (FR-10, ADR-0011) with the ClickHouse data source plugin baked in.
# Built on Pi5 (arm64) like the Airflow image; the same file builds on the laptop (amd64) for tests.
# Everything is pinned: the image by tag and digest, the plugin by version and sha256 per
# architecture. The plugin lives outside /var/lib/grafana: a named volume copies the image content
# only when it is created, so a plugin kept there would stay at the old version after a rebuild.
# Update: take the new version and sha256 from
# https://grafana.com/api/plugins/grafana-clickhouse-datasource/versions (packages.linux-<arch>).
FROM grafana/grafana-oss:12.4.3@sha256:2e986801428cd689c2358605289c90ab37d2b39e24808874971f54c99bcdc412

ARG CH_PLUGIN_VERSION=4.22.0
ARG CH_PLUGIN_SHA256_AMD64=2041f4c32160aeedc36c6e51cc996542b44c27f4595dd33546cb18018b30b8d8
ARG CH_PLUGIN_SHA256_ARM64=530bef1ff592a5aa3ff43e391e7a66d8df875c940d051da4fe30ddac0e540543

USER root
RUN set -eu; \
    case "$(uname -m)" in \
        x86_64) arch=amd64; sha=$CH_PLUGIN_SHA256_AMD64 ;; \
        aarch64) arch=arm64; sha=$CH_PLUGIN_SHA256_ARM64 ;; \
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/plugin.zip \
        "https://grafana.com/api/plugins/grafana-clickhouse-datasource/versions/${CH_PLUGIN_VERSION}/download?os=linux&arch=${arch}"; \
    echo "${sha}  /tmp/plugin.zip" | sha256sum -c -; \
    mkdir -p /opt/grafana/plugins; \
    unzip -q /tmp/plugin.zip -d /opt/grafana/plugins; \
    rm /tmp/plugin.zip; \
    chown -R 472:0 /opt/grafana; \
    chmod -R g=u /opt/grafana
ENV GF_PATHS_PLUGINS=/opt/grafana/plugins
USER 472
