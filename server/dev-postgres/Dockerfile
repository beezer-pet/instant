# Postgres for Instant: pg_hint_plan, which the server's queries use, and
# wal2json, which its replication slots decode with. Both come from the
# Postgres project's apt repository that the base image is set up with.
# Postgres 18 refuses an output plugin that output_plugin_libraries does not
# name, so the command names wal2json there.
# Published as ghcr.io/beezer-pet/postgresql:<PG_VERSION> by
# .github/workflows/beezer-postgres.yml.
ARG PG_VERSION=18.7
FROM postgres:${PG_VERSION}-trixie

ARG PG_HINT_PLAN_VERSION=1.8.1-1.pgdg13+2
ARG WAL2JSON_VERSION=2.7-1.pgdg13+2

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      "postgresql-${PG_MAJOR}-pg-hint-plan=${PG_HINT_PLAN_VERSION}" \
      "postgresql-${PG_MAJOR}-wal2json=${WAL2JSON_VERSION}" && \
    rm -rf /var/lib/apt/lists/*

CMD ["postgres", \
       "-c", "wal_level=logical", \
       "-c", "max_replication_slots=4", \
       "-c", "max_wal_senders=4", \
       "-c", "shared_preload_libraries=pg_hint_plan", \
       "-c", "output_plugin_libraries=pgoutput,test_decoding,wal2json", \
       "-c", "random_page_cost=1.1"]
