Builds the Postgres image that the local docker compose setups, the self-hosting compose files and CI use: Postgres with pg_hint_plan and wal2json, at the versions the Dockerfile's `ARG`s name.

`.github/workflows/beezer-postgres.yml` publishes it as `ghcr.io/beezer-pet/postgresql:<PG_VERSION>` for amd64 and arm64 when the Dockerfile changes on `self-hosted`, or when it is run by hand. A new Postgres version is a new `PG_VERSION` in the Dockerfile and the same tag in the compose files and in `.github/workflows/clojure.yml`.

A local build:

```sh
make build-image
```

Postgres 18 keeps its data under `/var/lib/postgresql/18/docker`, so a volume is mounted at `/var/lib/postgresql`. Data of an older major version is not read by it: it is moved over by a dump and a restore (`self-hosting/UPGRADE_DB.md`).
