# upag

`upag` is an internal HTTP(S) uptime monitor. It runs as one Go daemon on a private VPS or container, reads monitor definitions from YAML, records state and probe history in SQLite, and sends email alerts through SMTP when monitors transition DOWN or UP.

## Build

```sh
go build -o upag .
```

The SQLite driver uses CGO, so the build environment needs a C compiler.

## Configure

Start from `config.example.yaml`.

Each monitor requires:

- `id`: stable identifier used as the SQLite primary key for monitor state.
- `name`: human-readable monitor name.
- `url`: `http` or `https` URL.
- `expected_status_code`: exact HTTP status code required for success.

Redirects are not followed. For example, a monitor expecting `302` succeeds when the first response is `302`.

HTTPS certificates are verified by default. Set `insecure_skip_verify: true` only for internal endpoints that intentionally use self-signed or otherwise unverifiable certificates.

## Run

```sh
./upag run --config ./config.yaml --db ./upag.sqlite
```

Reload configuration without restarting the process:

```sh
kill -HUP <pid>
```

Configuration reloads add new monitors, update monitors with matching IDs, and stop scheduling monitors removed from the YAML file.

## Inspect

```sh
./upag status --db ./upag.sqlite
./upag incidents --db ./upag.sqlite --limit 50
```

The service exposes no HTTP UI, HTTP API, metrics endpoint, or built-in authentication in v1. Restrict access with private-network placement and normal host/container controls.
