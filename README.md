# upag

`upag` is an internal HTTP(S) uptime monitor. It runs as one Go daemon on a private VPS or container, reads monitor definitions from YAML, records state and probe history in SQLite, and sends email alerts through SMTP or Mailtrap when monitors transition DOWN or UP.

## Build

```sh
go build -o upag .
```

The SQLite driver uses CGO, so the build environment needs a C compiler.

## Configure

Start from `config.example.yaml`.

Alert delivery uses every configured provider. A config with only `smtp:` sends
through SMTP. A config with only `mailtrap:` sends through Mailtrap. A config
with both blocks sends each incident alert through both providers.

```yaml
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to:
    - ops@example.com
```

For Mailtrap's HTTPS Transactional Email API, use:

```yaml
mailtrap:
  token: change-me
  from: alerts@example.com
  from_name: upag
  to:
    - ops@example.com
```

When `mailtrap.endpoint` is omitted, it defaults to
`https://send.api.mailtrap.io/api/send`.

Failed alert notification attempts are retried by the daemon. Retry policy is
global:

```yaml
alerts:
  notification_retries:
    max_attempts: 3
    backoff: [1m, 5m, 15m]
```

`max_attempts` includes the initial send attempt.

Each monitor requires:

- `id`: stable identifier used as the SQLite primary key for monitor state.
- `name`: human-readable monitor name.
- `url`: `http` or `https` URL.
- `expected_status_code`: exact HTTP status code required for success.

Redirects are not followed. For example, a monitor expecting `302` succeeds when the first response is `302`.

Monitors can also assert exact, case-sensitive substrings in the response body:

```yaml
response_body:
  must_contain: "Welcome"
  must_not_contain: "Maintenance mode"
```

When `response_body` is omitted, only the HTTP status code is checked. Body
assertions are evaluated only after the observed status code matches
`expected_status_code`.

Monitors can also assert a maximum full response duration:

```yaml
max_response_time: 500ms
```

`latency_ms` in logs and stored probe history is time to response headers.
When `max_response_time` or `response_body` is configured, `response_time_ms`
is the full response duration, ending after the response body has been read.
Otherwise, `response_time_ms` is `0`. `max_response_time` is checked against
`response_time_ms`.

HTTPS certificates are verified by default. Set `insecure_skip_verify: true` only for internal endpoints that intentionally use self-signed or otherwise unverifiable certificates.

## Run

```sh
./upag run --config ./config.yaml --db ./upag.sqlite
```

Run as a background daemon:

```sh
./upag start --config ./config.yaml --db ./upag.sqlite --pid-file ./upag.pid --log-file ./upag.log
./upag status --pid-file ./upag.pid
./upag restart --config ./config.yaml --db ./upag.sqlite --pid-file ./upag.pid --log-file ./upag.log
./upag stop --pid-file ./upag.pid
```

The daemon writes line-oriented logs to stdout and stderr. When started with `start`,
both streams are appended to `--log-file`. Logged events include daemon start,
daemon ready, daemon shutdown, configuration reloads, probe results, state
storage failures, alert decisions, alert notification storage failures, and
history prune failures. Alert notification send attempts for transition
incidents are also persisted in SQLite.

Reload configuration without restarting the process:

```sh
./upag config reload --pid-file ./upag.pid
```

Configuration reloads add new monitors, update monitors with matching IDs, and stop scheduling monitors removed from the YAML file.

## Inspect

```sh
./upag monitors --db ./upag.sqlite
./upag incidents --db ./upag.sqlite --limit 50
```

The service exposes no HTTP UI, HTTP API, metrics endpoint, or built-in authentication in v1. Restrict access with private-network placement and normal host/container controls.

## Release (Linux amd64 + .deb)

Releases are built by GitHub Actions using GoReleaser on tag pushes.

Release steps:

1. Create a version tag and push it:

```sh
git tag v0.1.0
git push origin v0.1.0
```

2. Wait for the `release` workflow to finish.
3. Download assets from the GitHub release page:

- `upag_*_linux_amd64.tar.gz`
- `upag_*_linux_amd64.deb`
- `checksums.txt`

Local dry run (optional):

```sh
goreleaser release --snapshot --clean
```
