# upag

`upag` is a lightweight HTTP(S) uptime monitor for self-hosted systems. It runs
as a single Go daemon, reads monitor definitions from YAML, stores state and
probe history in SQLite, and sends email alerts when monitored endpoints
transition DOWN or UP.

## Features

- HTTP and HTTPS checks with exact expected status-code validation.
- Optional exact, case-sensitive response body assertions.
- Optional maximum full response duration checks.
- Per-monitor intervals and timeouts with global defaults.
- SQLite persistence for monitor state, probe history, incidents, and alert
  notification attempts.
- Email incident alerts through SMTP, the Mailtrap Transactional Email API, or
  both.
- Retry storage for failed alert notification attempts.
- Foreground and background daemon commands.
- Configuration reload by signal without restarting the daemon process.
- CLI inspection commands for current monitor state and recent incidents.

## Installation

Download a release asset from:

```text
https://github.com/mlahr/upag/releases
```

Current release builds publish Linux amd64 assets:

- `upag_*_linux_amd64.tar.gz`
- `upag_*_linux_amd64.deb`
- `checksums.txt`

Install the Debian package:

```sh
sudo dpkg -i upag_*_linux_amd64.deb
```

Or install from the tarball:

```sh
tar -xzf upag_*_linux_amd64.tar.gz
sudo install -m 0755 upag /usr/local/bin/upag
```

Build from source:

```sh
go build -o upag .
```

Source builds require Go and a C compiler because `upag` uses
`github.com/mattn/go-sqlite3`, a CGO-backed SQLite driver.

## Quick Start

Create a configuration file:

```sh
cp config.example.yaml config.yaml
```

Edit `config.yaml` with at least one alert provider and one monitor:

```yaml
smtp:
  host: smtp.example.com
  port: 587
  tls: starttls
  username: alerts@example.com
  password: change-me
  from: alerts@example.com
  to:
    - ops@example.com

defaults:
  interval: 60s
  timeout: 10s
  failure_threshold: 3
  history_retention: 720h

monitors:
  - id: homepage
    name: Homepage
    url: https://example.com/
    expected_status_code: 200
```

Run `upag` in the foreground:

```sh
upag run --config ./config.yaml --db ./upag.sqlite
```

Inspect monitor state and recent incidents from another shell:

```sh
upag monitors --db ./upag.sqlite
upag incidents --db ./upag.sqlite --limit 50
```

## Configuration

`upag` reads a YAML configuration file. Durations use Go duration syntax, such
as `500ms`, `10s`, `1m`, or `720h`.

Complete example:

```yaml
smtp:
  host: smtp.example.com
  port: 587
  tls: starttls
  username: alerts@example.com
  password: change-me
  from: alerts@example.com
  to:
    - ops@example.com

# To also use Mailtrap's HTTPS Transactional Email API, configure this block.
# If both smtp and mailtrap are configured, alerts are sent through both.
# mailtrap:
#   token: change-me
#   from: alerts@example.com
#   from_name: upag
#   to:
#     - ops@example.com

alerts:
  notification_retries:
    max_attempts: 3
    backoff: [1m, 5m, 15m]

defaults:
  interval: 60s
  timeout: 10s
  failure_threshold: 3
  history_retention: 720h

monitors:
  - id: example
    name: Example homepage
    url: https://example.com/
    expected_status_code: 200
    max_response_time: 500ms
    response_body:
      must_contain: Example Domain
      must_not_contain: Maintenance mode
```

### Alert Providers

Configure at least one alert provider. If both `smtp` and `mailtrap` are
configured, each incident alert is sent through both providers.

`smtp` fields:

- `host`: SMTP server hostname. Required when SMTP is configured.
- `port`: SMTP server TCP port. Defaults to `587`.
- `tls`: SMTP transport mode. Must be `none`, `starttls`, or `tls`. Defaults to
  `starttls`.
- `username`: SMTP username.
- `password`: SMTP password.
- `from`: sender email address. Required when SMTP is configured.
- `to`: recipient email addresses. Must contain at least one recipient.
- `local_name`: optional local name used by the SMTP client.

`mailtrap` fields:

- `token`: Mailtrap API token. Required when Mailtrap is configured.
- `endpoint`: Mailtrap API endpoint. Defaults to
  `https://send.api.mailtrap.io/api/send`.
- `from`: sender email address. Required when Mailtrap is configured.
- `from_name`: optional sender display name.
- `to`: recipient email addresses. Must contain at least one recipient.

### Alert Retry Policy

`alerts.notification_retries.max_attempts` is the total number of send attempts,
including the initial attempt. `alerts.notification_retries.backoff` is the list
of retry delays. Defaults are `max_attempts: 3` and `[1m, 5m, 15m]`.

Failed notification attempts are stored in SQLite and retried by the daemon.

### Defaults

`defaults` apply to monitors that do not override the value:

- `interval`: time between checks. Defaults to `60s`.
- `timeout`: HTTP request timeout. Defaults to `10s`.
- `failure_threshold`: consecutive failed checks required before a monitor
  transitions DOWN. Defaults to `3`.
- `history_retention`: retained probe history duration. Defaults to `720h`.

### Monitors

Each monitor requires:

- `id`: stable identifier used as the SQLite primary key for monitor state.
- `name`: human-readable monitor name.
- `url`: `http` or `https` URL.
- `expected_status_code`: exact HTTP status code required for success.

Optional monitor fields:

- `interval`: monitor-specific check interval.
- `timeout`: monitor-specific HTTP request timeout.
- `max_response_time`: maximum full response duration.
- `insecure_skip_verify`: disables HTTPS certificate verification when `true`.
- `response_body.must_contain`: exact, case-sensitive string required in the
  response body.
- `response_body.must_not_contain`: exact, case-sensitive string forbidden in
  the response body.

Redirects are not followed. For example, a monitor expecting `302` succeeds when
the first response is `302`.

Body assertions are evaluated only after the observed status code matches
`expected_status_code`.

`latency_ms` in logs and stored probe history is time to response headers. When
`max_response_time` or `response_body` is configured, `response_time_ms` is the
full response duration, ending after the response body has been read. Otherwise,
`response_time_ms` is `0`.

## Operations

Run in the foreground:

```sh
upag run --config ./config.yaml --db ./upag.sqlite
```

Run as a background daemon:

```sh
upag start --config ./config.yaml --db ./upag.sqlite --pid-file ./upag.pid --log-file ./upag.log
upag status --pid-file ./upag.pid
upag restart --config ./config.yaml --db ./upag.sqlite --pid-file ./upag.pid --log-file ./upag.log
upag stop --pid-file ./upag.pid
```

Reload configuration without restarting the daemon process:

```sh
upag config reload --pid-file ./upag.pid
```

Configuration reloads add new monitors, update monitors with matching IDs, and
stop scheduling monitors removed from the YAML file.

Inspect stored state:

```sh
upag monitors --db ./upag.sqlite
upag incidents --db ./upag.sqlite --limit 50
```

Print the binary version:

```sh
upag --version
```

The daemon writes line-oriented logs to stdout and stderr. When started with
`start`, both streams are appended to `--log-file`.

## Development

Common development commands:

```sh
make fmt
make vet
make test
make build
```

Run the complete default target:

```sh
make
```

Run tests directly:

```sh
go test ./...
```

## Release

Releases are built by GitHub Actions using GoReleaser on tag pushes.

Create and push a version tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

Wait for the `release` workflow to finish, then download assets from the GitHub
release page:

- `upag_*_linux_amd64.tar.gz`
- `upag_*_linux_amd64.deb`
- `checksums.txt`

Local dry run:

```sh
goreleaser release --snapshot --clean
```
