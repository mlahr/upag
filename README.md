# upag

`upag` is a lightweight HTTP(S) uptime monitor for self-hosted systems. It runs
as a single Go daemon, reads monitor definitions from YAML, stores state and
probe history in SQLite, and sends alerts when monitored endpoints transition
DOWN or UP.

## Features

- HTTP and HTTPS checks with exact expected status-code validation.
- Optional exact, case-sensitive and command-based response body assertions.
- Optional maximum full response duration checks.
- Per-monitor intervals and timeouts with global defaults.
- Deterministically staggered monitor scheduling to avoid synchronized probe
  bursts.
- Observer connectivity checks that suppress per-monitor DOWN transitions when
  the monitoring host itself loses outbound network connectivity.
- SQLite persistence for monitor state, probe history, incidents, and alert
  notification attempts, with automatic startup migrations.
- Incident alerts through SMTP, the Mailtrap Transactional Email API, Telegram,
  or any configured combination.
- Retry storage for failed alert notification attempts.
- Foreground and background daemon commands.
- Configuration reload by signal without restarting the daemon process.
- CLI inspection commands for current monitor state and recent incidents.
- Standalone text or JSON diagnostics for one configured monitor without state
  changes, alerting, or daemon coordination.
- Optional local HTTP endpoints for daemon health, monitor state, and alert
  delivery failures.

## Installation

On Debian-based Linux amd64 systems, install the latest released Debian package:

```sh
curl -fsSL https://raw.githubusercontent.com/mlahr/upag/main/install.sh | bash
```

The installer downloads the latest GitHub Release `.deb`, verifies it against
the release `checksums.txt`, and installs it with `apt-get`.

Or download a release asset manually from:

```text
https://github.com/mlahr/upag/releases
```

Current release builds publish Linux amd64 assets:

- `upag_*_linux_amd64.tar.gz`
- `upag_*_linux_amd64.deb`
- `checksums.txt`

Install the Debian package:

```sh
sudo apt-get install ./upag_*_linux_amd64.deb
```

The Debian package installs:

- `/usr/bin/upag`
- `/etc/upag/config.yaml`
- `/etc/default/upag`
- `/var/lib/upag/`
- `/lib/systemd/system/upag.service`
- `/etc/init.d/upag`

Edit `/etc/upag/config.yaml`, then enable service startup in
`/etc/default/upag`:

```sh
sudoedit /etc/upag/config.yaml
sudoedit /etc/default/upag
```

Set:

```sh
UPAG_ENABLED=true
```

Start the service on a systemd-based system:

```sh
sudo systemctl start upag
sudo systemctl status upag
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
alerts:
  providers:
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
  probe_retries: 2
  probe_retry_backoff: 500ms
  failure_threshold: 3
  history_retention: 720h

storage:
  backend: sqlite
  sqlite:
    path: ./upag.sqlite
  probe_results:
    retention: 24h
  probe_minute_rollups:
    retention: 30d
  probe_hourly_rollups:
    retention: 1y
  probe_daily_rollups:
    retention: forever

tenant_id: default

monitors:
  - id: homepage
    name: Homepage
    url: https://example.com/
    expected_status_code: 200
    failure_threshold: 2
```

Run `upag` in the foreground:

```sh
upag run --config ./config.yaml
```

Inspect monitor state and recent incidents from another shell:

```sh
upag monitors --config ./config.yaml
upag incidents --config ./config.yaml --limit 50
upag incidents --config ./config.yaml --limit 50 --since 2026-06-23T00:00:00Z
```

Run one immediate diagnostic attempt for a configured monitor:

```sh
upag check --config ./config.yaml --monitor homepage
upag check --config ./config.yaml --monitor homepage --format json
```

The diagnostic exits successfully only when every configured assertion passes.
It performs one HTTP attempt without configured probe retries, does not contact
the daemon, does not open storage, and does not update state, incidents, failure
counters, maintenance data, or alerts. It does execute a configured
`response_body.command`; that external program may have its own side effects.
Diagnostic output never includes the response body or response headers.

Schedule one-off maintenance when failures are expected:

```sh
upag maintenance add --config ./config.yaml --monitor homepage \
  --start 2026-06-23T01:00:00Z --end 2026-06-23T02:00:00Z \
  --reason "deploy"
upag maintenance list --config ./config.yaml
upag maintenance cancel --config ./config.yaml --id 1 --reason "finished"
```

For PostgreSQL backends, `tenant_id` defines the tenant namespace used for all
status, incidents, rollups, maintenance windows, and alert rows. If omitted, it
defaults to `default`.

`tenant_id` must be 1–63 characters and match `^[a-zA-Z0-9](?:[a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$`.
Whitespace inside, leading/trailing, and non-ASCII characters are not accepted.
If you are migrating into PostgreSQL in a multi-tenant topology, explicitly set
`--tenant-id` during migration to avoid writing legacy rows to the default
namespace accidentally.

## Configuration

`upag` reads a YAML configuration file. Durations use Go duration syntax, such
as `500ms`, `10s`, `1m`, or `720h`. Storage retention also accepts `d` for
24-hour days, `y` for 365-day years, and `forever`.

Complete example:

```yaml
alerts:
  providers:
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
    # mailtrap:
    #   token: change-me
    #   from: alerts@example.com
    #   from_name: upag
    #   to:
    #     - ops@example.com
    # To also use Telegram Bot API alerts, configure this block.
    # telegram:
    #   token: change-me
    #   chat_ids:
    #     - "123456789"
  notification_retries:
    max_attempts: 3
    backoff: [1m, 5m, 15m]

http:
  address: 127.0.0.1
  port: 0

observer:
  enabled: true
  interval: 30s
  timeout: 5s
  failure_threshold: 3
  recovery_threshold: 1
  required_successes: 1

defaults:
  interval: 60s
  timeout: 10s
  failure_threshold: 3
  history_retention: 720h

storage:
  backend: sqlite
  sqlite:
    path: ./upag.sqlite
  # Or use PostgreSQL/Supabase:
  # backend: postgres
  # postgres:
  #   dsn: postgres://user:password@host:5432/postgres?sslmode=require
  probe_results:
    retention: 24h
  probe_minute_rollups:
    retention: 30d
  probe_hourly_rollups:
    retention: 1y
  probe_daily_rollups:
    retention: forever

tenant_id: default

monitors:
  - id: example
    name: Example homepage
    url: https://example.com/
    expected_status_code: 200
    failure_threshold: 2
    max_response_time: 500ms
    response_body:
      must_contain:
        - Example Domain
      must_not_contain:
        - Maintenance mode
      command: ["jq", "-e", ".status == \"ok\""]
      command_timeout: 10s
```

### Alert Providers

Configure at least one alert provider under `alerts.providers`. If multiple
providers are configured, each incident alert is sent through each provider.

Existing top-level `smtp` and `mailtrap` blocks remain supported for
compatibility. Do not configure the same provider both at top level and under
`alerts.providers`.

`alerts.providers.smtp` fields:

- `host`: SMTP server hostname. Required when SMTP is configured.
- `port`: SMTP server TCP port. Defaults to `587`.
- `tls`: SMTP transport mode. Must be `none`, `starttls`, or `tls`. Defaults to
  `starttls`.
- `username`: SMTP username.
- `password`: SMTP password.
- `from`: sender email address. Required when SMTP is configured.
- `to`: recipient email addresses. Must contain at least one recipient.
- `local_name`: optional local name used by the SMTP client.

`alerts.providers.mailtrap` fields:

- `token`: Mailtrap API token. Required when Mailtrap is configured.
- `endpoint`: Mailtrap API endpoint. Defaults to
  `https://send.api.mailtrap.io/api/send`.
- `from`: sender email address. Required when Mailtrap is configured.
- `from_name`: optional sender display name.
- `to`: recipient email addresses. Must contain at least one recipient.

`alerts.providers.telegram` fields:

- `token`: Telegram bot token. Required when Telegram is configured.
- `chat_ids`: Telegram chat IDs. Must contain at least one chat ID. Quote chat
  IDs in YAML so negative group IDs are parsed as strings.
- `endpoint`: Telegram Bot API endpoint. Defaults to
  `https://api.telegram.org`.

### Alert Retry Policy

`alerts.notification_retries.max_attempts` is the total number of send attempts,
including the initial attempt. `alerts.notification_retries.backoff` is the list
of retry delays. Defaults are `max_attempts: 3` and `[1m, 5m, 15m]`.

Failed notification attempts are stored in SQLite and retried by the daemon.

### HTTP Status Listener

`http.port` controls the optional HTTP status listener. The default is `0`,
which disables the listener. When set to a TCP port from `1` through `65535`,
the daemon listens on `http.address:<port>`. `http.address` defaults to
`127.0.0.1`.

Use `0.0.0.0` or `::` only when the status endpoints should be reachable from
other hosts, and restrict access at the host, firewall, or reverse-proxy layer.
The status endpoints do not perform authentication.

Endpoints:

- `GET /health`: daemon liveness metadata.
- `GET /status`: daemon metadata, observer connectivity, monitor state,
  per-monitor uptime statistics, and actionable alert delivery failures.

Example `GET /health` response:

```json
{
  "status": "ok",
  "version": "dev",
  "started_at": "2026-06-22T02:15:04.123456789Z"
}
```

Example `GET /status` response:

```json
{
  "status": "ok",
  "version": "dev",
  "started_at": "2026-06-22T02:15:04.123456789Z",
  "config_path": "/etc/upag/config.yaml",
  "monitor_count": 1,
  "observer": {
    "status": "OBSERVER_UP",
    "consecutive_failures": 0,
    "consecutive_successes": 12,
    "last_checked_at": "2026-06-22T02:20:00.123456789Z",
    "last_success_at": "2026-06-22T02:20:00.123456789Z",
    "last_failure_at": null,
    "last_error": "",
    "updated_at": "2026-06-22T02:20:00.123456789Z",
    "sentinels": [
      {
        "id": "gstatic",
        "name": "Google connectivity check",
        "url": "https://www.gstatic.com/generate_204",
        "ok": true,
        "expected_status_code": 204,
        "observed_status_code": 204,
        "latency_ms": 12,
        "error": "",
        "checked_at": "2026-06-22T02:20:00.123456789Z"
      }
    ]
  },
  "monitors": [
    {
      "id": "homepage",
      "name": "Homepage",
      "url": "https://example.com/",
      "status": "UP",
      "consecutive_failures": 0,
      "last_checked_at": "2026-06-22T02:20:04.987654321Z",
      "last_success_at": "2026-06-22T02:20:04.987654321Z",
      "last_failure_at": null,
      "last_error": "",
      "last_observed_status_code": 200,
      "updated_at": "2026-06-22T02:20:04.987654321Z",
      "uptime": {
        "24h": {
          "total_checks": 24,
          "successful_checks": 24,
          "failed_checks": 0,
          "maintenance_checks": 0,
          "maintenance_failed_checks": 0,
          "downtime_seconds": 0,
          "reportable_seconds": 86400,
          "uptime_percent": 100,
          "window_started_at": "2026-06-21T02:20:04.987654321Z",
          "window_ended_at": "2026-06-22T02:20:04.987654321Z"
        },
        "7d": {
          "total_checks": 168,
          "successful_checks": 167,
          "failed_checks": 1,
          "maintenance_checks": 0,
          "maintenance_failed_checks": 0,
          "downtime_seconds": 0,
          "reportable_seconds": 604800,
          "uptime_percent": 100,
          "window_started_at": "2026-06-15T02:20:04.987654321Z",
          "window_ended_at": "2026-06-22T02:20:04.987654321Z"
        },
        "30d": {
          "total_checks": 720,
          "successful_checks": 718,
          "failed_checks": 2,
          "maintenance_checks": 0,
          "maintenance_failed_checks": 0,
          "downtime_seconds": 0,
          "reportable_seconds": 2592000,
          "uptime_percent": 100,
          "window_started_at": "2026-05-23T02:20:04.987654321Z",
          "window_ended_at": "2026-06-22T02:20:04.987654321Z"
        },
        "retained": {
          "total_checks": 1440,
          "successful_checks": 1436,
          "failed_checks": 4,
          "maintenance_checks": 0,
          "maintenance_failed_checks": 0,
          "downtime_seconds": 0,
          "reportable_seconds": 2592000,
          "uptime_percent": 100,
          "window_started_at": "2026-05-23T02:20:04.987654321Z",
          "window_ended_at": "2026-06-22T02:20:04.987654321Z"
        }
      },
      "active_maintenance": null,
      "upcoming_maintenance": []
    }
  ],
  "alert_delivery_failures": [
    {
      "incident_id": 42,
      "monitor_id": "api",
      "provider": "smtp",
      "attempted_at": "2026-06-22T02:20:10.111222333Z",
      "attempt_number": 2,
      "error": "send mail: dial tcp: lookup smtp.example.com: no such host",
      "next_retry_at": "2026-06-22T02:25:10.111222333Z",
      "retry_exhausted": false
    }
  ]
}
```

`alert_delivery_failures` contains at most 50 latest failed delivery attempts,
one per incident and provider, excluding failures followed by a later successful
delivery for that same incident and provider.

Each monitor's `uptime` object is calculated from stored probe history. The
`24h`, `7d`, and `30d` windows include probes whose `checked_at` timestamp is
inside that window, inclusive of the lower boundary for raw probes and at bucket
granularity for compacted rollups. `retained` includes all available history
across raw probe results and probe rollups. Checks covered by a maintenance
window are excluded from `total_checks`, `successful_checks`, and
`failed_checks`, but are still counted in `maintenance_checks` and
`maintenance_failed_checks`.

`uptime_percent` is strict-accounting availability, not raw probe success rate.
A confirmed outage starts at the first failed probe in a consecutive failure
streak once that streak reaches `failure_threshold`, and it ends at the first
later successful probe. `downtime_seconds` is confirmed outage duration inside
the window. `reportable_seconds` is the reportable window duration after
subtracting maintenance. `uptime_percent` is
`(reportable_seconds - downtime_seconds) / reportable_seconds * 100`, rounded
to two decimal places. For a window with no reportable seconds,
`uptime_percent` is `null`.

`GET /status/history` returns the daily strict-accounting availability used by
calendar and uptime-tick consumers. The response contains every monitor in the
current status feed and exactly 90 UTC calendar dates, ordered oldest to newest
and including the current date. Each daily entry contains `date`,
`reportable_seconds`, `downtime_seconds`, and `uptime_percent`. Dates before a
monitor's first reportable probe, and dates whose covered duration is entirely
excluded by maintenance, have zero reportable and downtime seconds and a null
percentage. Confirmed outages that cross a UTC midnight are split by their
actual overlap with each date; an open outage accrues through `generated_at`.

The history endpoint is intentionally derived from confirmed status intervals,
not raw failed probes. It therefore uses the same failure-threshold,
maintenance, and observer-suppression semantics as the aggregate uptime windows
returned by `GET /status`.

### Defaults

`defaults` apply to monitors that do not override the value:

- `interval`: time between checks. Defaults to `60s`.
- `timeout`: HTTP request timeout. Defaults to `10s`.
- `probe_retries`: additional in-probe attempts before recording a failed
  scheduled check. Defaults to `2`; set to `0` to disable retries.
- `probe_retry_backoff`: delay between in-probe retry attempts. Defaults to
  `500ms`.
- `failure_threshold`: fallback consecutive failed checks required before a
  monitor transitions DOWN when the monitor does not set
  `monitors[].failure_threshold`. Defaults to `3`.
- `history_retention`: legacy raw probe retention fallback. Defaults to `720h`
  and is used only when `storage.probe_results.retention` is unset.

`storage.backend` selects persistence. Use `sqlite` for local SQLite storage or
`postgres` for PostgreSQL providers such as Supabase.

`storage` also controls probe compaction retention:

- `sqlite.path`: SQLite database path when `storage.backend` is `sqlite`.
- `postgres.dsn`: PostgreSQL connection string when `storage.backend` is
  `postgres`.
- `probe_results.retention`: raw probe result retention. Defaults to
  `defaults.history_retention`.
- `probe_minute_rollups.retention`: minute rollup retention. Defaults to `30d`.
- `probe_hourly_rollups.retention`: hourly rollup retention. Defaults to `1y`.
- `probe_daily_rollups.retention`: daily rollup retention. Defaults to
  `forever`.

Migrate an existing SQLite database into a PostgreSQL/Supabase-backed config:

```sh
upag storage migrate --from-sqlite ./upag.sqlite --config ./config.yaml
upag storage migrate --from-sqlite ./upag.sqlite --config ./config.yaml --tenant-id tenant-blue
```

The target config must use `storage.backend: postgres`, and the PostgreSQL
target must be empty.

### Observer Connectivity

The observer is the `upag` host itself. Observer connectivity checks determine
whether that host can reach the wider network. When observer connectivity is
`OBSERVER_DOWN`, failed monitor probes are stored with
`observer_suppressed=true`, but they do not change monitor state, create
per-monitor incidents, or count as reported downtime.

By default, observer checks run every `30s`, require `1` successful sentinel out
of `4`, transition to `OBSERVER_DOWN` after `3` consecutive unhealthy observer
checks, and transition to `OBSERVER_UP` after `1` healthy observer check.

Leave `observer.sentinels` omitted or empty to use built-in public connectivity
checks:

- `gstatic`: `https://www.gstatic.com/generate_204`, expecting HTTP `204`
- `cloudflare`: `https://cp.cloudflare.com/generate_204`, expecting HTTP `204`
- `cloudflare-ip`: `http://1.1.1.1/cdn-cgi/trace`, expecting HTTP `301`;
  this sentinel uses a literal IPv4 address to bypass DNS resolution
- `msftconnecttest`: `http://www.msftconnecttest.com/connecttest.txt`,
  expecting HTTP `200`

Configure `observer.sentinels` to replace the built-ins:

```yaml
observer:
  enabled: true
  interval: 30s
  timeout: 5s
  failure_threshold: 3
  recovery_threshold: 1
  required_successes: 1
  sentinels:
    - id: edge
      name: Edge connectivity
      url: https://example.com/health
      expected_status_code: 200
```

Observer transitions create incidents with monitor ID `__observer__` and
transitions `OBSERVER_DOWN` or `OBSERVER_UP`. They use the same alert providers
and alert retry policy as monitor incidents.

### Monitors

Each monitor requires:

- `id`: stable identifier used as the SQLite primary key for monitor state.
- `name`: human-readable monitor name.
- `url`: `http` or `https` URL.
- `expected_status_code`: exact HTTP status code required for success.

Optional monitor fields:

- `interval`: monitor-specific check interval.
- `timeout`: monitor-specific HTTP request timeout.
- `failure_threshold`: monitor-specific consecutive failed checks required
  before this monitor transitions DOWN. Defaults to `defaults.failure_threshold`.
- `max_response_time`: maximum full response duration.
- `insecure_skip_verify`: disables HTTPS certificate verification when `true`.
- `follow_redirects`: follows HTTP redirects when `true`. Defaults to `false`.
- `max_redirects`: maximum number of redirect responses to follow. Defaults to
  `10` when `follow_redirects` is `true` and is invalid otherwise.
- `redirect_target.exact`: exact absolute final URL required after following at
  least one redirect.
- `redirect_target.regex`: Go regular expression that must match the entire
  absolute final URL after following at least one redirect. Configure either
  `exact` or `regex`, not both.
- `response_body.must_contain`: list of case-sensitive strings that must all
  appear in the response body.
- `response_body.must_not_contain`: list of case-sensitive strings that must
  all be absent from the response body.
- `response_body.command`: external command assertion expressed as an argv
  list. The first item is the executable and remaining items are arguments.
- `response_body.command_timeout`: maximum external command runtime. Defaults
  to `10s` when `response_body.command` is configured.

Redirects are not followed by default. For example, a monitor expecting `302`
succeeds when the first response is `302`. Redirect following and an optional
final-target assertion can be enabled per monitor:

```yaml
follow_redirects: true
max_redirects: 5
redirect_target:
  exact: https://example.com/final
  # Or use a full-URL regular expression instead:
  # regex: https://example\.com/users/[0-9]+
```

A redirect hop is one followed redirect response. A `max_redirects` value of
`2` permits two hops and rejects a third. Relative `Location` values are
resolved before the final URL is asserted. Exact matching uses the serialized
absolute final request URL without additional canonicalization or query sorting.
A configured `redirect_target` fails when the response does not follow at least
one redirect, even if the original request URL would otherwise match.

When redirects are followed, `expected_status_code`, `redirect_target`, body
assertions, and response-time assertions apply to the final response. Target and
body assertions are evaluated only after the observed status code matches
`expected_status_code`.

When `response_body.command` is configured, `upag` runs the command directly
without a shell and writes the exact response body bytes to the command's
standard input. Exit code `0` passes the assertion. A non-zero exit code, a
process start error, or a `command_timeout` expiry fails the assertion. Shell
features such as pipes, redirects, globbing, variable expansion, and `&&` are
available only when explicitly invoking a shell, for example:

```yaml
response_body:
  command: ["sh", "-c", "jq -e '.status == \"ok\"' | grep true"]
```

`latency_ms` in logs and stored probe history is time to response headers,
including the complete redirect chain when redirects are followed. When
`max_response_time` or `response_body` is configured, `response_time_ms` is the
full response duration, ending after the final response body has been read.
Otherwise, `response_time_ms` is `0`. External response body command runtime is
not included in `response_time_ms`.

## Operations

### Debian package

When installed from the Debian package, `upag` runs as the `upag` system user.
Configuration lives in `/etc/upag/config.yaml`, service defaults live in
`/etc/default/upag`, and the packaged config stores SQLite state in
`/var/lib/upag/upag.sqlite`.
Bare CLI commands read `/etc/default/upag` when it exists, so inspection
commands use the packaged config and PID file without extra flags.

Enable the packaged service only after configuring real alert credentials and
monitors:

```sh
sudoedit /etc/upag/config.yaml
sudoedit /etc/default/upag
sudo systemctl start upag
```

The systemd unit is enabled during package installation but gated by
`UPAG_ENABLED=false` in `/etc/default/upag`. Set `UPAG_ENABLED=true` when the
configuration is ready.

Manage the service:

```sh
sudo systemctl status upag
sudo systemctl reload upag
sudo systemctl restart upag
sudo systemctl stop upag
```

Inspect service logs:

```sh
journalctl -u upag
```

On non-systemd systems, use the SysV init script:

```sh
sudo /etc/init.d/upag start
sudo /etc/init.d/upag status
sudo /etc/init.d/upag reload
sudo /etc/init.d/upag stop
```

### Manual daemon

Run in the foreground:

```sh
upag run --config ./config.yaml
```

Run as a background daemon:

```sh
upag start --config ./config.yaml --pid-file ./upag.pid --log-file ./upag.log
upag status --pid-file ./upag.pid
upag restart --config ./config.yaml --pid-file ./upag.pid --log-file ./upag.log
upag stop --pid-file ./upag.pid
```

Reload configuration without restarting the daemon process:

```sh
upag config reload --pid-file ./upag.pid
```

Configuration reloads add new monitors, keep unchanged monitor workers running,
restart changed monitor workers, and stop scheduling monitors removed from the
YAML file. New and changed monitors use a deterministic initial delay so checks
are spread across each monitor interval.

Inspect stored state:

```sh
upag monitors
upag incidents --limit 50
upag failures --limit 20 --since 24h
upag intervals --monitor homepage --limit 20 --since 24h
```

`--since` on `incidents`, `failures`, and `intervals` accepts either an
RFC3339 timestamp or a positive Go duration. Duration values use Go
`time.ParseDuration` syntax: a sequence of decimal numbers with units `ns`,
`us` or `µs`, `ms`, `s`, `m`, and `h`, such as `30m`, `1.5h`, or `24h`.
Calendar units such as `d`, `w`, `mo`, and `y` are not supported; use hour
equivalents such as `168h` for seven days.

Manage one-off maintenance windows:

```sh
upag maintenance add --monitor homepage \
  --start 2026-06-23T01:00:00Z --end 2026-06-23T02:00:00Z \
  --reason "deploy"
upag maintenance list
upag maintenance list --all
upag maintenance cancel --id 1 --reason "finished"
```

Maintenance windows are attached to a single monitor. Checks still run during
maintenance. Failed checks covered by `starts_at <= checked_at < ends_at` do
not create alerts and do not count as reported downtime. Raw probe results are
still stored with their maintenance window ID. `maintenance add` rejects monitor
IDs that are not already present in `monitor_states` and rejects overlapping
non-cancelled windows for the same monitor. Audit fields default to the local
OS user; pass `--by` to override the recorded operator.

Print the binary version:

```sh
upag --version
```

The daemon writes line-oriented logs to stdout and stderr. When started with
`start`, both streams are appended to `--log-file`. On Linux, pass `--syslog`
to `run`, `start`, or `restart` to write daemon logs to syslog instead.

## Development

Common development commands:

```sh
make fmt
make vet
make test
make build
```

`make test` and `go test ./...` require Docker. The PostgreSQL storage tests
start a local `postgres:16-alpine` container automatically.

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
The Debian package is assembled with GoReleaser's nFPM integration.

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
