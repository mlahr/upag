APP_NAME := upag
CONFIG ?= ./config.yaml
DB ?= ./upag.sqlite
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin

.PHONY: all build clean fmt help incidents install run status test tidy vet

all: fmt vet test build

help:
	@printf '%s\n' \
		'Default target: all' \
		'' \
		'Targets:' \
		'  all        Run fmt, vet, test, and build' \
		'  build      Build ./$(APP_NAME)' \
		'  clean      Remove build and coverage artifacts' \
		'  fmt        Format Go source files' \
		'  incidents  Show recent incidents using DB=$(DB)' \
		'  install    Install $(APP_NAME) to $(BINDIR)' \
		'  run        Run with CONFIG=$(CONFIG) and DB=$(DB)' \
		'  status     Show monitor status using DB=$(DB)' \
		'  test       Run all Go tests' \
		'  tidy       Tidy Go module dependencies' \
		'  vet        Run go vet'

build:
	go build -o $(APP_NAME) .

clean:
	rm -f $(APP_NAME) coverage.out coverage.html

fmt:
	go fmt ./...

incidents:
	go run . incidents --db $(DB) --limit 50

install: build
	install -d $(BINDIR)
	install -m 0755 $(APP_NAME) $(BINDIR)/$(APP_NAME)

run:
	go run . run --config $(CONFIG) --db $(DB)

status:
	go run . status --db $(DB)

test:
	go test ./...

tidy:
	go mod tidy

vet:
	go vet ./...
