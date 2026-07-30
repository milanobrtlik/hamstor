include .env
export

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

LDFLAGS = -X main.version=$(VERSION) \
          -X github.com/milan/hamstor/internal/creds.AWSAccessKeyID=$(AWS_ACCESS_KEY_ID) \
          -X github.com/milan/hamstor/internal/creds.AWSSecretAccessKey=$(AWS_SECRET_ACCESS_KEY) \
          -X github.com/milan/hamstor/internal/creds.AWSRegion=$(AWS_REGION) \
          -X github.com/milan/hamstor/internal/creds.Passphrase=$(HAMSTOR_PASSPHRASE)

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install it with:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	golangci-lint run ./...
	go vet ./...

build:
# Silenced deliberately: LDFLAGS carries the S3 access key, the secret key and
# the encryption passphrase, so echoing the command prints all three. They then
# live in terminal scrollback, in any CI log, and in whatever captured the build
# output — none of which is where a bucket credential or the only key to the
# stored data belongs.
	@echo "go build -o hamstor ./cmd/hamstor"
	@go build -ldflags "$(LDFLAGS)" -o hamstor ./cmd/hamstor

install: build
	@{ \
		printf '%s\n' '[Unit]'; \
		printf '%s\n' 'Description=Hamstor FUSE filesystem'; \
		printf '%s\n' 'After=network-online.target'; \
		printf '%s\n' 'Wants=network-online.target'; \
		printf '%s\n' ''; \
		printf '%s\n' '[Service]'; \
		printf '%s\n' 'Type=simple'; \
		printf '%s\n' 'ExecStartPre=-/bin/umount -l $(HAMSTOR_MOUNT)'; \
		printf '%s\n' 'ExecStartPre=/bin/mkdir -p $(HAMSTOR_MOUNT)'; \
		printf '%s\n' 'ExecStart=/usr/local/bin/hamstor --mount $(HAMSTOR_MOUNT) --bucket $(HAMSTOR_BUCKET) --endpoint $(HAMSTOR_ENDPOINT) --db /var/lib/hamstor/hamstor.db --uid $(shell id -u) --gid $(shell id -g)'; \
		printf '%s\n' 'ExecStop=/bin/umount $(HAMSTOR_MOUNT)'; \
		printf '%s\n' 'Restart=on-failure'; \
		printf '%s\n' 'RestartSec=5'; \
		printf '%s\n' ''; \
		printf '%s\n' '[Install]'; \
		printf '%s\n' 'WantedBy=multi-user.target'; \
	} > hamstor.service
	sudo sh -c '\
		systemctl stop hamstor 2>/dev/null; \
		fusermount -uz $(HAMSTOR_MOUNT) 2>/dev/null; \
		cp hamstor /usr/local/bin/hamstor && \
		mkdir -p $(HAMSTOR_MOUNT) && \
		mkdir -p /var/lib/hamstor && \
		cp hamstor.service /etc/systemd/system/ && \
		systemctl daemon-reload && \
		systemctl enable --now hamstor'

uninstall:
	sudo sh -c '\
		systemctl disable --now hamstor 2>/dev/null; \
		fusermount -uz $(HAMSTOR_MOUNT) 2>/dev/null; \
		rm -f /etc/systemd/system/hamstor.service; \
		systemctl daemon-reload; \
		rm -f /usr/local/bin/hamstor; \
		rm -rf /var/lib/hamstor; \
		true'

purge-s3: build
	@systemctl is-active --quiet hamstor 2>/dev/null && { echo "Error: hamstor service is running. Run 'make uninstall' first."; exit 1; } || true
	@echo "WARNING: This will delete ALL data in S3 bucket '$(HAMSTOR_BUCKET)' and the local database!"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || { echo "Aborted."; exit 1; }
# sudo for the same reason install and uninstall have it: no --db here means the
# compiled-in default /var/lib/hamstor/hamstor.db, which install creates as root.
# purge-s3 is exempt from the must-exist check on purpose ("I lost the database,
# wipe the bucket"), so unprivileged it does not stop at "no database" — it goes
# on to create the directory and dies there.
	sudo ./hamstor --bucket $(HAMSTOR_BUCKET) --endpoint $(HAMSTOR_ENDPOINT) purge-s3

.PHONY: lint build install uninstall purge-s3
