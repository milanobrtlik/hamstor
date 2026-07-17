include .env
export

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

LDFLAGS = -X main.version=$(VERSION) \
          -X github.com/milan/hamstor/internal/creds.AWSAccessKeyID=$(AWS_ACCESS_KEY_ID) \
          -X github.com/milan/hamstor/internal/creds.AWSSecretAccessKey=$(AWS_SECRET_ACCESS_KEY) \
          -X github.com/milan/hamstor/internal/creds.AWSRegion=$(AWS_REGION) \
          -X github.com/milan/hamstor/internal/creds.Passphrase=$(HAMSTOR_PASSPHRASE)

build:
	@mkdir -p data
	go build -ldflags "$(LDFLAGS)" -o hamstor ./cmd/hamstor

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
	./hamstor --bucket $(HAMSTOR_BUCKET) --endpoint $(HAMSTOR_ENDPOINT) purge-s3

.PHONY: build install uninstall purge-s3
