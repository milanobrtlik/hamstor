include .env
export

LDFLAGS = -X github.com/milan/hamstor/internal/creds.AWSAccessKeyID=$(AWS_ACCESS_KEY_ID) \
          -X github.com/milan/hamstor/internal/creds.AWSSecretAccessKey=$(AWS_SECRET_ACCESS_KEY) \
          -X github.com/milan/hamstor/internal/creds.AWSRegion=$(AWS_REGION) \
          -X github.com/milan/hamstor/internal/creds.Passphrase=$(HAMSTOR_PASSPHRASE)

build:
	@mkdir -p data
	go build -ldflags "$(LDFLAGS)" -o hamstor ./cmd/hamstor

install: build
	-sudo systemctl stop hamstor 2>/dev/null
	-sudo fusermount -uz $(HAMSTOR_MOUNT) 2>/dev/null
	sudo cp hamstor /usr/local/bin/hamstor
	sudo mkdir -p $(HAMSTOR_MOUNT)
	@printf '[Unit]\n\
Description=Hamstor FUSE filesystem\n\
After=network-online.target\n\
Wants=network-online.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStartPre=-/bin/umount -l $(HAMSTOR_MOUNT)\n\
ExecStartPre=/bin/mkdir -p $(HAMSTOR_MOUNT)\n\
ExecStart=/usr/local/bin/hamstor --mount $(HAMSTOR_MOUNT) --bucket $(HAMSTOR_BUCKET) --endpoint $(HAMSTOR_ENDPOINT) --db /var/lib/hamstor/hamstor.db\n\
ExecStop=/bin/umount $(HAMSTOR_MOUNT)\n\
Restart=on-failure\n\
RestartSec=5\n\
\n\
[Install]\n\
WantedBy=multi-user.target\n' > hamstor.service
	sudo mkdir -p /var/lib/hamstor
	sudo cp hamstor.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable --now hamstor

uninstall:
	-sudo systemctl disable --now hamstor 2>/dev/null
	-sudo fusermount -uz $(HAMSTOR_MOUNT) 2>/dev/null
	-sudo rm -f /etc/systemd/system/hamstor.service
	sudo systemctl daemon-reload
	-sudo rm -f /usr/local/bin/hamstor
	-sudo rm -rf /var/lib/hamstor

.PHONY: build install uninstall
