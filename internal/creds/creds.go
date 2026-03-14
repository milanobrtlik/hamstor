package creds

// Set at build time via -ldflags -X
var (
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSRegion          string
	Passphrase         string
)
