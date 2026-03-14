package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	Version1     byte = 0x01
	SaltLen           = 16
	KeyLen            = 32 // AES-256
	NonceLen          = 12 // GCM standard
	ArgonTime         = 3
	ArgonMemory       = 64 * 1024 // 64 MB
	ArgonThreads      = 4
)

type Encryptor struct {
	aead cipher.AEAD
}

// New derives a 256-bit key from passphrase+salt using Argon2id
// and creates an AES-256-GCM cipher.
func New(passphrase string, salt []byte) (*Encryptor, error) {
	key := argon2.IDKey([]byte(passphrase), salt, ArgonTime, ArgonMemory, ArgonThreads, KeyLen)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: new gcm: %w", err)
	}
	return &Encryptor{aead: aead}, nil
}

// GenerateSalt returns a cryptographically random salt.
func GenerateSalt() ([]byte, error) {
	salt := make([]byte, SaltLen)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("crypto: generate salt: %w", err)
	}
	return salt, nil
}

// Encrypt returns [version][nonce][ciphertext+tag].
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, NonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: generate nonce: %w", err)
	}
	out := make([]byte, 1+NonceLen, 1+NonceLen+len(plaintext)+e.aead.Overhead())
	out[0] = Version1
	copy(out[1:], nonce)
	out = e.aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

// Decrypt parses [version][nonce][ciphertext+tag] and returns plaintext.
func (e *Encryptor) Decrypt(data []byte) ([]byte, error) {
	if len(data) < 1+NonceLen+e.aead.Overhead() {
		return nil, errors.New("crypto: ciphertext too short")
	}
	if data[0] != Version1 {
		return nil, fmt.Errorf("crypto: unknown version %d", data[0])
	}
	nonce := data[1 : 1+NonceLen]
	ciphertext := data[1+NonceLen:]
	plaintext, err := e.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt: %w", err)
	}
	return plaintext, nil
}

// IsEncrypted checks if data begins with a known version byte.
func IsEncrypted(data []byte) bool {
	return len(data) > 0 && data[0] == Version1
}
