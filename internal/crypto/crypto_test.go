package crypto

import (
	"bytes"
	"testing"
)

var testSalt = []byte("0123456789abcdef") // 16 bytes

func newTestEncryptor(t *testing.T, passphrase string) *Encryptor {
	t.Helper()
	enc, err := New(passphrase, testSalt)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return enc
}

func TestRoundTrip(t *testing.T) {
	enc := newTestEncryptor(t, "test-passphrase")
	plain := []byte("hello world")

	ct, err := enc.Encrypt(plain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	got, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %q, want %q", got, plain)
	}
}

func TestRoundTripEmpty(t *testing.T) {
	enc := newTestEncryptor(t, "test-passphrase")

	ct, err := enc.Encrypt([]byte{})
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	got, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d bytes", len(got))
	}
}

func TestWrongKey(t *testing.T) {
	enc1 := newTestEncryptor(t, "passphrase-1")
	enc2 := newTestEncryptor(t, "passphrase-2")

	ct, err := enc1.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	_, err = enc2.Decrypt(ct)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}
}

func TestCorruptedData(t *testing.T) {
	enc := newTestEncryptor(t, "test-passphrase")

	ct, err := enc.Encrypt([]byte("data"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Flip a byte in the ciphertext
	ct[len(ct)-1] ^= 0xff

	_, err = enc.Decrypt(ct)
	if err == nil {
		t.Fatal("expected error on corrupted ciphertext")
	}
}

func TestTooShort(t *testing.T) {
	enc := newTestEncryptor(t, "test-passphrase")

	_, err := enc.Decrypt([]byte{Version1, 0, 0})
	if err == nil {
		t.Fatal("expected error on too-short data")
	}
}

func TestNonceUniqueness(t *testing.T) {
	enc := newTestEncryptor(t, "test-passphrase")
	plain := []byte("same data")

	ct1, _ := enc.Encrypt(plain)
	ct2, _ := enc.Encrypt(plain)

	if bytes.Equal(ct1, ct2) {
		t.Fatal("two encryptions of same data should produce different ciphertext")
	}
}

func TestIsEncrypted(t *testing.T) {
	if IsEncrypted(nil) {
		t.Fatal("nil should not be encrypted")
	}
	if IsEncrypted([]byte{}) {
		t.Fatal("empty should not be encrypted")
	}
	if IsEncrypted([]byte{0x00, 0x01}) {
		t.Fatal("version 0x00 should not be encrypted")
	}
	if !IsEncrypted([]byte{Version1, 0x01}) {
		t.Fatal("version 0x01 should be encrypted")
	}
}

func TestGenerateSalt(t *testing.T) {
	s1, err := GenerateSalt()
	if err != nil {
		t.Fatalf("GenerateSalt: %v", err)
	}
	if len(s1) != SaltLen {
		t.Fatalf("salt len %d, want %d", len(s1), SaltLen)
	}
	s2, _ := GenerateSalt()
	if bytes.Equal(s1, s2) {
		t.Fatal("two salts should differ")
	}
}
