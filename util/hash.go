package util

import (
	"encoding/hex"
	"fmt"
)

// Hash represents the hash of some algorithm
type Hash struct {
	alg  string
	data []byte
	hex  string
	str  string
}

// Algorithm returns the hash algorithm name.
func (h *Hash) Algorithm() string {
	return h.alg
}

// Bytes returns the hash value in []byte.
func (h *Hash) Bytes() []byte {
	return h.data
}

// Hex returns the hash value in hex.
func (h *Hash) Hex() string {
	return h.hex
}

// String returns the string representation of the hash object
// in format [algorithm]:[hash-value-in-hex]
func (h *Hash) String() string {
	return h.str
}

// NewSha1Hash creates a Hash object with "sha1" algorithm and
// the given []byte as hash value.
func NewSha1Hash(data []byte) *Hash {
	hexStr := hex.EncodeToString(data)
	return &Hash{
		alg:  "sha1",
		data: data,
		hex:  hexStr,
		str:  fmt.Sprintf("%s:%s", "sha1", hexStr),
	}
}

// NewSha1HashFromHex creates a Hash object with "sha1" algorithm
// and the given hex string as hash value.
func NewSha1HashFromHex(s string) (*Hash, error) {
	data, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return &Hash{
		alg:  "sha1",
		data: data,
		hex:  s,
		str:  fmt.Sprintf("%s:%s", "sha1", s),
	}, nil
}
