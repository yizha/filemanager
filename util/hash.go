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
	if h.hex == "" {
		h.hex = hex.EncodeToString(h.data)
	}
	return h.hex
}

// String returns the string representation of the hash object
// in format [algorithm]:[hash-value-in-hex]
func (h *Hash) String() string {
	if h.str == "" {
		h.str = fmt.Sprintf("%s:%s", h.alg, h.Hex())
	}
	return h.str
}

// NewHash returns a Hash object with given hash algorithm and
// hash value in []byte.
func newHash(alg string, data []byte) *Hash {
	return &Hash{
		alg:  alg,
		data: data,
		hex:  "",
		str:  "",
	}
}

// NewSha1Hash creates a Hash object with "sha1" algorithm and
// given []byte as hash value.
func NewSha1Hash(data []byte) *Hash {
	return newHash("sha1", data)
}
