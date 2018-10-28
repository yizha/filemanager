package blob

import ()

// Storage is the interface for blob storage
type Storage interface {
	Store(chan Blob) StoreStatus
}

type StoreStatus interface {
	LoadStatus
	SkipCount() int
	SkipSize() int64
}
