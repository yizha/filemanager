package meta

import ()

type ValueType int

const (
	TypeString ValueType = iota
	TypeInt
)

type Value interface {
	Type() ValueType
}

type StringValue string

func (s StringValue) Type() ValueType {
	return TypeString
}

func (s StringValue) Value() string {
	return string(s)
}

type IntValue int

func (i IntValue) Type() ValueType {
	return TypeInt
}

func (i IntValue) Value() int {
	return int(i)
}

type Metadata map[string]Value

type BlobMeta struct {
	id   string
	meta Metadata
}

func NewBlobMeta(id string) *BlobMeta {
	return &BlobMeta{
		id:   id,
		meta: make(map[string]Value),
	}
}

func (m *BlobMeta) ID() string {
	return m.id
}

func (m *BlobMeta) Meta() Metadata {
	return m.meta
}

func (m *BlobMeta) Add(k string, v Value) {
	m.meta[k] = v
}

type MetaExtractResult struct {
	Error    error
	BlobMeta *BlobMeta
}

func NewMetaExtractErr(err error) *MetaExtractResult {
	return &MetaExtractResult{
		Error:    err,
		BlobMeta: nil,
	}
}

func NewMetaExtractResult(meta *BlobMeta) *MetaExtractResult {
	return &MetaExtractResult{
		Error:    nil,
		BlobMeta: meta,
	}
}
