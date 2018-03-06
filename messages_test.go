package singer

import (
	"bytes"
	"testing"
	"time"
)

var (
	writer          = new(bytes.Buffer)
)

func TestWriteRecord(t *testing.T) {
	OUTPUT = writer
	result := string(`{"type":"RECORD","stream":"streamValue","record":{"id":12,"name":"foo"},"version":"","time_extracted":"0001-01-01T00:00:00Z"}
`)
	WriteRecord("streamValue", []byte(`{"id":12,"name": "foo"}`), "", "", time.Time{})
	if writer.String() != result {
		t.Error("Expected: ", result,
			"Got: ", writer.String())
	}
	writer.Reset()
}

func TestWriteSchema(t *testing.T) {
	OUTPUT = writer
	result := string(`{"type":"SCHEMA","stream":"users","schema":{"properties":{"name":{"type":"string"}},"type":"object"},"key_properties":["name"]}
`)
	WriteSchema("users", []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`), []string {"name"})
	if writer.String() != result {
		t.Error("Expected:", result, "Got:", writer.String())
	}
	writer.Reset()
}