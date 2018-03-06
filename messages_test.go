package singer

import (
	"bytes"
	"testing"
	"time"
)

var (
	testLocation, _ = time.LoadLocation("America/New_York")
	testDateTime    = time.Date(2000, time.January, 1, 0, 0, 0, 0, testLocation)
	writer          = new(bytes.Buffer)
)

func TestWriteRecord(t *testing.T) {
	OUTPUT = writer
	result := string(`{"type":"RECORD","stream":"streamValue","record":{"name":"foo"},"version":"","time_extracted":"2000-01-01T00:00:00-05:00"}
`)
	WriteRecord("streamValue", []byte(`{"name": "foo"}`), "", "", testDateTime)
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