package singer

import (
	"testing"
	"time"
	"bytes"
)

var (
	testLocation, _ = time.LoadLocation("America/New_York")
	testDateTime = time.Date(2000, time.January, 1,0,0,0,0, testLocation)
)

func TestWriteRecord(t *testing.T) {
	var writer = new(bytes.Buffer)
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