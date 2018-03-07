package singer

import (
	"bytes"
	"testing"
	"github.com/onsi/gomega"
)

var (
	writer          = new(bytes.Buffer)
)

func TestWriteRecord(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	OUTPUT = writer
	result := []byte(`{"type":"RECORD","stream":"streamValue","record":{"id":12,"name":"foo"}}`)
	WriteRecord("streamValue", []byte(`{"id":12,"name": "foo"}`))
	g.Expect(writer.String()).To(gomega.MatchJSON(result), "Single record should match")
	writer.Reset()
}

func TestWriteRecords(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	OUTPUT = writer
	result := [][]byte {[]byte(`{"type":"RECORD","stream":"users","record":{"id":1,"name":"Chris"}}`),
		[]byte(`{"type":"RECORD","stream":"users","record":{"id":2,"name":"Mike"}}`)}
	records := [][]byte {[]byte(`{"id":1,"name":"Chris"}`),[]byte(`{"id":2,"name":"Mike"}`)}
	WriteRecords("users", records)
	for i := 0; ;i++ {
		line, err := writer.ReadBytes(byte('\n'))
		if err != nil {
			break
		}
		g.Expect(line).To(gomega.MatchJSON(result[i]), "Each record should output on its own line")
	}
	writer.Reset()
}

func TestWriteSchema(t *testing.T) {
	OUTPUT = writer
	result := string(`{"type":"SCHEMA","stream":"users","schema":{"properties":{"name":{"type":"string"}},"type":"object"},"key_properties":["name"]}
`)
	WriteSchema("users", []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`), []string {"name"}, []string{})
	if writer.String() != result {
		t.Error("Expected:", result, "Got:", writer.String())
	}
	writer.Reset()
}