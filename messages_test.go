package singer

import (
	"bytes"
	"github.com/onsi/gomega"
	"testing"
)

var (
	writer = new(bytes.Buffer)
)

func TestWriteRecord(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	OUTPUT = writer
	defer writer.Reset()
	result := []byte(`{"type":"RECORD","stream":"streamValue","record":{"id":12,"name":"foo"}}`)
	WriteRecord("streamValue", []byte(`{"id":12,"name": "foo"}`))
	g.Expect(writer.String()).To(gomega.MatchJSON(result), "JSON records should match")
}

func TestWriteRecords(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	OUTPUT = writer
	defer writer.Reset()
	result := [][]byte{[]byte(`{"type":"RECORD","stream":"users","record":{"id":1,"name":"Chris"}}`),
		[]byte(`{"type":"RECORD","stream":"users","record":{"id":2,"name":"Mike"}}`)}
	records := [][]byte{[]byte(`{"id":1,"name":"Chris"}`), []byte(`{"id":2,"name":"Mike"}`)}
	WriteRecords("users", records)
	for i := 0; ; i++ {
		line, err := writer.ReadBytes(byte('\n'))
		if err != nil {
			break
		}
		g.Expect(line).To(gomega.MatchJSON(result[i]), "Each record should output on its own line and be a valid JSON object")
	}
}

func TestWriteSchema(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	OUTPUT = writer
	defer writer.Reset()
	result := []byte(`{"type":"SCHEMA","stream":"users","schema":{"properties":{"name":{"type":"string"}},"type":"object"},"key_properties":["name"]}
`)
	streamName := "users"
	schema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)
	keyProperties := []string{"name"}
	WriteSchema(streamName, schema, keyProperties)
	g.Expect(writer.String()).To(gomega.MatchJSON(result), "Output should be Schema JSON object")
	lastByte := writer.Bytes()[len(writer.Bytes())-1]
	g.Expect(lastByte).To(gomega.Equal(byte('\n')), "Output should end in line break")
}
