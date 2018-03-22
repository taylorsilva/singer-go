package singer

import (
	"encoding/json"
	"io"
	"os"
	"time"
)

var (
	OUTPUT io.Writer = os.Stdout
)

// JSON keys for all message types
const (
	KEYTYPE          = "type"
	KEYSTREAM        = "stream"
	KEYRECORD        = "record"
	KEYSCHEMA        = "schema"
	KEYVERSION       = "version"
	KEYTIMEEXTRACTED = "time_extracted"
	KEYPROPERTIES    = "key_properties"
	KEYBOOKMARK      = "bookmark_properties"
	KEYVALUE         = "value"
)

type Message interface {
	AsMap() map[string]interface{}
}

type RecordMessage struct {
	Type          string
	Stream        string
	Record        map[string]interface{} // a json copy of the record, must UnMarshal
	Version       string
	TimeExtracted time.Time
}

// Record should be json encoded already, type []byte. This ensures that when we encode the entire struct
// that the whole record is proper json
func newRecordMessage(stream string, jsonRecord []byte, version string, timeExtracted time.Time) (*RecordMessage, error) {
	var r map[string]interface{}
	err := json.Unmarshal(jsonRecord, &r) // this will reorder the keys so they're alphabetical
	if err != nil {
		return nil, err
	}

	return &RecordMessage{
		Type:          "RECORD",
		Stream:        stream,
		Record:        r,
		Version:       version,
		TimeExtracted: timeExtracted,
	}, nil
}

// returns the record and excludes unused fields like version and
// extracted_time if they're equal to their zero values
func (r RecordMessage) AsMap() map[string]interface{} {
	msg := map[string]interface{}{
		KEYTYPE:   r.Type,
		KEYSTREAM: r.Stream,
		KEYRECORD: r.Record,
	}
	if r.Version != "" {
		msg[KEYVERSION] = r.Version
	}
	if !r.TimeExtracted.Equal(time.Time{}) {
		msg[KEYTIMEEXTRACTED] = r.TimeExtracted
	}
	return msg
}

type SchemaMessage struct {
	Type          string
	Stream        string
	Schema        map[string]interface{} // a json copy of the schema, must UnMarshals
	KeyProperties []string
	Bookmarks     []string
}

func newSchemaMessage(stream string, schemaJson []byte, keyProperties []string, bookmarks []string) (*SchemaMessage, error) {
	var s map[string]interface{}
	err := json.Unmarshal(schemaJson, &s)
	if err != nil {
		return nil, err
	}

	return &SchemaMessage{
		Type:          "SCHEMA",
		Stream:        stream,
		Schema:        s,
		KeyProperties: keyProperties,
		Bookmarks:     bookmarks,
	}, nil
}

func (s SchemaMessage) AsMap() map[string]interface{} {
	msg := map[string]interface{}{
		KEYTYPE:       s.Type,
		KEYSTREAM:     s.Stream,
		KEYSCHEMA:     s.Schema,
		KEYPROPERTIES: s.KeyProperties,
	}
	if len(s.Bookmarks) > 0 {
		msg[KEYBOOKMARK] = s.Bookmarks
	}
	return msg
}

type StateMessage struct {
	Type  string
	Value map[string]interface{}
}

// Value should be a json encoded string
func newStateMessage(value []byte) (*StateMessage, error) {
	var v map[string]interface{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return nil, err
	}

	return &StateMessage{
		Type:  "STATE",
		Value: v,
	}, nil
}

func (s StateMessage) AsMap() map[string]interface{} {
	msg := map[string]interface{}{
		KEYTYPE:  s.Type,
		KEYVALUE: s.Value,
	}
	return msg
}

func writeMessage(msg Message) {
	// TODO: research easyjson for faster encoding
	// Encode() adds a line break
	json.NewEncoder(OUTPUT).Encode(msg.AsMap())
}

func WriteRecord(stream string, jsonRecord []byte) error {
	err := WriteRecordExtras(stream, jsonRecord, "", "", time.Time{})
	if err != nil {
		return err
	}
	return nil
}

func WriteRecordExtras(stream string, jsonRecord []byte, streamAlias string, version string,
	timeExtracted time.Time) error {
	msg, err := newRecordMessage(stream, jsonRecord, version, timeExtracted)
	if err != nil {
		return err
	}
	writeMessage(msg)
	return nil
}

func WriteRecords(stream string, jsonRecords [][]byte) error {
	err := WriteRecordsExtras(stream, jsonRecords, "", "", time.Time{})
	if err != nil {
		return err
	}
	return nil
}

func WriteRecordsExtras(stream string, jsonRecords [][]byte, version string, streamAlias string, timeExtracted time.Time) error {
	for _, record := range jsonRecords {
		err := WriteRecordExtras(stream, record, version, streamAlias, timeExtracted)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteSchema(stream string, schemaJson []byte, keyProperties []string) error {
	return WriteSchemaExtras(stream, schemaJson, keyProperties, []string{})
}

func WriteSchemaExtras(stream string, schemaJson []byte, keyProperties []string, bookmarks []string) error {
	msg, err := newSchemaMessage(stream, schemaJson, keyProperties, bookmarks)
	if err != nil {
		return err
	}
	writeMessage(msg)
	return nil
}

func WriteState(jsonValues []byte) error {
	msg, err := newStateMessage(jsonValues)
	if err != nil {
		return err
	}
	writeMessage(msg)
	return nil
}

// returns one of the above message types
func ParseMessage(jsonMsg []byte) (interface{}, error) {
	return nil, nil
}
