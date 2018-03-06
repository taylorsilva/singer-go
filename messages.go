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

type recordMessage struct {
	Type          string
	Stream        string
	Record        map[string]interface{} // a json copy of the record, must UnMarshal
	Version       string
	TimeExtracted time.Time
}

// Record should be json encoded already, type []byte. This ensures that when we encode the entire struct
// that the whole record is proper json
func newRecordMessage(stream string, jsonRecord []byte, version string, timeExtracted time.Time) (*recordMessage, error) {
	var r map[string]interface{}
	err := json.Unmarshal(jsonRecord, &r) // this will reorder the keys so they're alphabetical
	if err != nil {
		return nil, err
	}

	return &recordMessage{
		Type:          "RECORD",
		Stream:        stream,
		Record:        r,
		Version:       version,
		TimeExtracted: timeExtracted,
	}, nil
}

// returns the record and excludes unused fields like version and
// extracted_time if they're equal to their zero values
func (r *recordMessage) AsMap() map[string]interface{} {
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

type schemaMessage struct {
	Type          string
	Stream        string
	Schema        map[string]interface{} // a json copy of the schema, must UnMarshals
	KeyProperties []string
	Bookmarks     []string
}

func newSchemaMessage(stream string, schemaJson []byte, keyProperties []string, bookmarks []string) (*schemaMessage, error) {
	var s map[string]interface{}
	err := json.Unmarshal(schemaJson, &s)
	if err != nil {
		return nil, err
	}

	return &schemaMessage{
		Type:          "SCHEMA",
		Stream:        stream,
		Schema:        s,
		KeyProperties: keyProperties,
		Bookmarks: bookmarks,
	}, nil
}

func (s *schemaMessage) AsMap() map[string]interface{} {
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

type stateMessage struct {
	Type  string `json:"type"`
	Value map[string]interface{}
}

// Value should be a json encoded string
func newStateMessage(value []byte) (*stateMessage, error) {
	var v map[string]interface{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return nil, err
	}

	return &stateMessage{
		Type:  "STATE",
		Value: v,
	}, nil
}

type activateVersionMessage struct {
	Type    string `json:"type"`
	Stream  string `json:"stream"`
	Version string `json:"version"`
}

func newActivateVersionMsg(stream string, version string) *activateVersionMessage {
	return &activateVersionMessage{
		Type:    "ACTIVATE_VERSION",
		Stream:  stream,
		Version: version,
	}
}

func writeMessage(msg Message) {
	// TODO: research easyjson for faster encoding
	json.NewEncoder(OUTPUT).Encode(msg.AsMap())
}

func WriteRecord(stream string, jsonRecord []byte, version string, streamAlias string, timeExtracted time.Time) error {
	msg, err := newRecordMessage(stream, jsonRecord, version, timeExtracted)
	if err != nil {
		return err
	}
	writeMessage(msg)
	return nil
}

func WriteRecords(stream string, jsonRecords [][]byte, version string, streamAlias string, timeExtracted time.Time) error {
	for _, record := range jsonRecords {
		err := WriteRecord(stream, record, version, streamAlias, timeExtracted)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteSchema(stream string, schemaJson []byte, keyProperties []string, bookmarks []string) error {
	msg, err := newSchemaMessage(stream, schemaJson, keyProperties, bookmarks)
	if err != nil {
		return err
	}
	json.NewEncoder(OUTPUT).Encode(msg)
	return nil
}

func WriteState(jsonValues []byte) error {
	msg, err := newStateMessage(jsonValues)
	if err != nil {
		return err
	}
	json.NewEncoder(OUTPUT).Encode(msg)
	return nil
}

func WriteActiveVersion(stream string, version string) {
	msg := newActivateVersionMsg(stream, version)
	json.NewEncoder(OUTPUT).Encode(msg)
}

// returns one of the above message types
func ParseMessage(jsonMsg []byte) (interface{}, error) {
	return nil, nil
}
