package singer

import (
	"encoding/json"
	"os"
	"time"
	"io"
)

var (
	OUTPUT io.Writer = os.Stdout
)

type recordMessage struct {
	Type          string                 `json:"type"`
	Stream        string                 `json:"stream"`
	Record        map[string]interface{} `json:"record"` // a json copy of the record, must UnMarshal
	Version       string                 `json:"version"`
	TimeExtracted time.Time              `json:"time_extracted"`
}

// Record should be json encoded already, type []byte. This ensures that when we encode the entire struct
// that the whole record is proper json
func newRecordMessage(stream string, jsonRecord []byte, version string, timeExtracted time.Time) (*recordMessage, error) {
	var r map[string]interface{}
	err := json.Unmarshal(jsonRecord, &r)
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

type schemaMessage struct {
	Type          string                 `json:"type"`
	Stream        string                 `json:"stream"`
	Schema        map[string]interface{} `json:"schema"` // a json copy of the schema, must UnMarshals
	KeyProperties []string               `json:"key_properties"`
}

func newSchemaMessage(stream string, schemaJson []byte, keyProperties []string) (*schemaMessage, error) {
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
	}, nil
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

func WriteRecord(stream string, jsonRecord []byte, version string, streamAlias string, timeExtracted time.Time) error {
	msg, err := newRecordMessage(stream, jsonRecord, version, timeExtracted)
	if err != nil {
		return err
	}
	json.NewEncoder(OUTPUT).Encode(msg)
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

func WriteSchema(stream string, schemaJson []byte, keyProperties []string) error {
	msg, err := newSchemaMessage(stream, schemaJson, keyProperties)
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
