package replication

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteReadMessage(t *testing.T) {
	r, w := io.Pipe()

	go func() {
		err := WriteMessage(w, MsgWALData, []byte("hello"))
		assert.NoError(t, err)
		err = WriteMessage(w, MsgStandbyStatusUpdate, []byte{1, 2, 3})
		assert.NoError(t, err)
		err = WriteMessage(w, MsgPrimaryKeepAlive, nil)
		assert.NoError(t, err)
		w.Close()
	}()

	msgType, payload, err := ReadMessage(r)
	assert.NoError(t, err)
	assert.Equal(t, MsgWALData, msgType)
	assert.Equal(t, []byte("hello"), payload)

	msgType, payload, err = ReadMessage(r)
	assert.NoError(t, err)
	assert.Equal(t, MsgStandbyStatusUpdate, msgType)
	assert.Equal(t, []byte{1, 2, 3}, payload)

	msgType, payload, err = ReadMessage(r)
	assert.NoError(t, err)
	assert.Equal(t, MsgPrimaryKeepAlive, msgType)
	assert.Empty(t, payload)
}

func TestWALDataEncodeDecode(t *testing.T) {
	lsn := int64(42)
	record := []byte("test record data")

	encoded := EncodeWALData(lsn, record)
	gotLSN, gotRecord, err := DecodeWALData(encoded)
	assert.NoError(t, err)
	assert.Equal(t, lsn, gotLSN)
	assert.Equal(t, record, gotRecord)
}

func TestStandbyStatusUpdateEncodeDecode(t *testing.T) {
	replayLSN := int64(100)

	encoded := EncodeStandbyStatusUpdate(replayLSN)
	gotLSN, err := DecodeStandbyStatusUpdate(encoded)
	assert.NoError(t, err)
	assert.Equal(t, replayLSN, gotLSN)
}

func TestPrimaryKeepAliveEncodeDecode(t *testing.T) {
	tests := []struct {
		name          string
		lsn           int64
		replyRequired bool
	}{
		{"reply required", 55, true},
		{"no reply required", 99, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodePrimaryKeepAlive(tt.lsn, tt.replyRequired)
			gotLSN, gotReply, err := DecodePrimaryKeepAlive(encoded)
			assert.NoError(t, err)
			assert.Equal(t, tt.lsn, gotLSN)
			assert.Equal(t, tt.replyRequired, gotReply)
		})
	}
}
