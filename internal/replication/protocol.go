package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Message types for the replication wire protocol.
// Frame format: [msgType(1)] [payloadLen(4)] [payload(N)]
const (
	MsgWALData             byte = 1
	MsgStandbyStatusUpdate byte = 2
	MsgPrimaryKeepAlive    byte = 3
)

// WriteMessage writes a framed message: [msgType(1)] [payloadLen(4)] [payload(N)].
func WriteMessage(w io.Writer, msgType byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

// ReadMessage reads a framed message and returns (msgType, payload, error).
func ReadMessage(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, fmt.Errorf("read header: %w", err)
	}
	msgType := header[0]
	payloadLen := binary.BigEndian.Uint32(header[1:5])
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("read payload: %w", err)
		}
	}
	return msgType, payload, nil
}

// EncodeWALData encodes a WALData payload: [lsn(8)] [recordBytes(N)].
func EncodeWALData(lsn int64, record []byte) []byte {
	buf := make([]byte, 8+len(record))
	binary.BigEndian.PutUint64(buf[0:8], uint64(lsn))
	copy(buf[8:], record)
	return buf
}

// DecodeWALData decodes a WALData payload into (lsn, recordBytes).
func DecodeWALData(payload []byte) (int64, []byte, error) {
	if len(payload) < 8 {
		return 0, nil, fmt.Errorf("WALData payload too short: %d bytes", len(payload))
	}
	lsn := int64(binary.BigEndian.Uint64(payload[0:8]))
	return lsn, payload[8:], nil
}

// EncodeStandbyStatusUpdate encodes a StandbyStatusUpdate payload: [replayLSN(8)].
func EncodeStandbyStatusUpdate(replayLSN int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(replayLSN))
	return buf
}

// DecodeStandbyStatusUpdate decodes a StandbyStatusUpdate payload into replayLSN.
func DecodeStandbyStatusUpdate(payload []byte) (int64, error) {
	if len(payload) < 8 {
		return 0, fmt.Errorf("StandbyStatusUpdate payload too short: %d bytes", len(payload))
	}
	return int64(binary.BigEndian.Uint64(payload[0:8])), nil
}

// EncodePrimaryKeepAlive encodes a PrimaryKeepAlive payload: [currentLSN(8)] [replyRequired(1)].
func EncodePrimaryKeepAlive(currentLSN int64, replyRequired bool) []byte {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[0:8], uint64(currentLSN))
	if replyRequired {
		buf[8] = 1
	}
	return buf
}

// DecodePrimaryKeepAlive decodes a PrimaryKeepAlive payload into (currentLSN, replyRequired).
func DecodePrimaryKeepAlive(payload []byte) (int64, bool, error) {
	if len(payload) < 9 {
		return 0, false, fmt.Errorf("PrimaryKeepAlive payload too short: %d bytes", len(payload))
	}
	lsn := int64(binary.BigEndian.Uint64(payload[0:8]))
	return lsn, payload[8] == 1, nil
}
