package protocol

// Sync protocol message types
const (
	SyncStep1 = 0
	SyncStep2 = 1
	Update    = 2
)

// WriteSyncStep1 writes a sync step 1 message (state vector request)
// Note: stateVector should already be encoded with length prefix if needed
func WriteSyncStep1(stateVector []byte) []byte {
	buf := make([]byte, 0, len(stateVector)+10)
	buf = appendVarUint(buf, SyncStep1)
	buf = append(buf, stateVector...)
	return buf
}

// WriteSyncStep2 writes a sync step 2 message (update)
// Note: update should already be encoded with length prefix if needed
func WriteSyncStep2(update []byte) []byte {
	buf := make([]byte, 0, len(update)+10)
	buf = appendVarUint(buf, SyncStep2)
	buf = append(buf, update...)
	return buf
}

// WriteUpdate writes an update message
// Note: update should already be encoded with length prefix if needed
func WriteUpdate(update []byte) []byte {
	buf := make([]byte, 0, len(update)+10)
	buf = appendVarUint(buf, Update)
	buf = append(buf, update...)
	return buf
}

// ReadSyncMessage reads and processes a sync protocol message
func ReadSyncMessage(data []byte) (syncType uint64, payload []byte, bytesRead int) {
	syncType, n := readVarUint(data)
	return syncType, data[n:], n
}

func appendVarUint(buf []byte, num uint64) []byte {
	for num > 0x7f {
		buf = append(buf, byte(0x80|(num&0x7f)))
		num >>= 7
	}
	buf = append(buf, byte(num&0x7f))
	return buf
}

func readVarUint(buf []byte) (uint64, int) {
	var num uint64
	var mult uint64 = 1
	pos := 0

	for pos < len(buf) {
		b := buf[pos]
		pos++

		num += uint64(b&0x7f) * mult
		mult *= 128

		if b < 0x80 {
			return num, pos
		}
	}

	return num, pos
}
