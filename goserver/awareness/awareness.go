package awareness

import (
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"
)

// Awareness manages client awareness states
type Awareness struct {
	mu       sync.RWMutex
	clientID uint64
	states   map[uint64]json.RawMessage
	meta     map[uint64]*Meta
	onChange []func(*AwarenessEvent)
}

// Meta contains metadata for a client's awareness state
type Meta struct {
	Clock       uint32
	LastUpdated time.Time
}

// AwarenessEvent represents changes in awareness states
type AwarenessEvent struct {
	Added   []uint64
	Updated []uint64
	Removed []uint64
}

// NewAwareness creates a new awareness instance
func NewAwareness(clientID uint64) *Awareness {
	return &Awareness{
		clientID: clientID,
		states:   make(map[uint64]json.RawMessage),
		meta:     make(map[uint64]*Meta),
		onChange: make([]func(*AwarenessEvent), 0),
	}
}

// ClientID returns the client ID
func (a *Awareness) ClientID() uint64 {
	return a.clientID
}

// GetLocalState returns the local client's state
func (a *Awareness) GetLocalState() json.RawMessage {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.states[a.clientID]
}

// SetLocalState sets the local client's state
func (a *Awareness) SetLocalState(state json.RawMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.states[a.clientID] = state
	meta := a.meta[a.clientID]
	if meta == nil {
		meta = &Meta{}
		a.meta[a.clientID] = meta
	}
	meta.Clock++
	meta.LastUpdated = time.Now()
}

// GetStates returns all awareness states
func (a *Awareness) GetStates() map[uint64]json.RawMessage {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[uint64]json.RawMessage)
	for k, v := range a.states {
		result[k] = v
	}
	return result
}

// EncodeAwarenessUpdate encodes awareness update in lib0 v1 format
// Format: [num_clients (varuint)] [client_id (varuint), clock (uint32), state_len (varuint), state_json]...
func (a *Awareness) EncodeAwarenessUpdate(clients []uint64) []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()

	buf := make([]byte, 0, 256)

	// Write number of clients
	buf = appendVarUint(buf, uint64(len(clients)))

	for _, clientID := range clients {
		// Write client ID
		buf = appendVarUint(buf, clientID)

		// Write clock (fixed 4 bytes, little-endian)
		meta := a.meta[clientID]
		var clock uint32
		if meta != nil {
			clock = meta.Clock
		}
		clockBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(clockBytes, clock)
		buf = append(buf, clockBytes...)

		// Write state
		state, exists := a.states[clientID]
		if !exists || state == nil {
			// Null state - write 0 length
			buf = appendVarUint(buf, 0)
		} else {
			// Write state length and JSON
			buf = appendVarUint(buf, uint64(len(state)))
			buf = append(buf, state...)
		}
	}

	return buf
}

// ApplyAwarenessUpdate applies an awareness update received from another peer
func (a *Awareness) ApplyAwarenessUpdate(update []byte, origin interface{}) *AwarenessEvent {
	a.mu.Lock()
	defer a.mu.Unlock()

	event := &AwarenessEvent{
		Added:   make([]uint64, 0),
		Updated: make([]uint64, 0),
		Removed: make([]uint64, 0),
	}

	pos := 0

	// Read number of clients
	numClients, n := readVarUint(update[pos:])
	pos += n

	now := time.Now()

	for i := 0; i < int(numClients); i++ {
		// Read client ID
		clientID, n := readVarUint(update[pos:])
		pos += n

		// Read clock (fixed 4 bytes)
		if pos+4 > len(update) {
			break
		}
		clock := binary.LittleEndian.Uint32(update[pos : pos+4])
		pos += 4

		// Read state length
		stateLen, n := readVarUint(update[pos:])
		pos += n

		var state json.RawMessage
		if stateLen > 0 {
			if pos+int(stateLen) > len(update) {
				break
			}
			state = make(json.RawMessage, stateLen)
			copy(state, update[pos:pos+int(stateLen)])
			pos += int(stateLen)
		}

		// Get current meta for this client
		meta := a.meta[clientID]
		isNew := meta == nil

		if isNew || clock > meta.Clock {
			// Update or add state
			if stateLen == 0 {
				// Remove state (client disconnected)
				delete(a.states, clientID)
				delete(a.meta, clientID)
				event.Removed = append(event.Removed, clientID)
			} else {
				// Add or update state
				if isNew {
					meta = &Meta{}
					a.meta[clientID] = meta
					event.Added = append(event.Added, clientID)
				} else {
					event.Updated = append(event.Updated, clientID)
				}

				a.states[clientID] = state
				meta.Clock = clock
				meta.LastUpdated = now
			}
		}
	}

	// Trigger onChange callbacks
	if len(event.Added) > 0 || len(event.Updated) > 0 || len(event.Removed) > 0 {
		for _, callback := range a.onChange {
			callback(event)
		}
	}

	return event
}

// RemoveStates removes awareness states for given clients
func (a *Awareness) RemoveStates(clients []uint64, origin interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	event := &AwarenessEvent{
		Removed: make([]uint64, 0, len(clients)),
	}

	for _, clientID := range clients {
		if _, exists := a.states[clientID]; exists {
			delete(a.states, clientID)
			delete(a.meta, clientID)
			event.Removed = append(event.Removed, clientID)
		}
	}

	if len(event.Removed) > 0 {
		for _, callback := range a.onChange {
			callback(event)
		}
	}
}

// OnChange registers a callback for awareness changes
func (a *Awareness) OnChange(callback func(*AwarenessEvent)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onChange = append(a.onChange, callback)
}

// Helper functions for varuint encoding/decoding (lib0 format)

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

	for {
		if pos >= len(buf) {
			return num, pos
		}

		b := buf[pos]
		pos++

		num += uint64(b&0x7f) * mult
		mult *= 128

		if b < 0x80 {
			return num, pos
		}
	}
}
