package server

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
	"go-yjs-server/encoding"
	"go-yjs-server/protocol"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
)

// Client represents a WebSocket client connection
type Client struct {
	conn     *websocket.Conn
	room     *Room
	clientID uint64
	send     chan []byte
}

// NewClient creates a new client
func NewClient(conn *websocket.Conn, room *Room, clientID uint64) *Client {
	return &Client{
		conn:     conn,
		room:     room,
		clientID: clientID,
		send:     make(chan []byte, 256),
	}
}

// ReadPump reads messages from the WebSocket
func (c *Client) ReadPump() {
	defer func() {
		c.room.RemoveClient(c)
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		c.handleMessage(message)
	}
}

// WritePump sends messages to the WebSocket
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				log.Printf("Write error: %v", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage processes incoming messages
func (c *Client) handleMessage(data []byte) {
	if len(data) == 0 {
		return
	}

	decoder := encoding.NewDecoder(data)
	messageType, err := decoder.ReadVarUint()
	if err != nil {
		log.Printf("Error reading message type: %v", err)
		return
	}

	encoder := &messageEncoder{}

	switch messageType {
	case MessageSync:
		c.handleSync(decoder, encoder)
	case MessageAwareness:
		c.handleAwareness(decoder)
	case MessageQueryAwareness:
		c.handleQueryAwareness(encoder)
	case MessageAuth:
		c.handleAuth(decoder)
	default:
		log.Printf("Unknown message type: %d", messageType)
	}

	if len(encoder.buf) > 0 {
		c.send <- encoder.bytes()
	}
}

// handleSync handles sync protocol messages
func (c *Client) handleSync(decoder *encoding.Decoder, encoder *messageEncoder) {
	syncType, err := decoder.ReadVarUint()
	if err != nil {
		return
	}

	encoder.writeVarUint(MessageSync)

	switch syncType {
	case protocol.SyncStep1:
		// Client sent their state vector, send them missing updates
		stateVector, err := decoder.ReadUint8Array()
		if err != nil {
			return
		}

		txn := c.room.GetDoc().ReadTransaction()
		update := txn.StateDiff(stateVector)
		txn.Commit()

		encoder.writeVarUint(protocol.SyncStep2)
		encoder.writeBytes(update)

		c.send <- encoder.bytes()

	case protocol.SyncStep2:
		// Client sent update during sync
		update, err := decoder.ReadUint8Array()
		if err != nil {
			return
		}

		txn := c.room.GetDoc().WriteTransaction(nil)
		if err := txn.Apply(update); err != nil {
			log.Printf("Error applying update: %v", err)
			return
		}
		txn.Commit()

		// Publish to NATS for cross-server sync
		c.room.PublishUpdate(update)

		// Broadcast update to other local clients
		broadcastEncoder := &messageEncoder{}
		broadcastEncoder.writeVarUint(MessageSync)
		broadcastEncoder.writeVarUint(protocol.Update)
		broadcastEncoder.writeBytes(update)

		c.room.Broadcast(broadcastEncoder.bytes(), c)

	case protocol.Update:
		// Client sent update
		update, err := decoder.ReadUint8Array()
		if err != nil {
			return
		}

		txn := c.room.GetDoc().WriteTransaction(nil)
		if err := txn.Apply(update); err != nil {
			log.Printf("Error applying update: %v", err)
			return
		}
		txn.Commit()

		// Publish to NATS for cross-server sync
		c.room.PublishUpdate(update)

		// Broadcast update to other local clients
		broadcastEncoder := &messageEncoder{}
		broadcastEncoder.writeVarUint(MessageSync)
		broadcastEncoder.writeVarUint(protocol.Update)
		broadcastEncoder.writeBytes(update)

		c.room.Broadcast(broadcastEncoder.bytes(), c)
	}
}

// handleAwareness handles awareness protocol messages
func (c *Client) handleAwareness(decoder *encoding.Decoder) {
	awarenessUpdate, err := decoder.ReadUint8Array()
	if err != nil {
		return
	}

	c.room.GetAwareness().ApplyAwarenessUpdate(awarenessUpdate, c)

	// Publish to NATS for cross-server sync
	c.room.PublishAwareness(awarenessUpdate)

	// Broadcast to other local clients
	encoder := &messageEncoder{}
	encoder.writeVarUint(MessageAwareness)
	encoder.writeBytes(awarenessUpdate)

	c.room.Broadcast(encoder.bytes(), c)
}

// handleQueryAwareness handles awareness query messages
func (c *Client) handleQueryAwareness(encoder *messageEncoder) {
	states := c.room.GetAwareness().GetStates()
	clients := make([]uint64, 0, len(states))
	for clientID := range states {
		clients = append(clients, clientID)
	}

	awarenessUpdate := c.room.GetAwareness().EncodeAwarenessUpdate(clients)

	encoder.writeVarUint(MessageAwareness)
	encoder.writeBytes(awarenessUpdate)

	c.send <- encoder.bytes()
}

// handleAuth handles authentication messages
func (c *Client) handleAuth(decoder *encoding.Decoder) {
	// Implement authentication if needed
	// For now, just accept all connections
}

// SendInitialSync sends the initial sync message to the client
func (c *Client) SendInitialSync() {
	// Send sync step 1 (empty state vector to request client's state)
	txn := c.room.GetDoc().ReadTransaction()
	stateVector := txn.StateVector()
	txn.Commit()

	encoder := &messageEncoder{}
	encoder.writeVarUint(MessageSync)
	encoder.writeVarUint(protocol.SyncStep1)
	encoder.writeBytes(stateVector)

	c.send <- encoder.bytes()
}

// messageEncoder is a helper for encoding messages
type messageEncoder struct {
	buf []byte
}

func (e *messageEncoder) writeVarUint(num uint64) {
	for num > 0x7f {
		e.buf = append(e.buf, byte(0x80|(num&0x7f)))
		num >>= 7
	}
	e.buf = append(e.buf, byte(num&0x7f))
}

func (e *messageEncoder) writeBytes(data []byte) {
	e.writeVarUint(uint64(len(data)))
	e.buf = append(e.buf, data...)
}

func (e *messageEncoder) bytes() []byte {
	return e.buf
}

