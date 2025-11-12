package sync

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// MessageType represents the type of sync message
type MessageType uint8

const (
	MessageTypeUpdate    MessageType = 0
	MessageTypeAwareness MessageType = 1
	MessageTypeServerID  MessageType = 2
)

// SyncMessage represents a message to be synced across servers
type SyncMessage struct {
	Type      MessageType `json:"type"`
	RoomName  string      `json:"room_name"`
	Data      []byte      `json:"data"`
	ServerID  string      `json:"server_id"`
	Timestamp int64       `json:"timestamp"`
}

// SyncService handles cross-server synchronization via NATS JetStream
type SyncService struct {
	nc          *nats.Conn
	js          nats.JetStreamContext
	kv          nats.KeyValue
	serverID    string
	subscribers map[string]*nats.Subscription
	mu          sync.RWMutex
	onMessage   func(roomName string, msgType MessageType, data []byte)
}

// NewSyncService creates a new NATS sync service
func NewSyncService(natsURL string, serverID string) (*SyncService, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	// Create stream if it doesn't exist
	streamName := "YJS_SYNC"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"yjs.sync.>"},
		Retention: nats.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   nats.FileStorage,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		nc.Close()
		return nil, err
	}

	// Create KV store for document persistence
	kvStoreName := "YJS_DOCS"
	kv, err := js.KeyValue(kvStoreName)
	if err != nil {
		// KV store doesn't exist, create it
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      kvStoreName,
			Description: "Yjs document state storage",
			Storage:     nats.FileStorage,
		})
		if err != nil {
			nc.Close()
			return nil, err
		}
	}

	service := &SyncService{
		nc:          nc,
		js:          js,
		kv:          kv,
		serverID:    serverID,
		subscribers: make(map[string]*nats.Subscription),
	}

	return service, nil
}

// PublishUpdate publishes a document update to NATS
func (s *SyncService) PublishUpdate(roomName string, update []byte) error {
	msg := SyncMessage{
		Type:      MessageTypeUpdate,
		RoomName:  roomName,
		Data:      update,
		ServerID:  s.serverID,
		Timestamp: time.Now().UnixNano(),
	}

	return s.publish(roomName, msg)
}

// PublishAwareness publishes an awareness update to NATS
func (s *SyncService) PublishAwareness(roomName string, awarenessUpdate []byte) error {
	msg := SyncMessage{
		Type:      MessageTypeAwareness,
		RoomName:  roomName,
		Data:      awarenessUpdate,
		ServerID:  s.serverID,
		Timestamp: time.Now().UnixNano(),
	}

	return s.publish(roomName, msg)
}

// publish sends a message to NATS JetStream
func (s *SyncService) publish(roomName string, msg SyncMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	subject := s.subjectForRoom(roomName)
	_, err = s.js.Publish(subject, data)
	return err
}

// SubscribeToRoom subscribes to updates for a specific room
func (s *SyncService) SubscribeToRoom(roomName string, handler func(msgType MessageType, data []byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already subscribed
	if _, exists := s.subscribers[roomName]; exists {
		return nil
	}

	subject := s.subjectForRoom(roomName)

	// Create consumer with durable name (sanitize server ID)
	safeServerID := sanitizeConsumerName(s.serverID)
	consumerName := "yjs-consumer-" + safeServerID

	sub, err := s.js.Subscribe(subject, func(msg *nats.Msg) {
		var syncMsg SyncMessage
		if err := json.Unmarshal(msg.Data, &syncMsg); err != nil {
			log.Printf("Error unmarshaling sync message: %v", err)
			msg.Ack()
			return
		}

		// Ignore messages from this server
		if syncMsg.ServerID == s.serverID {
			msg.Ack()
			return
		}

		// Call handler
		handler(syncMsg.Type, syncMsg.Data)
		msg.Ack()
	}, nats.Durable(consumerName), nats.DeliverNew())

	if err != nil {
		return err
	}

	s.subscribers[roomName] = sub
	return nil
}

// UnsubscribeFromRoom unsubscribes from a room
func (s *SyncService) UnsubscribeFromRoom(roomName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, exists := s.subscribers[roomName]
	if !exists {
		return nil
	}

	err := sub.Unsubscribe()
	delete(s.subscribers, roomName)
	return err
}

// sanitizeConsumerName sanitizes a string for use as a NATS consumer name
// Consumer names can only contain alphanumeric, dashes, and underscores
func sanitizeConsumerName(name string) string {
	result := ""
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result += string(c)
		} else {
			result += "_"
		}
	}
	// Ensure it's not empty and doesn't start/end with dash or underscore
	if result == "" {
		result = "default"
	}
	// Remove leading/trailing dashes and underscores
	for len(result) > 0 && (result[0] == '-' || result[0] == '_') {
		result = result[1:]
	}
	for len(result) > 0 && (result[len(result)-1] == '-' || result[len(result)-1] == '_') {
		result = result[:len(result)-1]
	}
	if result == "" {
		result = "default"
	}
	return result
}

// subjectForRoom returns the NATS subject for a room
func (s *SyncService) subjectForRoom(roomName string) string {
	// Sanitize room name for NATS subject
	safeName := roomName
	if safeName == "" || safeName == "/" {
		safeName = "default"
	}
	// Remove leading slash and replace slashes with dots
	if len(safeName) > 0 && safeName[0] == '/' {
		safeName = safeName[1:]
	}
	return "yjs.sync." + safeName
}

// Close closes the NATS connection
func (s *SyncService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sub := range s.subscribers {
		sub.Unsubscribe()
	}
	s.subscribers = nil

	if s.nc != nil {
		s.nc.Close()
	}
}

// ServerID returns the server ID
func (s *SyncService) ServerID() string {
	return s.serverID
}

// LoadRoomHistory loads all historical updates for a room from JetStream
// Returns updates sorted by timestamp (oldest first)
func (s *SyncService) LoadRoomHistory(roomName string) ([]SyncMessage, error) {
	subject := s.subjectForRoom(roomName)

	// Create a unique consumer name per room (sanitize both server ID and room name)
	safeRoomName := roomName
	if safeRoomName == "" || safeRoomName == "/" {
		safeRoomName = "default"
	}
	if len(safeRoomName) > 0 && safeRoomName[0] == '/' {
		safeRoomName = safeRoomName[1:]
	}

	// Sanitize both server ID and room name for NATS consumer name
	safeServerID := sanitizeConsumerName(s.serverID)
	safeRoomNameSanitized := sanitizeConsumerName(safeRoomName)
	consumerName := "yjs-history-" + safeServerID + "-" + safeRoomNameSanitized

	// Limit consumer name length (NATS has a max length, typically 64 chars)
	if len(consumerName) > 64 {
		// Truncate room name part if needed
		maxRoomLen := 64 - len("yjs-history-") - len(safeServerID) - len("-")
		if maxRoomLen > 0 && len(safeRoomNameSanitized) > maxRoomLen {
			safeRoomNameSanitized = safeRoomNameSanitized[:maxRoomLen]
		}
		consumerName = "yjs-history-" + safeServerID + "-" + safeRoomNameSanitized
		if len(consumerName) > 64 {
			// If still too long, truncate both parts
			maxServerLen := 32
			maxRoomLen := 20
			if len(safeServerID) > maxServerLen {
				safeServerID = safeServerID[:maxServerLen]
			}
			if len(safeRoomNameSanitized) > maxRoomLen {
				safeRoomNameSanitized = safeRoomNameSanitized[:maxRoomLen]
			}
			consumerName = "yjs-history-" + safeServerID + "-" + safeRoomNameSanitized
		}
	}

	// Delete existing consumer if it exists (to reset position for fresh load)
	_ = s.js.DeleteConsumer("YJS_SYNC", consumerName)

	// Create a pull consumer to fetch all messages
	_, err := s.js.AddConsumer("YJS_SYNC", &nats.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    1,
	})
	if err != nil {
		return nil, err
	}

	// Create pull subscription
	sub, err := s.js.PullSubscribe(subject, consumerName, nats.Bind("YJS_SYNC", consumerName))
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	msgs := make([]SyncMessage, 0)

	// Fetch messages in batches
	batchSize := 100
	for {
		fetched, err := sub.Fetch(batchSize, nats.MaxWait(1*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				// No more messages
				break
			}
			return nil, err
		}

		if len(fetched) == 0 {
			break
		}

		for _, msg := range fetched {
			var syncMsg SyncMessage
			if err := json.Unmarshal(msg.Data, &syncMsg); err != nil {
				log.Printf("Error unmarshaling historical message: %v", err)
				msg.Ack()
				continue
			}

			// Only collect update messages (not awareness)
			if syncMsg.Type == MessageTypeUpdate {
				msgs = append(msgs, syncMsg)
			}
			msg.Ack()
		}

		// If we got fewer than batchSize, we're done
		if len(fetched) < batchSize {
			break
		}
	}

	// Sort messages by timestamp (oldest first)
	for i := 0; i < len(msgs)-1; i++ {
		for j := i + 1; j < len(msgs); j++ {
			if msgs[i].Timestamp > msgs[j].Timestamp {
				msgs[i], msgs[j] = msgs[j], msgs[i]
			}
		}
	}

	// Clean up the temporary consumer
	_ = s.js.DeleteConsumer("YJS_SYNC", consumerName)

	return msgs, nil
}

// SaveDocumentState saves the full document state to KV store
func (s *SyncService) SaveDocumentState(roomName string, docState []byte) error {
	if s.kv == nil {
		return nil // KV store not available
	}

	key := s.keyForRoom(roomName)
	_, err := s.kv.Put(key, docState)
	if err != nil {
		log.Printf("Error saving document state for room %s: %v", roomName, err)
		return err
	}
	log.Printf("Saved document state for room %s (%d bytes)", roomName, len(docState))
	return nil
}

// LoadDocumentState loads the full document state from KV store
func (s *SyncService) LoadDocumentState(roomName string) ([]byte, error) {
	if s.kv == nil {
		return nil, nil // KV store not available, return nil (no error)
	}

	key := s.keyForRoom(roomName)
	entry, err := s.kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, nil // No document state found, return nil (not an error)
		}
		log.Printf("Error loading document state for room %s: %v", roomName, err)
		return nil, err
	}

	state := make([]byte, len(entry.Value()))
	copy(state, entry.Value())
	log.Printf("Loaded document state for room %s (%d bytes)", roomName, len(state))
	return state, nil
}

// keyForRoom returns the KV key for a room
func (s *SyncService) keyForRoom(roomName string) string {
	// Sanitize room name for KV key
	safeName := roomName
	if safeName == "" || safeName == "/" {
		safeName = "default"
	}
	// Remove leading slash
	if len(safeName) > 0 && safeName[0] == '/' {
		safeName = safeName[1:]
	}
	return safeName
}
