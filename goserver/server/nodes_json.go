package server

import (
	"log"
)

// NodeJSON represents a node structure for JSON serialization
// You can expand this struct as needed to match your frontend
// For now, it matches the shape used in main.go's /doc POST handler
//
type NodeJSON struct {
	ID       string                 `json:"id"`
	Position map[string]float64     `json:"position"`
	Data     map[string]interface{} `json:"data"`
}

// GetNodesAsJSON extracts the nodes array from the Yjs document and returns it as JSON-compatible Go structs
// This is a stub implementation. You must implement the actual extraction from the Yjs document using FFI.
func (r *Room) GetNodesAsJSON() []NodeJSON {
	// TODO: Use Yrs FFI to extract the nodes array from the Yjs document
	// For now, return an empty array and log a warning
	log.Printf("GetNodesAsJSON: Extraction from Yjs document not implemented, returning empty array")
	return []NodeJSON{}
}
