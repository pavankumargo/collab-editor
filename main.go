package main

import (
	"log"
	"net/http"
	"github.com/gorilla/websocket"
)

// The Upgrader converts a standard HTTP request into a persistent WebSocket connection.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// System Design: In production, we restrict this to your Vercel domain to prevent CSRF attacks.
	// For local development, we allow all origins.
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// handleWebSocket intercepts the HTTP request at the /ws endpoint
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade the connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Failed to upgrade connection:", err)
		return
	}
	// Ensure the connection closes when this function exits (we will modify this later)
	defer conn.Close()

	log.Println("New client connected from:", r.RemoteAddr)
    
	// We will hand this 'conn' off to our concurrent workers in the next step.
}

func main() {
	// Route traffic hitting /ws to our handler
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("WebSocket server starting on :8080...")
	
	// Start the HTTP server. This blocks forever, listening for requests.
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	
	// Initialize the client
	client := &Client{
		conn: conn, 
		send: make(chan []byte, 256), // Buffered channel
	}

	// Start the read and write pumps in separate concurrent goroutines
	go client.writePump()
	go client.readPump()
}