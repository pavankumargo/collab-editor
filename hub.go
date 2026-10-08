package main

// Message wraps the binary payload with its destination room.
type Message struct {
	room    string
	payload []byte
}

type Hub struct {
	// Registered clients, partitioned by room ID.
	rooms map[string]map[*Client]bool

	// Inbound messages to broadcast to a room.
	broadcast chan Message

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		rooms:      make(map[string]map[*Client]bool),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			// Create the room map if it doesn't exist yet
			if h.rooms[client.room] == nil {
				h.rooms[client.room] = make(map[*Client]bool)
			}
			h.rooms[client.room][client] = true

		case client := <-h.unregister:
			if _, ok := h.rooms[client.room][client]; ok {
				delete(h.rooms[client.room], client)
				close(client.send)
				// Garbage collection: remove empty rooms to save memory
				if len(h.rooms[client.room]) == 0 {
					delete(h.rooms, client.room)
				}
			}

		case message := <-h.broadcast:
			// Get all clients in the target room
			connections := h.rooms[message.room]
			for client := range connections {
				select {
				case client.send <- message.payload:
				default:
					// Backpressure handling: if the client's send buffer is full,
					// assume they are dead/hung, forcefully disconnect them, and drop the connection.
					close(client.send)
					delete(h.rooms[message.room], client)
				}
			}
		}
	}
}