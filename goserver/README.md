# Y-protocol WebSocket Server (Distributed)

A horizontally scalable Y-protocol compatible WebSocket server built with Go, using NATS JetStream for cross-server synchronization.

## Features

- ✅ **Y-protocol compatible** - Works with JavaScript `y-websocket` clients
- ✅ **Horizontally scalable** - Multiple server instances sync via NATS JetStream
- ✅ **Official Yrs FFI** - Uses official Rust Yrs CRDT library via CGo bindings
- ✅ **Awareness protocol** - Real-time presence and cursor tracking
- ✅ **Room-based** - Multi-document support with room isolation

## Architecture

```
Client 1 ──┐
           ├──> Server Instance 1 ──┐
Client 2 ──┘                        │
                                    ├──> NATS JetStream ──> All servers sync
Client 3 ──┐                        │
           ├──> Server Instance 2 ──┘
Client 4 ──┘
```

## Prerequisites

1. **NATS Server with JetStream** (for distributed mode)
   ```bash
   # Using Docker
   docker run -d -p 4222:4222 -p 8222:8222 nats:latest -js
   
   # Or install locally
   # https://docs.nats.io/running-a-nats-service/introduction/installation
   ```

2. **Rust & Cargo** (for building Yrs FFI)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

3. **Go 1.25+**

## Building

### 1. Build the Yrs FFI Library

```bash
cd yrs-ffi
cargo build --release
```

This creates `target/release/libyrs_ffi.dylib` (or `.so` on Linux).

### 2. Build the Go Server

```bash
cd goserver
go build -o yrs-server main.go
```

## Running

### Single Instance Mode (No NATS)

```bash
./yrs-server
```

The server will run without NATS and work fine for single-instance deployments.

### Distributed Mode (With NATS)

1. **Start NATS Server:**
   ```bash
   docker run -d -p 4222:4222 -p 8222:8222 nats:latest -js
   ```

2. **Start Server Instances:**
   ```bash
   # Terminal 1
   NATS_URL=nats://localhost:4222 SERVER_ID=server1 ./yrs-server
   
   # Terminal 2 (different port)
   NATS_URL=nats://localhost:4222 SERVER_ID=server2 PORT=8081 ./yrs-server
   ```

3. **Connect Clients:**
   ```javascript
   // Client connects to server1
   const provider1 = new WebsocketProvider('ws://localhost:8080/room1', ydoc);
   
   // Client connects to server2
   const provider2 = new WebsocketProvider('ws://localhost:8081/room1', ydoc);
   
   // Both clients will sync through NATS!
   ```

## Environment Variables

- `NATS_URL` - NATS server URL (default: `nats://localhost:4222`)
- `SERVER_ID` - Unique server identifier (default: hostname)
- `PORT` - HTTP server port (default: `8080`)

## How It Works

### Update Flow

1. **Client sends update** → Server Instance A
2. **Server A applies update** to local document
3. **Server A publishes** update to NATS JetStream
4. **All other servers** receive update via NATS subscription
5. **Other servers apply** update and broadcast to their local clients
6. **Loop prevention**: Messages include `server_id` to avoid re-broadcasting own messages

### Awareness Flow

Same as updates, but for presence/cursor information.

## NATS JetStream Configuration

The server creates a JetStream stream named `YJS_SYNC` with:
- **Subjects**: `yjs.sync.*` (one per room)
- **Retention**: Limits policy
- **Max Age**: 24 hours
- **Storage**: File storage

Each server creates a durable consumer to receive updates.

## Testing

1. Start NATS: `docker run -d -p 4222:4222 nats:latest -js`
2. Start two server instances on different ports
3. Connect clients to different servers
4. Make edits - they should sync across servers!

## Troubleshooting

### NATS Connection Failed

If you see "Warning: Failed to connect to NATS", the server will run in single-instance mode. Check:
- NATS server is running: `docker ps | grep nats`
- NATS URL is correct: `echo $NATS_URL`
- Firewall allows connection to NATS port (4222)

### Updates Not Syncing

- Verify NATS connection: Check server logs for "Connected to NATS"
- Check NATS subjects: `nats stream info YJS_SYNC`
- Verify room names match across servers
- Check server IDs are unique

## Production Considerations

1. **NATS Cluster**: Use NATS cluster for high availability
2. **Authentication**: Configure NATS authentication/authorization
3. **TLS**: Enable TLS for NATS connections
4. **Monitoring**: Monitor NATS JetStream metrics
5. **Load Balancing**: Use a load balancer in front of server instances
6. **Persistence**: Configure NATS persistence for message durability

