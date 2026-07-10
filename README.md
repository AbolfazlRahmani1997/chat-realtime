# Chat Service

Real-time WebSocket-based chat service with room management, message delivery/read receipts, file sharing, presence tracking, and horizontal scaling support. Built in Go with a hexagonal architecture.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Environment Variables](#environment-variables)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [WebSocket Protocol](#websocket-protocol)
- [Integration Guide](#integration-guide)
- [Deployment](#deployment)

---

## Architecture

### High-Level Design

```
┌─────────────┐     ┌──────────────────────────────────────────────┐
│   Client    │────▶│              Gin HTTP Server                 │
│ (Web/Mobile)│     │  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
└─────────────┘     │  │ REST API │  │  Admin   │  │ WebSocket │  │
                    │  │  Routes  │  │  Routes  │  │  Upgrade  │  │
                    │  └────┬─────┘  └────┬─────┘  └─────┬─────┘  │
                    └───────┼──────────────┼──────────────┼────────┘
                            │              │              │
                            ▼              ▼              ▼
                    ┌──────────────────────────────────────────┐
                    │              Hub (In-Memory)              │
                    │  ┌─────────┐ ┌──────────┐ ┌───────────┐  │
                    │  │  Rooms  │ │  Users   │ │  Clients  │  │
                    │  │ Manager │ │  Pool    │ │  Manager  │  │
                    │  └────┬────┘ └────┬─────┘ └─────┬─────┘  │
                    └───────┼───────────┼──────────────┼────────┘
                            │           │              │
        ┌───────────────────┼───────────┼──────────────┼──────────────┐
        │                   │           │              │              │
        ▼                   ▼           ▼              ▼              ▼
┌──────────────┐   ┌─────────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│   MongoDB /  │   │  Redis      │ │  Kafka   │ │RabbitMQ │ │  MinIO   │
│   ScyllaDB   │   │(Cache/PubSub│ │(Messages)│ │ (Rooms) │ │ (Files)  │
│  (Messages,  │   │ /Presence)  │ │          │ │         │ │          │
│   Rooms,     │   └─────────────┘ └──────────┘ └──────────┘ └──────────┘
│   Users)     │
└──────────────┘
```

### Architectural Pattern: Hexagonal (Ports & Adapters)

The codebase follows a **hexagonal architecture** with clear separation between domain logic, ports, and adapters:

| Layer | Directory | Responsibility |
|-------|-----------|----------------|
| **Domain** | `internal/ws/`, `internal/core/domain/` | Core entities (Room, Message, User, Client), Hub (in-memory state machine) |
| **Ports** | `ports/`, `internal/core/ports/` | Interfaces for repositories, services, and message brokers |
| **Adapters** | `adapters/`, `internal/adapters/` | Concrete implementations: MongoDB, ScyllaDB, Kafka producer/consumer, RabbitMQ, MinIO |
| **Handlers** | `router/`, `internal/admin/`, `internal/ws/ws_handler.go` | HTTP/WS route handlers (Gin) |
| **Config** | `config/`, root `.env` | Environment variables, mock users |

### Data Flow

#### Message Lifecycle

```
Client A ──sendMessage──▶ Hub (in-memory) ──broadcast──▶ Clients in Room B
                                │
                          ┌─────┴──────┐
                          ▼            ▼
                      Kafka         Redis Pub/Sub
                   (async persist)   (cross-instance)
                          │
                          ▼
                    MongoDB/ScyllaDB
                          │
                          ▼
                    Ntfy.sh (push notification to offline users)
```

#### Room Creation

```
External Service ──RabbitMQ──▶ Chat Service ──▶ MongoDB ──▶ WebSocket notification to online users
       OR
HTTP POST /chat/api/createRoom ──▶ Chat Service (direct)
```

### Horizontal Scaling

```
┌───────────┐  ┌───────────┐  ┌───────────┐
│ Server 1  │  │ Server 2  │  │ Server 3  │
│   Hub     │  │   Hub     │  │   Hub     │
└─────┬─────┘  └─────┬─────┘  └─────┬─────┘
      └───────────────┼──────────────┘
                      ▼
            Redis Pub/Sub
       (cross-instance broadcast)
```

Rooms are in-memory per instance. When a user sends a message, the instance persists it (via Kafka → DB) and publishes to Redis Pub/Sub. All other instances receive the Pub/Sub message and broadcast to their local clients in that room.

### Hub (In-Memory State Machine)

The central `Hub` struct manages all real-time state using goroutine-safe channels:

- **Register/Unregister** — client connection lifecycle
- **JoinRoom/LeaveRoom** — room membership per connection
- **Broadcast** — distribute messages within a room
- **Manager** — periodic cleanup and room status updates
- **UpdateUserPool** — periodic token validation against Gateway

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | Go 1.25 |
| HTTP/WS Framework | Gin + Gorilla WebSocket |
| Primary Database | MongoDB (via `mongo-go-driver`) |
| Alternative Database | ScyllaDB / Cassandra (via `gocql`) |
| Cache & Pub/Sub | Redis |
| Message Queue (Messages) | Kafka (Sarama) |
| Message Queue (Rooms) | RabbitMQ |
| File Storage | MinIO (S3-compatible) |
| Push Notifications | Ntfy.sh |
| Auth | External Gateway Service (HTTP) |
| ID Generation | ULID |
| Container Runtime | Docker + Kubernetes |
| Reverse Proxy | Nginx (with WS support) |

---

## Prerequisites

- **Go 1.25+** (for local development)
- **MongoDB** (or ScyllaDB)
- **Redis**
- **Kafka** (optional, for async message persistence)
- **RabbitMQ** (optional, for external room creation)
- **MinIO** (optional, for file uploads)
- An external **Gateway/Auth Service** that exposes `GET /api/user` with `Authorization: <token>` header

---

## Environment Variables

Create a `.env` file in the project root:

```env
# Server
CHAT_WS_PORT=8080

# Database Driver (mongo or scylla)
DB_DRIVER=mongo

# MongoDB
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
MONGO_DB_USERNAME=user
MONGO_DB_PASSWORD=password
CHAT_DB=chat_db
MAIN_DB=main_db

# ScyllaDB (when DB_DRIVER=scylla)
SCYLLA_HOSTS=localhost
SCYLLA_PORT=9042
SCYLLA_KEYSPACE=chat

# Redis
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=

# Gateway (Auth Service)
GATEWAY_URL=http://localhost:8000

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=chat.messages

# RabbitMQ (for external room creation)
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_HOST=localhost
RABBITMQ_QUEUE=chat.rooms

# MinIO (file storage)
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET=chat-files

# Ntfy (push notifications)
NTFY_TOPIC=chat-notifications
NTFY_URL=https://ntfy.sh

# Support chat (comma-separated admin user IDs)
SUPPORT_ADMIN_IDS=10,11,12

# Mock users (development only)
MOCK_USERS_ENABLED=1
```

---

## Getting Started

### Local Development

```bash
go mod download
go run ./cmd/main.go
```

Server starts on `http://0.0.0.0:CHAT_WS_PORT` (default 8080).

### Docker Compose

```bash
docker-compose up --build
```

Spins up: 3 chat server replicas, MongoDB, Redis, Kafka, RabbitMQ, MinIO, Nginx (WS load balancer).

---

## API Reference

### REST Endpoints

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `POST` | `/chat/api/createRoom` | Create a new room | No |
| `PUT` | `/chat/api/editRoom/:roomId` | Edit room properties | Token* |
| `PUT` | `/chat/api/room/:roomId/avatar` | Update room avatar | Token* |
| `GET` | `/chat/me` | Get current user info | Token |
| `GET` | `/chat/mock-users` | List mock users (dev) | No |
| `GET` | `/chat/room/pin/:roomId` | Toggle room pin | Token |
| `GET` | `/chat/room/notification/:roomId` | Toggle room notification | Token |
| `GET` | `/chat/user/last-seen/:userId` | Get user's last seen | Token |
| `POST` | `/chat/files` | Upload a file | Token* |
| `GET` | `/chat/files/:fileId` | Download/serve file | Token* |

> `Token*` = token passed as query param `?token=...` or `Authorization` header.

### Admin Endpoints

Prefix: `/chat/api/admin`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/message/` | List messages (paginated, filterable) |
| `GET` | `/message/:messageId` | Get single message |
| `GET` | `/room/` | List rooms (paginated, filterable) |
| `GET` | `/room/:roomId` | Get single room details |
| `PUT` | `/room/:roomId/deactive` | Deactivate a room |
| `PUT` | `/room/:roomId/active` | Activate a room |

### WebSocket Endpoints

| Path | Description |
|------|-------------|
| `GET /chat/ws` | **Unified WebSocket** — single connection for all rooms + user notifications (recommended) |
| `GET /chat/ws/joinRoom/:roomId` | Legacy: connect to a specific room (one WS per room) |
| `GET /chat/ws/seenMessage/:roomId` | Legacy: mark messages as read |
| `GET /chat/ws/getRooms/` | Legacy: user room list + notification stream |
| `GET /chat/ws/syncRooms/` | Legacy: sync additional connections |
| `GET /chat/ws/getClients/:roomId` | Get online clients in a room |

---

## WebSocket Protocol

### Unified WebSocket (`/chat/ws`)

This is the **recommended** endpoint. A single connection handles rooms, messages, and user-level notifications.

#### Connection

```
ws://localhost:8080/chat/ws?token=<TOKEN>
```

#### Server → Client Events

| `event_type` | Payload | Description |
|---|---|---|
| `init` | `{ rooms: Room[], userId: string }` | Initial room list on connect |
| `roomHistory` | `{ roomId, messages[] }` | Paginated message history |
| `roomMessage` | `{ message }` | New message in a joined room |
| `deliverMessage` | `{ messageId, userIds[] }` | Delivery confirmation |
| `incomeMessage` | `{ message, room }` | New message (user not in room) |
| `roomStatus` | `{ roomId, status }` | Room online/offline change |
| `createNewRoom` | `{ room }` | New room created (via MQ or API) |
| `seenMessage` | `{ roomId, userId, messageId }` | Read receipt |
| `error` | `{ message }` | Error notification |

#### Client → Server Events

| `event_type` | Payload | Description |
|---|---|---|
| `listRooms` | `{ item: "1" }` | Request fresh room list (page number) |
| `seenMessage` | `{ roomId, messageId, userId }` | Mark message as read |
| `joinRoom` | `{ roomId }` | Join a room's message stream |
| `leaveRoom` | `{ roomId }` | Leave a room's message stream |
| `sendMessage` | `{ roomId, content, ulid }` | Send a message |
| `forwardMessage` | `{ roomId, messageId }` | Forward a message |
| `roomHistory` | `{ roomId, page }` | Request paginated history |
| `ping` | `{ ulid: "ping" }` | Keep-alive |

### Legacy: Per-Room WebSocket (`/chat/ws/joinRoom/:roomId`)

```
ws://localhost:8080/chat/ws/joinRoom/<roomId>?token=<TOKEN>&page=1
```

1. Server validates membership and sends message history.
2. Client sends messages as JSON:
   ```json
   { "ulid": "<unique-id>", "content": "message text" }
   ```
3. Server broadcasts enriched message to all clients in the room.
4. `{ "ulid": "ping" }` serves as keep-alive.

### Support Room (Auto-Created)

On first WebSocket connection (`/chat/ws` or `/chat/ws/getRooms/`), the service checks for a room with ID `support-<userId>`. If it doesn't exist:

- Creates a room with ID `support-<userId>`, name `"پشتیبانی"` (Support).
- Members: the user (Owner/Writer roles) + configured `SUPPORT_ADMIN_IDS` (Writer roles).
- Stored in MongoDB; returned in the room list.

---

## Integration Guide

### 1) Auth Service (Gateway)

Your Gateway must expose:

```http
GET {GATEWAY_URL}/api/user
Authorization: <token>
```

**Success (200):**
```json
{
  "id": "user-123",
  "username": "john",
  "firstname": "John",
  "lastname": "Doe",
  "avatar": "https://..."
}
```

**Non-200:** The chat service treats the token as invalid.

### 2) Creating Rooms

#### Option A: HTTP API (direct)

```http
POST /chat/api/createRoom
Content-Type: application/json

{
  "Id": "room-uuid",
  "Name": "Room Name",
  "Type": "GROUP",
  "Member": [
    {
      "Id": "user-1",
      "Roles": ["Owner", "Writer"],
      "FirstName": "John",
      "LastName": "Doe",
      "AvatarUrl": "https://...",
      "Notification": true,
      "Pin": false
    }
  ]
}
```

#### Option B: RabbitMQ (decoupled)

Publish to the configured `RABBITMQ_QUEUE`:

```json
{
  "Id": "room-uuid",
  "Name": "Room Name",
  "type": "GROUP",
  "Member": [
    {
      "Id": "user-1",
      "roles": ["Owner", "Writer"],
      "firstname": "John",
      "lastname": "Doe",
      "AvatarUrl": "https://...",
      "Notification": false,
      "Pin": false
    }
  ]
}
```

Room types: `PRIVATE`, `GROUP`, `TEMP`, `SUPPORT`, `CHANEL`, `BOT`.

### 3) File Uploads

```http
POST /chat/files
Content-Type: multipart/form-data
?token=<TOKEN>

file: <binary>
```

Returns `204` with `Location` header containing the file ID.

### 4) Client Integration (Recommended Flow)

1. Obtain a **token** from your Gateway.
2. Connect to `ws://<chat-host>/chat/ws?token=<TOKEN>` (Unified WS).
3. Listen for `init` event → get room list.
4. To open a room, send `{ "event_type": "joinRoom", "roomId": "..." }`.
5. Send messages: `{ "event_type": "sendMessage", "roomId": "...", "content": "...", "ulid": "..." }`.
6. Mark as read: `{ "event_type": "seenMessage", "roomId": "...", "messageId": "...", "userId": "..." }`.

---

## Deployment

### Docker

```bash
docker build -t chat-service .
docker-compose up --build
```

The `docker-compose.yml` includes:
- 3 chat server replicas (for scaling)
- MongoDB, Redis, Kafka, RabbitMQ, MinIO
- Nginx reverse proxy with WebSocket support and least-connections load balancing

### Kubernetes

Manifests in `k8s/`:

```bash
kubectl apply -f k8s/chat-shanakht.yaml
kubectl apply -f k8s/mongo.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/scylla.yaml          # optional
```

Features:
- 2 replicas with session affinity
- ConfigMap from `k8s/env.dev` or `k8s/env.prod`
- NodePort service
- StatefulSets for MongoDB, Redis, ScyllaDB

### CI/CD (GitLab)

Configured in `.gitlab-ci.yml`:
- Build and push Docker image on `main`/`develop` branches to `registry.meetpeet.ir`
- Deploy to k3s via kubectl (create ConfigMap, apply manifests, update image, rollout)

### Nginx Configuration

The `nginx/` directory provides a reverse proxy config with:
- WebSocket `Upgrade` header support
- Least-connections load balancing across 3 upstream servers
- 24-hour proxy timeout for long-lived WS connections

---

## Project Structure

```
cmd/
├── main.go              # Entry point (MongoDB, Redis, Kafka, Hub init)
├── scylla.go            # ScyllaDB initialization

internal/
├── ws/                  # Core WebSocket engine
│   ├── hub.go           # Hub: in-memory state machine (rooms, users, clients)
│   ├── client.go        # WebSocket client, message types, ConnsWriter
│   ├── user.go          # User model + WireRooms (room-to-user routing)
│   ├── ws_handler.go    # Gin handlers for WS upgrade + REST endpoints
│   ├── database.go      # DatabaseProvider interface
│   ├── database_mongo.go    # MongoDB adapter
│   ├── database_scylla.go   # ScyllaDB adapter
│   ├── message_service.go   # Message persistence + Redis cache
│   ├── room_service.go      # Room operations (sync, pin, notifications)
│   ├── redis_pubsub.go      # Cross-instance Pub/Sub
│   ├── redis_presence.go    # Online/offline tracking
│   ├── rabbitbroker.go      # RabbitMQ consumer for room creation
│   ├── minio_storage.go     # MinIO S3 file storage
│   ├── file_handler.go      # File upload/download
│   ├── ntfy_service.go      # Push notifications
│   ├── permissions.go       # Role-based access control
│   └── helpers.go           # ULID generation, type converters
├── core/                    # Clean architecture (newer)
│   ├── domain/              # Domain entities
│   ├── ports/               # Service + repository interfaces
│   └── services/            # ChatService implementation
├── adapters/repository/     # Adapter implementations
│   ├── mongo/               # MongoDB repo (room, message, user)
│   └── scylla/              # ScyllaDB repo
├── admin/admin_handler.go   # Admin REST API handler
└── message_broker/          # Kafka producer + consumer

router/router.go          # All Gin route definitions

entity/                   # Legacy entity layer
ports/                    # Legacy port interfaces
adapters/                 # Legacy adapter implementations
Dtos/                     # Data transfer objects
enums/                    # Status enums (online/offline, room status)
helper/                   # API response wrapper
transform/                # Entity-to-DTO transformers
util/                     # Password hashing utilities
config/                   # Mock users JSON
```

---

## Project Status

This project has **two coexisting architectural layers**:

1. **Legacy** (`entity/`, `ports/`, `adapters/`, `internal/ws/`) — the original implementation, actively used.
2. **Newer clean architecture** (`internal/core/`, `internal/adapters/repository/`) — a refactoring effort with cleaner separation.

Both coexist; the legacy layer powers the main WebSocket engine while the newer layer is progressively being adopted.
