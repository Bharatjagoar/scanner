# ChatMeSerivice

A WhatsApp-style real-time messaging backend built with a microservices architecture. Two independent services communicate asynchronously via RabbitMQ — the main service owns all socket and session logic, while a dedicated message service owns all database operations.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     React Frontend                      │
│              Vite · Redux Toolkit · Socket.IO client    │
└────────────────────┬──────────────┬─────────────────────┘
                     │ HTTP (REST)  │ WebSocket
                     ▼              ▼
┌─────────────────────────────────────────────────────────┐
│                  Main Service  :5000                    │
│                                                         │
│  Express · Passport · Socket.IO · Redis · RabbitMQ     │
│                                                         │
│  Responsibilities:                                      │
│  • Auth (register / login / logout via Passport local) │
│  • Session management (express-session + MongoStore)   │
│  • Socket connection lifecycle                          │
│  • Redis: socket ID routing per user                   │
│  • RabbitMQ: producer for messageSent, fetchPending    │
│  • RabbitMQ: RPC caller for ReadConvos, SendChatId     │
└───────────────────────┬─────────────────────────────────┘
                        │ RabbitMQ (amqp://localhost:5672)
          ┌─────────────┼──────────────────┐
          │             │                  │
      messageSent   ReadConvos /       fetchPending /
                    SendChatId         markdeliver
          │             │                  │
          ▼             ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│               Message Service  (no port)                │
│                                                         │
│  amqplib · Mongoose                                     │
│                                                         │
│  Responsibilities:                                      │
│  • Persist messages to MongoDB (messageSent consumer)  │
│  • Serve conversation lists via RPC (ReadConvos)       │
│  • Serve chat history via RPC (SendChatId)             │
│  • Mark messages delivered (markdeliver consumer)      │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │     MongoDB      │
              │  Messages        │
              │  ChatsCollection │
              │  Users           │
              │  Sessions        │
              └──────────────────┘
```

---

## Project Structure

```
ChatMeSerivice/
├── backend/                        # Main service (port 5000)
│   ├── index.js                    # Express + Socket.IO entry point
│   ├── config/
│   │   ├── mongoose.js             # MongoDB connection
│   │   ├── redis.js                # Redis client (socket ID store)
│   │   ├── RabbitMQ.js             # RabbitMQ connection + getChannel()
│   │   └── passportConfig.js       # Passport local strategy
│   ├── Route/
│   │   └── index.js                # All HTTP routes
│   ├── conttroller/
│   │   ├── usercontroller.js       # Register, login, search
│   │   └── messagesController.js   # RPC callers for conversation/message reads
│   ├── socket/
│   │   └── socket.js               # All Socket.IO event handlers
│   ├── Services/
│   │   └── Messaages.js            # Queue setup helper
│   └── schema/
│       └── userSchema.js           # User model
│
├── MessageServices/                # Message microservice (no HTTP port)
│   ├── index.js                    # Bootstraps all consumers
│   ├── schema/
│   │   ├── messageSchema.js        # Message model
│   │   └── chatschema.js           # ChatCollection model
│   └── src/
│       ├── config/
│       │   ├── mongoose.js         # MongoDB connection
│       │   └── RabbitMQ.js         # RabbitMQ connection + getchannel()
│       └── consumer/
│           ├── sendmessage.js      # Persists incoming messages
│           ├── readConversation.js # RPC: returns conversation list
│           ├── checkConvo.js       # RPC: returns chat message history
│           └── MarkDelivery.js     # Marks messages as delivered
│
└── frontend/                       # React client (Vite, port 3000)
    ├── src/
    │   ├── component/
    │   │   ├── login/              # Login page
    │   │   ├── Register/           # Registration page
    │   │   ├── message/            # Chat screen layout
    │   │   └── message/MainScreen/
    │   │       ├── Contacts.jsx    # Sidebar contact/conversation list
    │   │       └── RightChattingwindows/
    │   │           └── ChattingWindow.jsx  # Active chat window
    │   └── socket/
    │       └── socket.js           # Socket.IO client singleton
    └── redux/
        ├── store.js
        ├── reducer.js
        └── chatslice.js            # Conversations, messages, unread counts
```

---

## RabbitMQ Queues

| Queue | Direction | Type | Purpose |
|---|---|---|---|
| `messageSent` | main → message service | Fire and forget | Persist a new message to MongoDB |
| `ReadConvos` | main → message service | RPC (request) | Fetch all conversations for a user |
| `ResponseReadConvos` | message service → main | RPC (reply) | Returns conversation list |
| `SendChatId` | main → message service | RPC (request) | Fetch message history for a chatId |
| `ReadfromDBMessages` | message service → main | RPC (reply) | Returns message list |
| `fetchPending` | main → message service | Fire and forget | Trigger offline delivery sweep on user connect |
| `markdeliver` | main → message service | Fire and forget | Mark a batch of message IDs as delivered |

RPC calls use `correlationId` + `replyTo` for request-reply matching. Both services use a shared `MessageExchange` direct exchange.

---

## Message Delivery Flow

### Online delivery (sender → recipient online)

```
1. Sender emits  getthesocketID-forMessage
2. Main reads recipient socket ID from Redis
3. Main calls io.timeout(5000).to(socketId).emitWithAck("MessageRecieved")
4. Client acks with { received: true } → status = "delivered"
   Client fails / timeout        → status = "sent"
5. Main enqueues to messageSent with final status
6. Message service persists to MongoDB
```

### Offline delivery (recipient connects later)

```
1. User connects → main writes socket ID to Redis
2. Main enqueues userid to fetchPending
3. Message service queries { recieverID: userid, status: "sent" }
4. Message service returns list to main (Lane 2 — in progress)
5. Main pushes messages via emitWithAck, collects acked IDs
6. Main enqueues acked IDs to markdeliver
7. Message service updateMany({ _id: { $in: ids } }, { status: "delivered" })
```

> **Note:** Lanes 2–3 of the offline delivery flow are currently in development.

---

## Socket Events

### Client → Server

| Event | Payload | Description |
|---|---|---|
| `getthesocketID-forMessage` | `{ userid, senderId, Message, ... }` | Send a message. Acks with `{ time, status }` |
| `typing` | `{ userId }` | Notify recipient that sender is typing |
| `custome_disconnect` | — | Manual logout hook |

### Server → Client

| Event | Payload | Description |
|---|---|---|
| `MessageRecieved` | `{ data }` | Incoming message. Client must ack with `{ received: true }` |
| `types` | — | Typing indicator from another user |

---

## REST API

All routes are on the main service at `http://localhost:5000`.

| Method | Route | Auth | Description |
|---|---|---|---|
| `POST` | `/createUser` | No | Register a new user |
| `POST` | `/checkUserName` | No | Check if a username exists |
| `POST` | `/SearchString` | No | Search users by string |
| `POST` | `/login` | No | Login with username + password (Passport local) |
| `POST` | `/logout` | Yes | Logout and clear Redis socket entry |
| `GET` | `/test` | No | Auth status check |
| `GET` | `/LoadConversation/:id` | No | Load all conversations for user `id` (RPC) |
| `GET` | `/getMessages/:ChatId` | No | Load message history for a chat (RPC) |

---

## Data Models

### Message
```
chatId      String        required  — sorted join of senderId + receiverId
message     String        required
senderId    ObjectId      required
recieverID  ObjectId      required
reaction    String
time        Date
status      sent | delivered | read   default: sent
```

### ChatCollection
```
chatId       String
participant  [ObjectId]   ref: User
LastMessage  String
Time         Date
unreadCount  Map<userId, Number>
```

### User
```
UserName  String  required
EmailId   String  required
Password  String  required
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 18, Vite, Redux Toolkit, Socket.IO client, Axios, Framer Motion |
| Main service | Node.js, Express, Socket.IO, Passport (local), express-session |
| Message service | Node.js, Mongoose |
| Realtime | Socket.IO 4.7.5 |
| Session store | connect-mongo |
| Message queue | RabbitMQ (amqplib) |
| Cache / routing | Redis (redis v4) |
| Database | MongoDB (Mongoose) |

---

## Local Setup

### Prerequisites

- Node.js 18+
- MongoDB running locally
- RabbitMQ running locally on `amqp://localhost:5672`
- Redis running locally

### 1. Clone

```bash
git clone https://github.com/Bharatjagoar/ChatMeSerivice.git
cd ChatMeSerivice
```

### 2. Main service

```bash
cd backend
npm install
```

Create `backend/.env`:
```
PORT=5000
MongodbURL=mongodb://localhost:27017/chatme
redisURL=redis://localhost:6379
NODE_ENV=development
```

```bash
npm start
```

### 3. Message service

```bash
cd MessageServices
npm install
```

Create `MessageServices/.env`:
```
MongodbURL=mongodb://localhost:27017/chatme
```

```bash
npm start
```

### 4. Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:3000`. Backend expected at `http://localhost:5000`.

---

## Known Limitations

- No socket authentication middleware — handshake `user` is client-supplied and unverified
- `messageSent` queue is `durable: false` — messages can be lost on broker restart
- `timestamps` option is misspelled in schemas (`timestamp: true`) — `createdAt`/`updatedAt` are not generated
- Offline delivery sweep (Lanes 2–3) is not yet complete
- No read receipt reset logic when unread messages are viewed
- No HTTPS or production session secret management

---

*Built by Bharat*
