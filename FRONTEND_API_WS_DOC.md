# Chat Service API + WebSocket Guide (Frontend)

این فایل یک مستند واحد برای تیم فرانت است و شامل:
- REST API (JSON) برای Postman
- سناریوی کامل WebSocket (مدل Unified)

---

## 1) Environment Variables (برای Postman)

در Postman این متغیرها را تعریف کنید:

- `base_url` = `http://localhost:8080`
- `ws_base` = `ws://localhost:8080`
- `token` = توکن کاربر (یا mock token)
- `room_id` = شناسه اتاق (مثلا `support-123` یا `room-abc`)
- `message_ulid` = شناسه یکتا برای پیام (مثلا `01J...`)

---

## 2) REST API (JSON) - قابل استفاده در Postman

### 2.1 Create Room

- **Method:** `POST`
- **URL:** `{{base_url}}/chat/ws/createRoom`
- **Headers:**
  - `Content-Type: application/json`
- **Body (raw/json):**

```json
{
  "Id": "room-abc",
  "Name": "اتاق تست",
  "Member": [
    {
      "Id": "123",
      "Notification": true,
      "Pin": false
    },
    {
      "Id": "10",
      "Notification": true,
      "Pin": false
    }
  ],
  "Owner": ["123"],
  "Writer": ["123", "10"]
}
```

- **Success Response (`200`):**

```json
{
  "Id": "room-abc",
  "Name": "اتاق تست",
  "Member": [
    {
      "Id": "123",
      "Notification": true,
      "Pin": false
    },
    {
      "Id": "10",
      "Notification": true,
      "Pin": false
    }
  ],
  "Owner": ["123"],
  "Writer": ["123", "10"]
}
```

---

### 2.2 Get Current User

- **Method:** `GET`
- **URL:** `{{base_url}}/chat/me?token={{token}}`

- **Success Response (`200`):**

```json
{
  "id": 123,
  "firstname": "Ali",
  "lastname": "Ahmadi",
  "username": "ali",
  "avatar": "https://..."
}
```

- **Error (`401`):**

```json
{
  "error": "token required"
}
```

---

### 2.3 Get Online Clients of Room

- **Method:** `GET`
- **URL:** `{{base_url}}/chat/ws/getClients/{{room_id}}`

- **Success Response (`200`):**

```json
[
  { "id": "123", "username": "ali" },
  { "id": "10", "username": "support_admin" }
]
```

اگر اتاق داخل hub نباشد، آرایه خالی می‌دهد:

```json
[]
```

---

### 2.4 Toggle Pin Room

- **Method:** `GET`
- **URL:** `{{base_url}}/chat/room/pin/{{room_id}}`
- **Headers:**
  - `Authorization: {{token}}`

- **Success Response (`200`):**

```json
{
  "Notification": true,
  "Pin": true
}
```

> این endpoint حالت Pin را toggle می‌کند.

---

### 2.5 Toggle Room Notification

- **Method:** `GET`
- **URL:** `{{base_url}}/chat/room/notification/{{room_id}}`
- **Headers:**
  - `Authorization: {{token}}`

- **Success Response (`200`):**

```json
{
  "Notification": false,
  "Pin": true
}
```

> این endpoint نیز Notification را toggle می‌کند.

---

### 2.6 Get Mock Users (اختیاری)

- **Method:** `GET`
- **URL:** `{{base_url}}/chat/mock-users`

- **Success Response (`200`):**

```json
{
  "users": [
    {
      "token": "mock-token-1",
      "label": "User One"
    },
    {
      "token": "mock-token-2",
      "label": "User Two"
    }
  ]
}
```

---

## 3) WebSocket Scenario (Unified) - سناریوی استاندارد فرانت

## 3.1 Connect

به این آدرس وصل شوید:

`{{ws_base}}/chat/ws?token={{token}}`

بعد از اتصال، اولین پیام از سرور:

```json
{
  "event_type": "init",
  "content": [],
  "current_user_id": "123"
}
```

`content` لیست اتاق‌هاست (ممکن است خالی باشد).

---

## 3.2 Request List Rooms (pagination)

از کلاینت به سرور:

```json
{
  "event_type": "listRooms",
  "item": "1"
}
```

پاسخ سرور:

```json
{
  "event_type": "listRooms",
  "Content": [
    {
      "Id": "support-123",
      "Name": "پشتیبانی",
      "Members": [],
      "Status": "offline",
      "CountMessages": 0,
      "Message": {},
      "Type": "TEMP"
    }
  ]
}
```

> نکته مهم: در این پاسخ کلید `Content` با C بزرگ است.

---

## 3.3 Join Room

از کلاینت به سرور:

```json
{
  "event_type": "joinRoom",
  "room_id": "{{room_id}}",
  "page": "1"
}
```

پاسخ سرور (history پیام‌ها):

```json
{
  "event_type": "roomHistory",
  "room_id": "room-abc",
  "content": [
    {
      "_id": "67d...",
      "Content": "سلام",
      "UniqId": "01J...",
      "RoomID": "room-abc",
      "Username": "ali",
      "ClientID": "123",
      "Deliver": ["10"],
      "Read": [],
      "CreatedAt": "2026-03-18T15:00:00Z"
    }
  ]
}
```

---

## 3.4 Send Message

از کلاینت به سرور:

```json
{
  "event_type": "sendMessage",
  "room_id": "{{room_id}}",
  "ulid": "{{message_ulid}}",
  "content": "سلام از فرانت",
  "type": "text"
}
```

Ack سرور برای sender:

```json
{
  "event_type": "deliverMessage",
  "Content": "{{message_ulid}}",
  "room_id": "room-abc"
}
```

Broadcast پیام برای اعضای اتاق:

```json
{
  "event_type": "roomMessage",
  "room_id": "room-abc",
  "content": {
    "_id": "67d...",
    "Content": "سلام از فرانت",
    "Type": "text",
    "UniqId": "{{message_ulid}}",
    "RoomID": "room-abc",
    "Username": "ali",
    "ClientID": "123",
    "Deliver": [],
    "Read": [],
    "CreatedAt": "2026-03-18T15:05:00Z"
  }
}
```

---

## 3.5 Read (Seen Message)

از کلاینت به سرور:

```json
{
  "event_type": "seenMessage",
  "item": "{{message_ulid}}"
}
```

نوتیف سرور برای طرف مقابل:

```json
{
  "event_type": "seenMessage",
  "Content": {
    "message_id": "{{message_ulid}}",
    "room_id": "room-abc"
  }
}
```

---

## 3.6 Leave Room

از کلاینت به سرور:

```json
{
  "event_type": "leaveRoom",
  "room_id": "{{room_id}}"
}
```

---

## 3.7 Ping / Keep Alive

دو مدل ping در کد موجود است. پیشنهاد استاندارد:

```json
{
  "event_type": "ping"
}
```

پاسخ:

```json
{
  "event_type": "deliverMessage",
  "Content": "ping"
}
```

همچنین در مسیر `sendMessage` اگر `ulid` برابر `"ping"` باشد، ack با `deliverMessage` برمی‌گردد.

---

## 4) Event Types (برای تیم فرانت)

event هایی که ممکن است روی unified socket دریافت شوند:

- `init` (بعد از connect)
- `listRooms`
- `roomHistory`
- `roomMessage`
- `deliverMessage`
- `seenMessage`
- `roomStatus`
- `incomeMessage`
- `createNewRoom`
- `error`

---

## 5) نکات پیاده‌سازی فرانت

- parser پیام WS باید نسبت به `content` و `Content` انعطاف داشته باشد.
- `current_user_id` را از پیام `init` نگه دارید تا پیام خود کاربر را در UI تشخیص دهید.
- قبل از `sendMessage` بهتر است حتما `joinRoom` انجام شده باشد.
- برای fallback، خطاهای `event_type: "error"` را در UI نمایش دهید.
- اگر از Mock mode استفاده می‌کنید، توکن‌ها را از `/chat/mock-users` بگیرید.

---

## 6) Quick Test Order (دموی سریع)

1. `GET /chat/mock-users` (اختیاری) -> توکن بگیرید  
2. WS connect به `/chat/ws?token=...`  
3. ارسال `{"event_type":"listRooms","item":"1"}`  
4. اگر لازم بود `POST /chat/ws/createRoom`  
5. ارسال `joinRoom`  
6. ارسال `sendMessage`  
7. ارسال `seenMessage`  

