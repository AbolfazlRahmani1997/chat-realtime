# Chat Service

سرویس چت real-time مبتنی بر WebSocket با پشتیبانی از اتاق‌های گروهی، تحویل و خوانده‌شدن پیام، و یکپارچه‌سازی با صف RabbitMQ و سرویس احراز هویت (Gateway).

---

## فهرست

- [معماری و تکنولوژی](#معماری-و-تکنولوژی)
- [پیش‌نیازها](#پیش‌نیازها)
- [متغیرهای محیطی](#متغیرهای-محیطی)
- [اجرا](#اجرا)
- [API و WebSocket](#api-و-websocket)
- [یکپارچه‌سازی با سیستم شما](#یکپارچه‌سازی-با-سیستم-شما)
- [Docker](#docker)

---

## معماری و تکنولوژی

| بخش | تکنولوژی |
|-----|----------|
| Runtime | Go 1.19+ |
| HTTP/WebSocket | Gin |
| دیتابیس اصلی | MongoDB |
| کش / صف | Redis |
| صف رویداد (اتاق جدید) | RabbitMQ |
| پروتکل چت | WebSocket (Gorilla) |

**جریان کلی:**

- کاربر با **توکن** (از Gateway شما) به endpointهای WebSocket وصل می‌شود.
- **اتاق‌ها** یا از طریق API (`POST /chat/ws/createRoom`) ساخته می‌شوند یا با ارسال پیام به **RabbitMQ** در سرویس چت سینک می‌شوند.
- پیام‌ها در MongoDB ذخیره و روی WebSocket به کلاینتهای همان اتاق broadcast می‌شوند.
- وضعیت **تحویل (Deliver)** و **خوانده‌شدن (Read)** برای هر پیام نگهداری می‌شود.

---

## پیش‌نیازها

- **Go 1.19** (برای اجرای محلی)
- **MongoDB** (با احراز هویت)
- **Redis**
- **RabbitMQ**
- یک **سرویس Gateway/Auth** که با درخواست `GET /api/user` و هدر `Authorization: <token>` اطلاعات کاربر را برگرداند.

---

## متغیرهای محیطی

در روت پروژه فایل `.env` بسازید (یا از نمونه زیر استفاده کنید):

```env
# سرور چت
CHAT_WS_PORT=8080

# MongoDB
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
MONGO_DB_USERNAME=your_user
MONGO_DB_PASSWORD=your_password
CHAT_DB=chat_db
MAIN_DB=main_db

# نام collectionها (اختیاری؛ در entity از env خوانده می‌شود)
# MESSAGE_COLLECTION=messages
# ROOM_COLLECTION=rooms

# سرویس احراز هویت (Gateway)
GATEWAY_URL=http://localhost:8000

# RabbitMQ
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_HOST=localhost
RABBITMQ_QUEUE=chat.rooms

# لیست ادمین‌های پشتیبانی برای چت پیش‌فرض
# مثال: SUPPORT_ADMIN_IDS=10,11,12
SUPPORT_ADMIN_IDS=
```

- **CHAT_WS_PORT**: پورتی که سرور چت روی آن بالا می‌آید.
- **GATEWAY_URL**: آدرس پایه سرویس Gateway؛ سرویس چت برای اعتبارسنجی توکن به `GET {GATEWAY_URL}/api/user` با هدر `Authorization: <token>` درخواست می‌زند.
- **RABBITMQ_QUEUE**: نام صفی که سرویس چت از آن پیام «اتاق جدید» را consume می‌کند.

---

## اجرا

### اجرای محلی

```bash
# وابستگی‌ها
go mod download

# اجرا (فرض: .env در پوشه بالاتر یا همین پوشه)
go run ./cmd/main.go
```

سرور روی `http://0.0.0.0:CHAT_WS_PORT` (مثلاً 8080) بالا می‌آید.

### اجرا با Docker

```bash
docker-compose up --build
```

سرویس چت روی پورت **8080** در دسترس است. MongoDB، Redis و RabbitMQ از داخل همان `docker-compose` استفاده می‌شوند.

---

## API و WebSocket

پایه آدرس: `http://localhost:8080` (یا همان `CHAT_WS_PORT`).

### احراز هویت

- در بیشتر endpointها **توکن** از طریق **query** ارسال می‌شود: `?token=<JWT یا توکن شما>`.
- برای `GetRooms` توکن اجباری است و باید حداقل سه بخش (مثل JWT) داشته باشد.
- سرویس چت خودش JWT را verify نمی‌کند؛ فقط وجود و فرمت را چک می‌کند. اعتبار واقعی با فراخوانی **Gateway** (`GET {GATEWAY_URL}/api/user`) انجام می‌شود.

---

### REST

| متد | مسیر | توضیح |
|-----|------|--------|
| `POST` | `/chat/ws/createRoom` | ساخت اتاق چت (بدون توکن) |
| `GET` | `/chat/ws/getRooms/` | لیست اتاق‌های کاربر؛ **نیاز به توکن** دارد (همراه با اتاق پیش‌فرض پشتیبانی) |
| `GET` | `/chat/ws/getClients/:roomId` | لیست کلاینت‌های آنلاین اتاق |
| `GET` | `/chat/room/pin/:roomId` | پین کردن اتاق؛ هدر `Authorization` |
| `GET` | `/chat/room/notification/:roomId` | تنظیم اعلان اتاق؛ هدر `Authorization` |

**ساخت اتاق (CreateRoom):**

```http
POST /chat/ws/createRoom
Content-Type: application/json

{
  "Id": "room-uuid-or-slug",
  "Name": "نام اتاق",
  "Member": [
    {
      "Id": "user-id-1",
      "Roles": ["Owner", "Writer"],
      "FirstName": "نام",
      "LastName": "نام خانوادگی",
      "AvatarUrl": "https://...",
      "Notification": true,
      "Pin": false
    }
  ],
  "Owner": ["user-id-1"],
  "Writer": ["user-id-1"]
}
```

---

### WebSocket Endpoints

همه این آدرس‌ها با **Upgrade به WebSocket** باز می‌شوند (مثلاً از مرورگر یا کلاینت WS).

| مسیر | Query | توضیح |
|------|--------|--------|
| `GET /chat/ws/joinRoom/:roomId` | `token`, `page` | ورود به اتاق و دریافت پیام‌ها |
| `GET /chat/ws/getRooms/` | `token` | لیست اتاق‌ها + اتصال به Hub برای نوتیفیکیشن (در صورت نبود، اتاق پشتیبانی را برای کاربر می‌سازد) |
| `GET /chat/ws/syncRooms/` | `token` | سینک وضعیت اتاق‌ها برای کاربر آنلاین |
| `GET /chat/ws/seenMessage/:roomId` | - | اعلام «خوانده‌شدن» پیام (نیاز به userId در context) |

**نکته:** برای `seenMessage` باید قبلاً کاربر با همان `userId` به اتاق join کرده باشد (از طریق `joinRoom`).

---

### جریان کلاینت (اتصال به اتاق و ارسال پیام)

1. **دریافت لیست اتاق‌ها (WebSocket)**  
   اتصال به:
   ```text
   ws://localhost:8080/chat/ws/getRooms/?token=<TOKEN>
   ```
   - اولین پیام از سرور: آرایه‌ای از اتاق‌ها (یا آرایه خالی).
   - بعد از آن همین اتصال برای نوتیفیکیشن (اتاق جدید، وضعیت آنلاین/آفلاین، پیام جدید و …) استفاده می‌شود.

2. **ورود به یک اتاق (WebSocket)**  
   اتصال به:
   ```text
   ws://localhost:8080/chat/ws/joinRoom/<roomId>?token=<TOKEN>&page=1
   ```
   - سرور ابتدا دسترسی کاربر را (بر اساس `Member` و نقش‌های Owner/Writer) چک می‌کند.
   - سپس لیست پیام‌های اتاق را (مثلاً ۱۰ تای آخر) روی همان WebSocket می‌فرستد.
   - از این به بعد این اتصال برای **ارسال و دریافت پیام همان اتاق** استفاده می‌شود.

3. **فرمت ارسال پیام از کلاینت به سرور (در همان WS اتاق)**  
   هر پیام به صورت JSON:
   ```json
   {
     "ulid": "یک شناسه یکتا (مثلاً ULID)",
     "content": "متن پیام"
   }
   ```
   - مقدار `"ulid": "ping"` برای نگه‌داشتن اتصال استفاده می‌شود.
   - سرور همان پیام را با فیلدهای تکمیل‌شده (مثل `_id`, `RoomID`, `ClientID`, `CreatedAt`) به بقیه اعضای اتاق broadcast می‌کند.

4. **فرمت پیام‌های دریافتی از سرور (در WS اتاق)**  
   - **پیام چت:** آبجکت با فیلدهایی مثل `_id`, `Content`, `RoomID`, `ClientID`, `Username`, `Deliver`, `Read`, `CreatedAt`.
   - **تأیید تحویل:** آبجکت با `event_type: "deliverMessage"` و محتوای مربوط به همان پیام.

5. **اتصال لیست اتاق‌ها (GetRooms WS)**  
   روی همین اتصال، سرور رویدادهایی با `event_type` می‌فرستد، از جمله:
   - `roomStatus`: تغییر وضعیت آنلاین/آفلاین اتاق
   - `incomeMessage`: پیام جدید در اتاقی که کاربر در آن باز نیست
   - `listRooms`: به‌روزرسانی لیست اتاق‌ها
   - `seenMessage`: خوانده‌شدن یک پیام
   - `createNewRoom`: ساخت اتاق جدید

   برای درخواست لیست به‌روز اتاق‌ها، کلاینت می‌تواند پیام زیر را روی همین WS بفرستد:
   ```json
   {
     "event_type": "listRooms",
     "item": "1"
   }
   ```
   (`item` همان شماره صفحه است.)

#### اتاق پیش‌فرض پشتیبانی برای هر کاربر

- به محض این‌که کلاینت روی WS زیر وصل شود:

  ```text
  ws://localhost:8080/chat/ws/getRooms/?token=<TOKEN>
  ```

  سرویس چک می‌کند که آیا برای این کاربر اتاقی با الگوی `support-<userId>` وجود دارد یا نه.

- اگر **وجود نداشته باشد**:
  - یک اتاق جدید با `Id = "support-<userId>"` و `Name = "پشتیبانی"` ساخته می‌شود.
  - اعضای اتاق:
    - خود کاربر با نقش‌های `["Owner", "Writer"]`
    - لیست ادمین‌های پشتیبانی که در `SUPPORT_ADMIN_IDS` (به صورت جداشده با کاما) تنظیم کرده‌اید، هر کدام با نقش `["Writer"]`.
  - این اتاق در MongoDB ذخیره و در خروجی `getRooms` به کلاینت برگردانده می‌شود.

- در سمت فرانت‌اند کافی است:
  - بعد از دریافت لیست اتاق‌ها، اتاقی با `Id` شروع‌شونده با `support-` را تشخیص دهید و آن را به عنوان **چت با پشتیبان** نمایش دهید.
  - join برای این اتاق مثل بقیه اتاق‌هاست:

    ```text
    ws://localhost:8080/chat/ws/joinRoom/support-<userId>?token=<TOKEN>&page=1
    ```

  - از نظر فرمت پیام و رویدادها هیچ تفاوتی با سایر اتاق‌ها ندارد؛ فقط نام/نوع و الگوی ID برای UI مهم است.

---

### API ادمین

پیشوند: `/chat/api/admin`.

| متد | مسیر | توضیح |
|-----|------|--------|
| `GET` | `/chat/api/admin/room/` | لیست اتاق‌ها (با query مثل `userId`) |
| `GET` | `/chat/api/admin/room/:roomId` | جزئیات یک اتاق |
| `GET` | `/chat/api/admin/message/` | پیام‌ها (با query مثل `id`) |
| `GET` | `/chat/api/admin/message/:messageId` | جزئیات یک پیام |

این endpointها برای پنل ادمین یا ابزارهای داخلی هستند و در این سند جزئیات body/response آن‌ها توضیح داده نشده است.

---

## یکپارچه‌سازی با سیستم شما

### ۱) سرویس احراز هویت (Gateway)

سرویس چت برای هر درخواستی که به توکن نیاز دارد، در صورت نبودن کاربر در کش داخلی، این درخواست را می‌زند:

```http
GET {GATEWAY_URL}/api/user
Authorization: <توکن دریافتی از کلاینت>
```

- در صورت **200**: بدنه پاسخ باید شامل اطلاعات کاربر باشد (مثلاً `id`, `username`, `firstname`, `lastname`, `avatar`). این مقادیر در سرویس چت برای نمایش و سینک استفاده می‌شوند.
- در صورت **غیر 200**: سرویس چت آن توکن را نامعتبر در نظر می‌گیرد و درخواست را رد می‌کند.

بنابراین کافی است در سیستم خودتان:
- یک endpoint مثل `GET /api/user` داشته باشید که با همان توکنی که به چت می‌دهید، کاربر را برگرداند.
- در `docker-compose` یا `.env` مقدار `GATEWAY_URL` را به آدرس همین سرویس (مثلاً در شبکه Docker) تنظیم کنید.

### ۲) ساخت اتاق از سیستم شما (بدون تماس مستقیم به چت)

اگر ترجیح می‌دهید اتاق‌ها فقط از سرویس خودتان ساخته شوند، می‌توانید به جای `POST /chat/ws/createRoom` از **RabbitMQ** استفاده کنید.

- **صف:** همان صفی که در `RABBITMQ_QUEUE` تنظیم کرده‌اید (مثلاً `chat.rooms`).
- **فرمت پیام (JSON):**

```json
{
  "Id": "room-unique-id",
  "Name": "نام اتاق",
  "type": "GROUP",
  "Member": [
    {
      "Id": "user-id-1",
      "roles": ["Owner", "Writer"],
      "firstname": "نام",
      "lastname": "نام خانوادگی",
      "AvatarUrl": "https://...",
      "Notification": false,
      "Pin": false
    }
  ]
}
```

- **نوع اتاق (`type`):** مقادیر مجاز در کد: `PRIVATE`, `GROUP`, `TEMP`.
- وقتی این پیام را در صف publish کنید، سرویس چت آن را consume می‌کند و در صورت نبود اتاق با همین `Id`، اتاق را در MongoDB می‌سازد و به کاربران آنلاین مربوطه نوتیفیکیشن `createNewRoom` می‌فرستد.

در سیستم خودتان:
- بعد از ساخت اتاق در دیتابیس/منطق خودتان، یک بار همین JSON را به صف RabbitMQ بفرستید تا چت با آن سینک شود.

### ۳) فراخوانی مستقیم CreateRoom از بک‌اند

اگر ترجیح می‌دهید اتاق مستقیماً در چت ساخته شود، از سرویس خودتان به صورت HTTP به `POST /chat/ws/createRoom` درخواست بزنید و body را مطابق جدول بالا بفرستید. در این حالت نیازی به RabbitMQ برای همان اتاق نیست.

### ۴) کلاینت (وب/موبایل)

- برای هر کاربر یک **توکن** (مثلاً JWT) از Gateway خودتان صادر کنید.
- در اپ یا وب، به آدرس پایه چت (مثلاً `https://chat.yourdomain.com`) وصل شوید و در همه درخواست‌ها و اتصال WebSocket همان توکن را در query یا هدر بفرستید.
- جریان توصیه‌شده:
  1. اتصال به `getRooms/?token=...` و نگه‌داشتن این اتصال برای نوتیفیکیشن.
  2. برای هر اتاقی که کاربر باز می‌کند: اتصال به `joinRoom/<roomId>?token=...&page=1` و ارسال/دریافت پیام با فرمت بالا.
  3. برای اعلام «خوانده‌شدن» از endpoint مربوط به `seenMessage` استفاده کنید (با توجه به اینکه `userId` در context ست شده باشد).

---

## Docker

- **Build:**
  ```bash
  docker build -t chat-service .
  ```

- **Run با docker-compose (همه سرویس‌ها):**
  ```bash
  docker-compose up --build
  ```

- در `docker-compose` سرویس **chat** به `chat-mongo`, `chat-redis` و `rabbitmq` وابسته است. برای اتصال به Gateway خودتان، متغیر **GATEWAY_URL** را در بخش `environment` سرویس `chat` به آدرس سرویس Gateway (مثلاً در همان شبکه Docker) تنظیم کنید.

- پورت‌ها:
  - چت: **8080**
  - MongoDB: **27017**
  - Redis: **6379**
  - RabbitMQ: **5672** (AMQP), **15672** (Management UI)

---

## جمع‌بندی

- **اجرا:** `go run ./cmd/main.go` یا `docker-compose up --build`.
- **احراز هویت:** توکن را از Gateway خودتان بگیرید و در هر درخواست/WebSocket به چت بفرستید؛ چت با `GET {GATEWAY_URL}/api/user` اعتبار را چک می‌کند.
- **اتاق:** یا با `POST /chat/ws/createRoom` یا با publish کردن JSON اتاق به صف `RABBITMQ_QUEUE`.
- **چت real-time:** WebSocket به `joinRoom/<roomId>?token=...` و ارسال پیام با `{ "ulid": "...", "content": "..." }`.

اگر خواستید برای یک پلتفرم خاص (مثلاً فقط وب یا فقط اندروید) نمونه کد کلاینت هم اضافه شود، می‌توان آن را به این README یا یک فایل جدا (مثلاً `INTEGRATION.md`) اضافه کرد.
