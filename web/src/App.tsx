import { FormEvent, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createRoom } from "./api";
import { avatarColor, filePayload, formatBytes, formatClock, initials, messagePreview } from "./format";
import type { ChatMessage, ChatRoom, ConnectionStatus, Receipt } from "./types";
import { useChat } from "./useChat";

const ACCOUNTS = [
  { token: "dev-token-user1", label: "کاربر اول", id: "1" },
  { token: "dev-token-user2", label: "کاربر دوم", id: "2" },
  { token: "dev-token-user3", label: "کاربر سوم", id: "3" },
];

function roomTitle(room: ChatRoom, myId: string) {
  if (room.Type !== "PRIVATE") return room.Name || room.Id;
  const peer = (room.Members ?? []).find((member) => member.Id !== myId);
  const name = [peer?.firstname, peer?.lastname].filter(Boolean).join(" ").trim();
  return name || room.Name || room.Id;
}

export default function App() {
  const [account, setAccount] = useState(ACCOUNTS[0]);
  const [token, setToken] = useState("");
  const [query, setQuery] = useState("");
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<{ id: string; text: string } | null>(null);
  const [forwarding, setForwarding] = useState<string>("");
  const chat = useChat(token);
  const active = chat.rooms.find((room) => room.Id === chat.activeRoomId);
  const rooms = useMemo(() => {
    const q = query.trim();
    if (!q) return chat.rooms;
    return chat.rooms.filter((room) => `${roomTitle(room, chat.userId)} ${room.Name} ${room.Id}`.includes(q));
  }, [chat.rooms, chat.userId, query]);

  return (
    <div className={chat.activeRoomId ? "shell chat-open" : "shell"}>
      <aside className="sidebar">
        <header className="side-head">
          <button
            type="button"
            className="avatar"
            style={{ background: avatarColor(account.id) }}
            onClick={() => {
              const next = ACCOUNTS[(ACCOUNTS.findIndex((item) => item.token === account.token) + 1) % ACCOUNTS.length];
              setAccount(next);
              setToken(next.token);
            }}
            title="تعویض حساب"
          >
            {initials(account.label)}
          </button>
          <div className="side-title">
            <strong>{account.label}</strong>
            <span className={`presence presence-${chat.status}`}>{statusLabel(chat.status)}</span>
          </div>
          <button type="button" className="icon-btn" onClick={() => setCreating(true)} disabled={!token}>
            جدید
          </button>
        </header>
        <div className="search">
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="جستجو" />
        </div>
        {!token ? (
          <div className="account-pick">
            {ACCOUNTS.map((item) => (
              <button
                key={item.token}
                type="button"
                onClick={() => {
                  setAccount(item);
                  setToken(item.token);
                }}
              >
                <span className="avatar sm" style={{ background: avatarColor(item.id) }}>
                  {initials(item.label)}
                </span>
                {item.label}
              </button>
            ))}
          </div>
        ) : null}
        {chat.error ? <p className="toast">{chat.error}</p> : null}
        <ul className="dialogs">
          {rooms.map((room) => (
            <DialogRow
              key={room.Id}
              room={room}
              userId={chat.userId}
              active={room.Id === chat.activeRoomId}
              onOpen={() => chat.joinRoom(room.Id)}
            />
          ))}
        </ul>
        {token && rooms.length === 0 ? <p className="hint">گفتگویی پیدا نشد.</p> : null}
      </aside>

      <section className="thread">
        {active ? (
          <>
            <header className="thread-head">
              <button type="button" className="back" onClick={() => chat.leaveRoom(active.Id)}>
                بازگشت
              </button>
              <span className="avatar" style={{ background: avatarColor(active.Id) }}>
                {initials(roomTitle(active, chat.userId))}
              </span>
              <div>
                <strong>{roomTitle(active, chat.userId)}</strong>
                {active.Type === "PRIVATE" ? null : (
                  <span className="sub">
                    {active.Members?.length ? `${active.Members.length} عضو` : active.Type || "گفتگو"}
                  </span>
                )}
              </div>
            </header>
            <MessageList
              messages={chat.messages}
              userId={chat.userId}
              hasOlder={chat.hasOlder}
              onLoadOlder={chat.loadOlder}
              onReact={(messageId, emoji) => chat.react(active.Id, messageId, emoji)}
              onEdit={(messageId, text) => setEditing({ id: messageId, text })}
              onForward={(messageId) => setForwarding(messageId)}
              onView={(messageId) => chat.viewSecret(active.Id, messageId)}
            />
            <Composer
              key={editing?.id ?? "compose"}
              disabled={chat.status !== "open"}
              initial={editing?.text ?? ""}
              editing={Boolean(editing)}
              onCancel={() => setEditing(null)}
              onFile={(file) => chat.sendFile(active.Id, file)}
              onSend={(text, secret) => {
                if (editing) {
                  chat.editText(active.Id, editing.id, text);
                  setEditing(null);
                  return;
                }
                chat.sendText(active.Id, text, secret);
              }}
            />
          </>
        ) : (
          <div className="welcome">
            <div className="plane" aria-hidden="true" />
            <h1>گفتگویی را انتخاب کن</h1>
            <p>یک اتاق را از فهرست باز کن، یا گفتگوی تازه بساز.</p>
          </div>
        )}
      </section>

      {creating ? (
        <NewChat
          defaultMembers={ACCOUNTS.map((item) => item.id).join(",")}
          onClose={() => setCreating(false)}
          onCreated={(roomId) => {
            setCreating(false);
            chat.refreshRooms();
            chat.joinRoom(roomId);
          }}
        />
      ) : null}
      {forwarding && active ? (
        <div className="modal-back" onClick={() => setForwarding("")}>
          <form className="modal" onClick={(event) => event.stopPropagation()}>
            <h2>فوروارد به</h2>
            <ul className="forward-list">
              {chat.rooms
                .filter((room) => room.Id !== active.Id)
                .map((room) => (
                  <li key={room.Id}>
                    <button
                      type="button"
                      onClick={() => {
                        chat.forwardTo(active.Id, forwarding, room.Id);
                        setForwarding("");
                      }}
                    >
                      {roomTitle(room, chat.userId)}
                    </button>
                  </li>
                ))}
            </ul>
            <div className="modal-actions">
              <button type="button" onClick={() => setForwarding("")}>
                انصراف
              </button>
            </div>
          </form>
        </div>
      ) : null}
    </div>
  );
}

function DialogRow({
  room,
  userId,
  active,
  onOpen,
}: {
  room: ChatRoom;
  userId: string;
  active: boolean;
  onOpen: () => void;
}) {
  const unread = room.CountMessages ?? 0;
  const title = roomTitle(room, userId);
  return (
    <li>
      <button type="button" className={active ? "dialog active" : "dialog"} onClick={onOpen}>
        <span className="avatar" style={{ background: avatarColor(room.Id) }}>
          {initials(title)}
        </span>
        <span className="dialog-body">
          <span className="dialog-top">
            <strong>{title}</strong>
            <time>{formatClock(room.Message?.CreatedAt)}</time>
          </span>
          <span className="dialog-bottom">
            <span>{messagePreview(room.Message)}</span>
            {unread > 0 ? <em>{unread}</em> : null}
          </span>
        </span>
      </button>
    </li>
  );
}

const REACTIONS = ["👍", "❤️", "😂", "😮", "😢", "🔥"];

function MessageList({
  messages,
  userId,
  hasOlder,
  onLoadOlder,
  onReact,
  onEdit,
  onForward,
  onView,
}: {
  messages: ChatMessage[];
  userId: string;
  hasOlder: boolean;
  onLoadOlder: () => boolean;
  onReact: (messageId: string, emoji: string) => void;
  onEdit: (messageId: string, text: string) => void;
  onForward: (messageId: string) => void;
  onView: (messageId: string) => void;
}) {
  const boxRef = useRef<HTMLOListElement>(null);
  const stick = useRef(true);
  const keep = useRef<number | null>(null);
  const lastScroll = useRef(0);
  const [picker, setPicker] = useState("");

  useLayoutEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    if (keep.current != null) {
      el.scrollTop = el.scrollHeight - keep.current;
      keep.current = null;
      return;
    }
    if (stick.current) el.scrollTop = el.scrollHeight;
  }, [messages]);

  function requestOlder() {
    const el = boxRef.current;
    if (!hasOlder || !el) return;
    if (!onLoadOlder()) return;
    keep.current = el.scrollHeight - el.scrollTop;
    stick.current = false;
  }

  return (
    <ol
      className="messages"
      ref={boxRef}
      onScroll={(event) => {
        const el = event.currentTarget;
        const goingUp = el.scrollTop < lastScroll.current;
        lastScroll.current = el.scrollTop;
        stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
        if (goingUp && el.scrollTop < 80) requestOlder();
      }}
    >
      {hasOlder ? (
        <li className="history-more">
          <button type="button" onClick={requestOlder}>
            پیام‌های قبلی
          </button>
        </li>
      ) : null}
      {messages.map((message, index) => {
        const mine = message.ClientID === userId;
        const id = message.UniqId || message._id || "";
        const secret = message.Type === "secret" || Boolean(message.SecretState);
        const reveal = message.Preview || "";
        const body = !secret
          ? message.Content
          : reveal
            ? reveal
            : message.SecretState === "purged"
              ? "پیام محرمانه حذف شد"
              : message.SecretState === "viewed"
                ? "مشاهده شد"
                : "پیام محرمانه";
        const locked = secret && !reveal && message.SecretState !== "viewed" && message.SecretState !== "purged";
        return (
          <li key={id || index} className={mine ? "bubble mine" : "bubble"}>
            {!mine ? <span className="who">{message.Username || message.ClientID}</span> : null}
            {message.ForwardFrom ? (
              <span className="forwarded">فوروارد از {message.ForwardFrom.username || "گفتگو"}</span>
            ) : null}
            {locked ? (
              <button type="button" className="secret-lock" onClick={() => id && onView(id)}>
                {body}
              </button>
            ) : message.Type === "file" ? (
              <FileCard content={message.Content} />
            ) : (
              <p className={secret ? "secret-note" : undefined}>{body}</p>
            )}
            {message.Reactions?.length ? (
              <span className="reacts">
                {message.Reactions.map((group) => (
                  <button
                    key={group.emoji}
                    type="button"
                    className={group.user_ids?.includes(userId) ? "react-chip mine" : "react-chip"}
                    onClick={() => id && onReact(id, group.emoji)}
                  >
                    {group.emoji} {group.count}
                  </button>
                ))}
              </span>
            ) : null}
            <span className="meta">
              <time>{formatClock(message.CreatedAt)}</time>
              {message.Edited ? <span className="edited">ویرایش شده</span> : null}
              {mine ? <ReceiptMark status={message.receipt ?? "sent"} /> : null}
              {id && mine && !secret && (message.Type === "text" || !message.Type) && message.Type !== "file" ? (
                <button type="button" className="react-open" onClick={() => onEdit(id, message.Content || "")}>
                  ویرایش
                </button>
              ) : null}
              {id && !secret ? (
                <button type="button" className="react-open" onClick={() => onForward(id)}>
                  فوروارد
                </button>
              ) : null}
              {id ? (
                <button type="button" className="react-open" onClick={() => setPicker((current) => (current === id ? "" : id))}>
                  🙂
                </button>
              ) : null}
            </span>
            {picker === id ? (
              <span className="picker">
                {REACTIONS.map((emoji) => (
                  <button
                    key={emoji}
                    type="button"
                    onClick={() => {
                      onReact(id, emoji);
                      setPicker("");
                    }}
                  >
                    {emoji}
                  </button>
                ))}
              </span>
            ) : null}
          </li>
        );
      })}
    </ol>
  );
}

function ReceiptMark({ status }: { status: Receipt }) {
  if (status === "sending") return <span className="ticks wait">◌</span>;
  if (status === "sent") return <span className="ticks">✓</span>;
  if (status === "delivered") return <span className="ticks">✓✓</span>;
  return <span className="ticks seen">✓✓</span>;
}

function FileCard({ content }: { content?: string }) {
  const file = filePayload(content);
  if (!file) return <p>فایل</p>;
  const image = file.mime?.startsWith("image/");
  return (
    <a className="file-card" href={file.url} target="_blank" rel="noreferrer">
      {image ? <img src={file.url} alt={file.name} /> : <span className="file-mark">فایل</span>}
      <span>
        <strong>{file.name}</strong>
        {file.size ? <small>{formatBytes(file.size)}</small> : null}
      </span>
    </a>
  );
}

function Composer({
  onSend,
  onFile,
  disabled,
  initial = "",
  editing = false,
  onCancel,
}: {
  onSend: (text: string, secret?: boolean) => void;
  onFile?: (file: File) => Promise<void>;
  disabled: boolean;
  initial?: string;
  editing?: boolean;
  onCancel?: () => void;
}) {
  const [text, setText] = useState(initial);
  const [secret, setSecret] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [fileError, setFileError] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);
  function submit() {
    if (!text.trim() || uploading) return;
    onSend(text, !editing && secret);
    if (!editing) {
      setText("");
      setSecret(false);
    }
  }
  async function chooseFile(file: File | undefined) {
    if (!file || !onFile) return;
    setFileError("");
    setUploading(true);
    try {
      await onFile(file);
    } catch (err) {
      setFileError(err instanceof Error ? err.message : "ارسال فایل شکست خورد");
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = "";
    }
  }
  return (
    <form
      className="composer"
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      {editing ? (
        <button type="button" className="react-open" onClick={onCancel}>
          لغو
        </button>
      ) : (
        <>
          <button
            type="button"
            className="react-open"
            disabled={disabled || uploading}
            onClick={() => fileRef.current?.click()}
          >
            {uploading ? "…" : "فایل"}
          </button>
          <input
            ref={fileRef}
            type="file"
            hidden
            onChange={(event) => void chooseFile(event.target.files?.[0])}
          />
          <button
            type="button"
            className={secret ? "react-open secret-on" : "react-open"}
            onClick={() => setSecret((current) => !current)}
          >
            محرمانه
          </button>
        </>
      )}
      <textarea
        rows={1}
        value={text}
        disabled={disabled}
        placeholder={editing ? "متن تازه…" : secret ? "پیام محرمانه…" : "پیام بنویس…"}
        onChange={(event) => setText(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            submit();
          }
        }}
      />
      <button type="submit" className="send" disabled={disabled || uploading || !text.trim()} aria-label={editing ? "ثبت ویرایش" : "ارسال"}>
        {editing ? "ثبت" : "ارسال"}
      </button>
      {fileError ? <p className="toast">{fileError}</p> : null}
    </form>
  );
}

function NewChat({
  defaultMembers,
  onClose,
  onCreated,
}: {
  defaultMembers: string;
  onClose: () => void;
  onCreated: (roomId: string) => void;
}) {
  const [id, setId] = useState("demo");
  const [name, setName] = useState("گروه تست");
  const [members, setMembers] = useState(defaultMembers);
  const [error, setError] = useState("");

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      await createRoom({
        id: id.trim(),
        name: name.trim(),
        memberIds: members.split(",").map((item) => item.trim()).filter(Boolean),
      });
      onCreated(id.trim());
    } catch (err) {
      setError(err instanceof Error ? err.message : "ساخت گفتگو شکست خورد");
    }
  }

  return (
    <div className="modal-back" onClick={onClose}>
      <form className="modal" onClick={(event) => event.stopPropagation()} onSubmit={onSubmit}>
        <h2>گفتگوی جدید</h2>
        <label>
          شناسه
          <input value={id} onChange={(event) => setId(event.target.value)} required />
        </label>
        <label>
          نام
          <input value={name} onChange={(event) => setName(event.target.value)} required />
        </label>
        <label>
          شناسه اعضا
          <input value={members} onChange={(event) => setMembers(event.target.value)} />
        </label>
        {error ? <p className="toast">{error}</p> : null}
        <div className="modal-actions">
          <button type="button" onClick={onClose}>
            انصراف
          </button>
          <button type="submit" className="send">
            ساخت
          </button>
        </div>
      </form>
    </div>
  );
}

function statusLabel(status: ConnectionStatus) {
  if (status === "open") return "آنلاین";
  if (status === "connecting") return "در حال اتصال";
  if (status === "closed") return "قطع شد";
  return "برای شروع یک حساب را انتخاب کن";
}
