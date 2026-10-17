import { useCallback, useEffect, useRef, useState } from "react";
import { uploadFile, wsURL } from "./api";
import type { ChatMessage, ChatRoom, ConnectionStatus, ReactionGroup, Receipt, ServerEvent } from "./types";

function asRoom(value: unknown): ChatRoom | null {
  if (!value || typeof value !== "object") return null;
  const room = value as ChatRoom;
  if (!room.Id) return null;
  return room;
}

function asRooms(value: unknown): ChatRoom[] {
  if (!Array.isArray(value)) return [];
  return value.map(asRoom).filter((room): room is ChatRoom => room !== null);
}

function asMessages(value: unknown): ChatMessage[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is ChatMessage => !!item && typeof item === "object");
}

const receiptRank: Record<Receipt, number> = { sending: 0, sent: 1, delivered: 2, seen: 3 };

function bump(current: Receipt | undefined, next: Receipt): Receipt {
  if (!current || receiptRank[next] > receiptRank[current]) return next;
  return current;
}

function receiptOf(message: ChatMessage): Receipt {
  const others = (ids?: string[]) => (ids ?? []).some((id) => id && id !== message.ClientID);
  if (others(message.Read)) return "seen";
  if (others(message.Deliver)) return "delivered";
  if (message.receipt) return message.receipt;
  return "sent";
}

function withReceipt(message: ChatMessage): ChatMessage {
  return { ...message, receipt: bump(message.receipt, receiptOf(message)) };
}

const historyPageSize = 10;

function messageKey(message: ChatMessage): string {
  return message.UniqId || message._id || "";
}

function byTime(list: ChatMessage[]): ChatMessage[] {
  return [...list].sort((a, b) => {
    const at = Date.parse(a.CreatedAt || "") || 0;
    const bt = Date.parse(b.CreatedAt || "") || 0;
    return at - bt;
  });
}

function markThrough(list: ChatMessage[], id: string, status: Receipt): ChatMessage[] {
  const index = list.findIndex((item) => item.UniqId === id || item._id === id);
  if (index < 0) return list;
  const author = list[index].ClientID;
  return list.map((item, i) => {
    if (i > index || item.ClientID !== author) return item;
    return { ...item, receipt: bump(item.receipt, status) };
  });
}

function applyGroups(list: ChatMessage[], id: string, reactions: ReactionGroup[]): ChatMessage[] {
  return list.map((item) => (item.UniqId === id || item._id === id ? { ...item, Reactions: reactions } : item));
}

function toggleLocal(groups: ReactionGroup[] | undefined, userId: string, emoji: string): ReactionGroup[] {
  const list = (groups ?? []).map((group) => ({ ...group, user_ids: [...(group.user_ids ?? [])] }));
  let already = false;
  for (const group of list) {
    if (group.emoji === emoji && group.user_ids.includes(userId)) already = true;
    group.user_ids = group.user_ids.filter((id) => id !== userId);
    group.count = group.user_ids.length;
  }
  if (!already) {
    const found = list.find((group) => group.emoji === emoji);
    if (found) {
      found.user_ids.push(userId);
      found.count = found.user_ids.length;
    } else {
      list.push({ emoji, count: 1, user_ids: [userId] });
    }
  }
  return list.filter((group) => group.count > 0);
}

export function useChat(token: string) {
  const socketRef = useRef<WebSocket | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("idle");
  const [error, setError] = useState("");
  const [userId, setUserId] = useState("");
  const [rooms, setRooms] = useState<ChatRoom[]>([]);
  const [activeRoomId, setActiveRoomId] = useState("");
  const [messagesByRoom, setMessagesByRoom] = useState<Record<string, ChatMessage[]>>({});
  const [notice, setNotice] = useState("");
  const revealTimers = useRef<Record<string, number>>({});
  const purgeAfter = useRef(new Set<string>());

  const send = useCallback((payload: Record<string, string>) => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setError("اتصال باز نیست");
      return;
    }
    socket.send(JSON.stringify(payload));
  }, []);

  const joinRoom = useCallback(
    (roomId: string) => {
      setActiveRoomId(roomId);
      pageByRoom.current[roomId] = 1;
      moreByRoom.current[roomId] = true;
      loadingOlder.current = false;
      setHasOlder(true);
      send({ event_type: "joinRoom", room_id: roomId, page: "1" });
    },
    [send],
  );

  const loadOlder = useCallback(() => {
    const roomId = activeRef.current;
    if (!roomId || loadingOlder.current || moreByRoom.current[roomId] === false) return false;
    loadingOlder.current = true;
    const next = (pageByRoom.current[roomId] || 1) + 1;
    send({ event_type: "roomHistory", room_id: roomId, item: String(next) });
    return true;
  }, [send]);

  const leaveRoom = useCallback(
    (roomId: string) => {
      send({ event_type: "leaveRoom", room_id: roomId });
      setActiveRoomId((current) => (current === roomId ? "" : current));
    },
    [send],
  );

  const userIdRef = useRef(userId);
  userIdRef.current = userId;
  const activeRef = useRef(activeRoomId);
  activeRef.current = activeRoomId;

  const sendRef = useRef(send);
  sendRef.current = send;
  const seenSent = useRef(new Set<string>());
  const pageByRoom = useRef<Record<string, number>>({});
  const moreByRoom = useRef<Record<string, boolean>>({});
  const loadingOlder = useRef(false);
  const [hasOlder, setHasOlder] = useState(false);
  const messagesRef = useRef(messagesByRoom);
  messagesRef.current = messagesByRoom;

  const ackSeen = useCallback((roomId: string, list: ChatMessage[]) => {
    const last = [...list].reverse().find((item) => item.ClientID && item.ClientID !== userIdRef.current);
    const id = last?.UniqId || last?._id;
    if (!roomId || !id || seenSent.current.has(id)) return;
    seenSent.current.add(id);
    sendRef.current({ event_type: "seenMessage", item: id });
  }, []);

  const sendText = useCallback(
    (roomId: string, content: string, secret = false) => {
      const text = content.trim();
      if (!text) return;
      const ulid = crypto.randomUUID();
      const pending: ChatMessage = {
        Content: secret ? "" : text,
        Type: secret ? "secret" : "text",
        UniqId: ulid,
        RoomID: roomId,
        ClientID: userIdRef.current,
        CreatedAt: new Date().toISOString(),
        receipt: "sending",
        SecretState: secret ? "locked" : undefined,
      };
      setMessagesByRoom((current) => ({
        ...current,
        [roomId]: [...(current[roomId] ?? []), pending],
      }));
      setRooms((current) =>
        current.map((room) => (room.Id === roomId ? { ...room, Message: pending } : room)),
      );
      send({
        event_type: "sendMessage",
        room_id: roomId,
        content: text,
        ulid,
        type: secret ? "secret" : "text",
      });
    },
    [send],
  );

  const sendFile = useCallback(
    async (roomId: string, file: File) => {
      if (!roomId || !file || !token) return;
      const uploaded = await uploadFile(token, file);
      const content = JSON.stringify({
        name: file.name,
        url: uploaded.url,
        mime: uploaded.mime_type || file.type,
        size: uploaded.size || file.size,
      });
      const ulid = crypto.randomUUID();
      const pending: ChatMessage = {
        Content: content,
        Type: "file",
        UniqId: ulid,
        RoomID: roomId,
        ClientID: userIdRef.current,
        CreatedAt: new Date().toISOString(),
        receipt: "sending",
      };
      setMessagesByRoom((current) => ({
        ...current,
        [roomId]: [...(current[roomId] ?? []), pending],
      }));
      setRooms((current) =>
        current.map((room) => (room.Id === roomId ? { ...room, Message: pending } : room)),
      );
      send({
        event_type: "sendMessage",
        room_id: roomId,
        content,
        ulid,
        type: "file",
      });
    },
    [send, token],
  );

  const react = useCallback(
    (roomId: string, messageId: string, emoji: string) => {
      if (!roomId || !messageId || !emoji) return;
      const me = userIdRef.current;
      setMessagesByRoom((current) => ({
        ...current,
        [roomId]: (current[roomId] ?? []).map((item) =>
          item.UniqId === messageId || item._id === messageId
            ? { ...item, Reactions: toggleLocal(item.Reactions, me, emoji) }
            : item,
        ),
      }));
      send({ event_type: "reactMessage", room_id: roomId, message_id: messageId, content: emoji });
    },
    [send],
  );

  const editText = useCallback(
    (roomId: string, messageId: string, content: string) => {
      const text = content.trim();
      if (!roomId || !messageId || !text) return;
      setMessagesByRoom((current) => ({
        ...current,
        [roomId]: (current[roomId] ?? []).map((item) =>
          item.UniqId === messageId || item._id === messageId ? { ...item, Content: text, Edited: true } : item,
        ),
      }));
      setRooms((current) =>
        current.map((room) =>
          room.Id === roomId && (room.Message?.UniqId === messageId || room.Message?._id === messageId)
            ? { ...room, Message: { ...room.Message, Content: text, Edited: true } }
            : room,
        ),
      );
      send({ event_type: "editMessage", room_id: roomId, message_id: messageId, content: text });
    },
    [send],
  );

  const forwardTo = useCallback(
    (roomId: string, messageId: string, targetRoomId: string) => {
      if (!roomId || !messageId || !targetRoomId) return;
      send({
        event_type: "forwardMessage",
        room_id: roomId,
        message_id: messageId,
        target_room: targetRoomId,
      });
      setNotice("پیام فوروارد شد");
    },
    [send],
  );

  const sealSecret = useCallback((id: string, state: "viewed" | "purged") => {
    const patch = (list: ChatMessage[]) =>
      list.map((item) =>
        item.UniqId === id || item._id === id ? { ...item, Content: "", Type: "secret", SecretState: state } : item,
      );
    setMessagesByRoom((current) => {
      const next: Record<string, ChatMessage[]> = {};
      for (const [key, list] of Object.entries(current)) next[key] = patch(list);
      return next;
    });
  }, []);

  const showSecret = useCallback(
    (id: string, text: string) => {
      const mark = (list: ChatMessage[]) =>
        list.map((item) => (item.UniqId === id || item._id === id ? { ...item, Preview: text, Content: "" } : item));
      setMessagesByRoom((current) => {
        const next: Record<string, ChatMessage[]> = {};
        for (const [key, list] of Object.entries(current)) next[key] = mark(list);
        return next;
      });
      window.clearTimeout(revealTimers.current[id]);
      revealTimers.current[id] = window.setTimeout(() => {
        delete revealTimers.current[id];
        const purged = purgeAfter.current.has(id);
        purgeAfter.current.delete(id);
        const state: "viewed" | "purged" = purged ? "purged" : "viewed";
        const clear = (list: ChatMessage[]) =>
          list.map((item) =>
            item.UniqId === id || item._id === id
              ? { ...item, Preview: undefined, Content: "", Type: "secret", SecretState: state }
              : item,
          );
        setMessagesByRoom((current) => {
          const next: Record<string, ChatMessage[]> = {};
          for (const [key, list] of Object.entries(current)) next[key] = clear(list);
          return next;
        });
      }, 4000);
    },
    [],
  );

  const viewSecret = useCallback(
    (roomId: string, messageId: string) => {
      if (!roomId || !messageId) return;
      send({ event_type: "viewSecret", room_id: roomId, message_id: messageId });
    },
    [send],
  );

  const applyEvent = useCallback((event: ServerEvent) => {
    const kind = event.event_type ?? "";
    if (kind === "init") {
      if (event.current_user_id) setUserId(String(event.current_user_id));
      setRooms(asRooms(event.content));
      return;
    }
    if (kind === "listRooms") {
      setRooms(asRooms(event.content));
      return;
    }
    if (kind === "createNewRoom") {
      const room = asRoom(event.content);
      if (!room) return;
      setRooms((current) => (current.some((item) => item.Id === room.Id) ? current : [room, ...current]));
      return;
    }
    if (kind === "roomHistory") {
      const roomId = event.room_id ?? "";
      if (!roomId) return;
      const page = Number(event.page || "1") || 1;
      const list = byTime(
        asMessages(event.content).map((item) => {
          const message = withReceipt(item);
          if (message.Type === "secret") message.Content = "";
          return message;
        }),
      );
      const existing = messagesRef.current[roomId] ?? [];
      const seen = new Set(existing.map(messageKey));
      const fresh = list.filter((item) => {
        const key = messageKey(item);
        return key && !seen.has(key);
      });
      const next = byTime([...fresh, ...existing]);
      messagesRef.current = { ...messagesRef.current, [roomId]: next };
      const more = list.length >= historyPageSize;
      pageByRoom.current[roomId] = Math.max(pageByRoom.current[roomId] || 1, page);
      moreByRoom.current[roomId] = more;
      loadingOlder.current = false;
      setMessagesByRoom(messagesRef.current);
      if (roomId === activeRef.current) {
        setHasOlder(more);
        if (page <= 1) ackSeen(roomId, list);
      }
      return;
    }
    if (kind === "roomMessage") {
      const message = event.content as ChatMessage | undefined;
      const roomId = event.room_id || message?.RoomID || "";
      if (!roomId || !message) return;
      const incoming = withReceipt(message);
      if (incoming.Type === "secret") {
        incoming.Content = "";
        incoming.SecretState = incoming.SecretState || "locked";
      }
      setMessagesByRoom((current) => {
        const list = current[roomId] ?? [];
        if (message.UniqId && list.some((item) => item.UniqId === message.UniqId)) {
          return {
            ...current,
            [roomId]: list.map((item) =>
              item.UniqId === message.UniqId
                ? { ...item, ...incoming, receipt: bump(item.receipt, incoming.receipt ?? "sent") }
                : item,
            ),
          };
        }
        return { ...current, [roomId]: [...list, incoming] };
      });
      if (roomId === activeRef.current && message.ClientID && message.ClientID !== userIdRef.current) {
        ackSeen(roomId, [message]);
      }
      setRooms((current) =>
        current.map((room) => {
          if (room.Id !== roomId) return room;
          const unread = activeRef.current === roomId ? 0 : (room.CountMessages ?? 0) + 1;
          return { ...room, Message: message, CountMessages: unread };
        }),
      );
      return;
    }
    if (kind === "error") {
      setError(typeof event.content === "string" ? event.content : "خطا از سرور");
      return;
    }
    if (kind === "messageReaction") {
      const body = (event.content ?? event.Content) as { message_id?: string; reactions?: ReactionGroup[] } | undefined;
      const id = body?.message_id ?? "";
      const roomId = event.room_id ?? "";
      if (!id) return;
      const reactions = body?.reactions ?? [];
      setMessagesByRoom((current) => {
        if (roomId && current[roomId]) {
          return { ...current, [roomId]: applyGroups(current[roomId], id, reactions) };
        }
        const next: Record<string, ChatMessage[]> = {};
        for (const [key, list] of Object.entries(current)) next[key] = applyGroups(list, id, reactions);
        return next;
      });
      return;
    }
    if (kind === "messageEdited") {
      const body = (event.content ?? event.Content) as { message_id?: string; content?: string } | undefined;
      const id = body?.message_id ?? "";
      const text = body?.content ?? "";
      const roomId = event.room_id ?? "";
      if (!id || !text) return;
      const patch = (list: ChatMessage[]) =>
        list.map((item) => (item.UniqId === id || item._id === id ? { ...item, Content: text, Edited: true } : item));
      setMessagesByRoom((current) => {
        if (roomId && current[roomId]) return { ...current, [roomId]: patch(current[roomId]) };
        const next: Record<string, ChatMessage[]> = {};
        for (const [key, list] of Object.entries(current)) next[key] = patch(list);
        return next;
      });
      setRooms((current) =>
        current.map((room) =>
          room.Message?.UniqId === id || room.Message?._id === id
            ? { ...room, Message: { ...room.Message, Content: text, Edited: true } }
            : room,
        ),
      );
      return;
    }
    if (kind === "secretBody") {
      const body = (event.content ?? event.Content) as { message_id?: string; content?: string } | undefined;
      const id = body?.message_id ?? "";
      const text = body?.content ?? "";
      if (id && text) showSecret(id, text);
      return;
    }
    if (kind === "secretViewed" || kind === "secretPurged") {
      const body = (event.content ?? event.Content) as { message_id?: string; user_id?: string } | undefined;
      const id = body?.message_id ?? "";
      if (!id) return;
      if (kind === "secretPurged") {
        if (revealTimers.current[id]) purgeAfter.current.add(id);
        else sealSecret(id, "purged");
        return;
      }
      if (body?.user_id && body.user_id === userIdRef.current && !revealTimers.current[id]) {
        sealSecret(id, "viewed");
      }
      return;
    }
    if (kind === "deliverMessage" || kind === "seenMessage") {
      const raw = event.Content ?? event.content;
      if (raw === "ping" || typeof raw === "undefined") return;
      if (typeof raw === "string") {
        const roomId = event.room_id ?? "";
        setMessagesByRoom((current) => {
          if (roomId && current[roomId]) {
            return { ...current, [roomId]: markThrough(current[roomId], raw, "sent") };
          }
          const next: Record<string, ChatMessage[]> = {};
          for (const [id, list] of Object.entries(current)) next[id] = markThrough(list, raw, "sent");
          return next;
        });
        return;
      }
      if (!raw || typeof raw !== "object") return;
      const body = raw as { message_id?: string; room_id?: string };
      const id = body.message_id ?? "";
      const roomId = body.room_id || event.room_id || "";
      if (!id) return;
      const status: Receipt = kind === "seenMessage" ? "seen" : "delivered";
      setMessagesByRoom((current) => {
        if (roomId && current[roomId]) {
          return { ...current, [roomId]: markThrough(current[roomId], id, status) };
        }
        const next: Record<string, ChatMessage[]> = {};
        for (const [key, list] of Object.entries(current)) next[key] = markThrough(list, id, status);
        return next;
      });
      return;
    }
    if (kind === "incomeMessage") {
      setNotice("پیام تازه در اتاقی که باز نیست");
    }
  }, [ackSeen, showSecret, sealSecret]);

  useEffect(() => {
    if (!token) return;
    const socket = new WebSocket(wsURL(token));
    socketRef.current = socket;
    setStatus("connecting");
    setError("");

    socket.onopen = () => setStatus("open");
    socket.onclose = () => {
      setStatus("closed");
      if (socketRef.current === socket) socketRef.current = null;
    };
    socket.onerror = () => setError("اتصال برقرار نشد");
    socket.onmessage = (message) => {
      try {
        applyEvent(JSON.parse(String(message.data)) as ServerEvent);
      } catch {
        setError("پیام سرور قابل خواندن نیست");
      }
    };

    return () => {
      socket.close();
    };
  }, [token, applyEvent]);

  const messages = messagesByRoom[activeRoomId] ?? [];

  return {
    status,
    error,
    userId,
    rooms,
    activeRoomId,
    messages,
    notice,
    joinRoom,
    leaveRoom,
    loadOlder,
    hasOlder,
    sendText,
    sendFile,
    editText,
    forwardTo,
    viewSecret,
    react,
    refreshRooms: () => send({ event_type: "listRooms", item: "1" }),
  };
}
