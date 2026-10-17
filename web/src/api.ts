import type { ChatRoom } from "./types";

function httpBase() {
  return (import.meta.env.VITE_CHAT_HTTP ?? "").replace(/\/$/, "");
}

export function wsURL(token: string) {
  const explicit = import.meta.env.VITE_CHAT_WS;
  if (explicit) {
    const url = new URL(explicit);
    url.searchParams.set("token", token);
    return url.toString();
  }
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const base = httpBase();
  if (base) {
    const http = new URL(base);
    const wsProto = http.protocol === "https:" ? "wss:" : "ws:";
    return `${wsProto}//${http.host}/chat/ws?token=${encodeURIComponent(token)}`;
  }
  return `${proto}//${location.host}/chat/ws?token=${encodeURIComponent(token)}`;
}

export async function createRoom(input: {
  id: string;
  name: string;
  memberIds: string[];
}) {
  const res = await fetch(`${httpBase()}/chat/api/createRoom`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      Id: input.id,
      Name: input.name,
      Type: "GROUP",
      Member: input.memberIds.map((id) => ({
        Id: id,
        Notification: true,
        Pin: false,
      })),
      Owner: input.memberIds,
      Writer: input.memberIds,
    }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return (await res.json()) as ChatRoom;
}

export type UploadedFile = {
  file_id: string;
  url: string;
  mime_type?: string;
  size?: number;
};

export async function uploadFile(token: string, file: File) {
  const body = new FormData();
  body.append("file", file);
  const res = await fetch(`${httpBase()}/chat/files?token=${encodeURIComponent(token)}`, {
    method: "POST",
    body,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return (await res.json()) as UploadedFile;
}
