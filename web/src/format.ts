const avatarColors = ["#e17076", "#faa774", "#a695e7", "#7bc862", "#6ec9cb", "#65aadd", "#ee7aae"];

export function avatarColor(seed: string) {
  let hash = 0;
  for (const char of seed) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return avatarColors[hash % avatarColors.length];
}

export function initials(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "؟";
  if (parts.length === 1) return parts[0].slice(0, 1);
  return parts[0].slice(0, 1) + parts[1].slice(0, 1);
}

export function formatClock(value?: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("fa-IR", { hour: "2-digit", minute: "2-digit" }).format(date);
}

export function previewText(content?: string) {
  const text = (content ?? "").replace(/\s+/g, " ").trim();
  if (!text) return "پیامی نیست";
  return text.length > 42 ? `${text.slice(0, 42)}…` : text;
}

export type FilePayload = {
  name: string;
  url: string;
  mime?: string;
  size?: number;
};

export function filePayload(content?: string): FilePayload | null {
  if (!content) return null;
  try {
    const parsed = JSON.parse(content) as FilePayload;
    if (!parsed?.name || typeof parsed.url !== "string" || !parsed.url.startsWith("/chat/files/")) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function formatBytes(size?: number) {
  if (!size || size < 0) return "";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${Math.round(size / 1024)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

export function messagePreview(message?: { Type?: string; Content?: string }) {
  if (message?.Type === "secret") return "پیام محرمانه";
  if (message?.Type === "file") {
    const file = filePayload(message.Content);
    return file ? `فایل · ${file.name}` : "فایل";
  }
  if (message?.Type === "voice") return "پیام صوتی";
  return previewText(message?.Content);
}
