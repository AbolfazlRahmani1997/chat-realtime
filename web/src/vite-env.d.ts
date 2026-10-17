/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CHAT_HTTP?: string;
  readonly VITE_CHAT_WS?: string;
  readonly VITE_CHAT_TARGET?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
