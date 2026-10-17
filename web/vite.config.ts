import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const chatTarget = process.env.VITE_CHAT_TARGET || "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/chat": {
        target: chatTarget,
        changeOrigin: true,
        ws: true,
      },
    },
  },
});
