import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [tailwindcss()],
  build: {
    rollupOptions: {
      input: {
        green: "green/index.html",
        navy: "navy/index.html",
        white: "white/index.html",
      },
    },
  },
});
