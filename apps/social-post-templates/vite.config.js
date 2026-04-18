import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [tailwindcss()],
  build: {
    rollupOptions: {
      input: {
        main: "index.html",
        green: "green/index.html",
        greenComposer: "green/composer.html",
        greenCover: "green/slides/cover.html",
        greenBodyImageText: "green/slides/body-image-text.html",
        greenBodyChart: "green/slides/body-chart.html",
        greenBodyText: "green/slides/body-text.html",
        greenCta: "green/slides/cta.html",
        navy: "navy/index.html",
        white: "white/index.html",
      },
    },
  },
});
