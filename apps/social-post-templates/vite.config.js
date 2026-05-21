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
        greenBodyLogoHalf: "green/slides/body-logo-half.html",
        greenBodyTextImage: "green/slides/body-text-image.html",
        greenCta: "green/slides/cta.html",

        red: "red/index.html",
        redComposer: "red/composer.html",
        redCover: "red/slides/cover.html",
        redBodyImageText: "red/slides/body-image-text.html",
        redBodyChart: "red/slides/body-chart.html",
        redBodyText: "red/slides/body-text.html",
        redCta: "red/slides/cta.html",

        white: "white/index.html",
        whiteComposer: "white/composer.html",
        whiteCover: "white/slides/cover.html",
        whiteBodyImageText: "white/slides/body-image-text.html",
        whiteBodyChart: "white/slides/body-chart.html",
        whiteBodyText: "white/slides/body-text.html",
        whiteCta: "white/slides/cta.html",

        navy: "navy/index.html",
        navyComposer: "navy/composer.html",
        navyCover: "navy/slides/cover.html",
        navyBodyImageText: "navy/slides/body-image-text.html",
        navyBodyChart: "navy/slides/body-chart.html",
        navyBodyText: "navy/slides/body-text.html",
        navyCta: "navy/slides/cta.html",
      },
    },
  },
});
