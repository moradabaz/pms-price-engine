import { defineConfig } from "astro/config";

// Static site, no JS framework needed for a single presentation page.
export default defineConfig({
  output: "static",
});
