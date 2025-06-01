import { defineConfig } from "tsup";

export default defineConfig({
	entry: ["src/index.ts"],
	outDir: "dist",
	clean: true,
	format: ["esm", "cjs"],
	target: ["es2022", "node20"],
	dts: true,
	minify: false,
	sourcemap: false,
	splitting: true
});
