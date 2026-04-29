import { rm, mkdir, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const distDir = resolve(root, "dist");
const rendererOut = resolve(distDir, "renderer");
const mainOut = resolve(distDir, "main");

function buildVersionStamp() {
  const now = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}-${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
}

const buildVersion = process.env.TMDS_BUILD_VERSION || buildVersionStamp();
const buildAt = new Date().toISOString();

async function clean(paths) {
  for (const path of paths) {
    await rm(path, { recursive: true, force: true });
  }
}

async function ensureDir(path) {
  await mkdir(path, { recursive: true });
}

async function buildRenderer() {
  await ensureDir(rendererOut);
  const result = await build({
    entryPoints: [resolve(root, "app/renderer/src/main.tsx")],
    bundle: true,
    format: "esm",
    platform: "browser",
    target: ["chrome120"],
    outdir: rendererOut,
    entryNames: "[name]",
    loader: {
      ".css": "css",
    },
    jsx: "automatic",
    define: {
      "process.env.NODE_ENV": '"production"',
      __TMDS_BUILD_VERSION__: JSON.stringify(buildVersion),
      __TMDS_BUILD_AT__: JSON.stringify(buildAt),
    },
    logLevel: "info",
    sourcemap: true,
    metafile: true,
    assetNames: "assets/[name]",
  });

  const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>TMDS Log Analyzer</title>
    <link rel="stylesheet" href="./main.css" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="./main.js"></script>
  </body>
</html>
`;
  await writeFile(resolve(rendererOut, "index.html"), html, "utf8");
  await writeFile(
    resolve(rendererOut, "version.json"),
    JSON.stringify({ version: buildVersion, buildAt }) + "\n",
    "utf8",
  );
  return result;
}

async function buildMain() {
  await ensureDir(mainOut);
  return build({
    entryPoints: [
      resolve(root, "app/main/main.ts"),
      resolve(root, "app/main/preload.ts"),
    ],
    bundle: true,
    platform: "node",
    format: "cjs",
    target: ["node20"],
    outdir: mainOut,
    entryNames: "[name]",
    external: ["electron"],
    logLevel: "info",
    sourcemap: true,
    metafile: true,
  });
}

async function main() {
  const mode = process.argv[2] ?? "all";
  await clean([rendererOut, mainOut]);
  await ensureDir(distDir);

  if (mode === "renderer") {
    await buildRenderer();
    return;
  }

  if (mode === "main") {
    await buildMain();
    return;
  }

  await buildRenderer();
  await buildMain();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
