# Log Analyzer

Webapp-based TMDS / Genisys log analysis tool.

## Run

```powershell
npm install
npm start
```

`npm start` builds the webapp and starts a TMDS server at `http://127.0.0.1:4173/` by default.
The browser UI supports file upload, folder upload, drag/drop upload, reference library browsing, selected-line details, and finder search.

This project is source-first and cross-platform. On another Windows, macOS, or ARM PC, install Node.js, run `npm install`, then run `npm start`.

On Windows, `TMDS-Server-Switch.bat` is the single on/off switch. Run it once to turn the TMDS server on (it also pulls the latest update from the configured update source, runs `npm install` if needed, builds the webapp, and starts the hosted server in the background); run it again to turn the TMDS server off.

`Open-TMDS-Webapp.html` is only the entry page. It can redirect either to the local launcher URL or to one shared TMDS server URL. The parser and grounding data still live behind the running server; opening a copied app HTML directly without a TMDS server behind it will fail.

## Carry Or Share

Carry or sync this project folder as the tool folder:

- `app/`
- `exports/`
- `reference_assets/`
- `sample_logs/`
- `scripts/`
- `package.json`
- `package-lock.json`
- `README.md`
- `tsconfig.json`
- `tsconfig.node.json`
- `vite.config.cjs`
- `TMDS-Server-Switch.bat`
- `Open-TMDS-Webapp.html`

`node_modules/` is optional. Keep it if the target PC will not have internet access for `npm install`; otherwise it can be recreated.

## Optional Update Source

Automatic updates require a source to update from. Put the master copy in OneDrive, a network share, or another synced folder, then create a `.tmds-update-source` file in the copied tool folder containing the master folder path.

Example `.tmds-update-source`:

```text
C:\Path\To\TMDS-Webapp-Master
```

When `TMDS-Server-Switch.bat` runs to turn the server on, it copies the current source files from that configured source before starting. It then checks whether the local dependency install is stale and refreshes `node_modules` automatically when the synced `package-lock.json` or `package.json` changed. The source sync is non-destructive; it does not delete local files.

If everyone should receive updates automatically, do not send separate disconnected copies. Put one master copy in a synced location, update that master copy, and point each user folder's `.tmds-update-source` at it. If everyone opens one hosted URL instead of a local folder, updates happen when the hosted server is redeployed.

## Shared Server Mode

If you want one always-current master tool that everyone opens, run the TMDS server from the master copy on your machine or another host PC.

Create these optional files in the master copy:

```text
.tmds-web-host
.tmds-web-port
```

Example values:

```text
.tmds-web-host -> 0.0.0.0
.tmds-web-port -> 4173
```

Example shared URL for a deployment:

```text
http://YOUR-PC:4173/
```

Then run `TMDS-Server-Switch.bat` on the host machine. Other users should open either:

- the shared server URL directly, such as `http://YOUR-PC:4173/`, or
- `Open-TMDS-Webapp.html` with that shared URL saved in the entry page

Browser users can still drag and drop folders and files into the shared tool. Uploaded browser sessions are now tracked by backend session ID so one user's uploaded logs do not replace another user's current parse session.

## Local-Only Fallback (No Server)

The renderer auto-detects when no TMDS server is reachable and switches to **Local mode**. In local mode, log files are parsed entirely in the browser. Users see line lists, timestamps, and the finder search; reference library, sample review logs, and per-line detail panels are disabled (those require the server).

The local parser handles every format the server-side parser handles: plain text logs, gzip (`.gz`), and ZIP archives — including ZIPs that contain nested gzips, text logs, or other ZIPs (up to four levels deep, matching the server). Drag-and-drop folders work too.

To deploy a local-mode fallback so users can keep working when your home PC is off:

1. Run `npm run build` once to produce `dist/renderer/` (a self-contained static bundle: `index.html`, `main.js`, `main.css`).
2. Upload the contents of `dist/renderer/` to any free static host:
   - **GitHub Pages** — push to a `gh-pages` branch on a public/private repo
   - **Cloudflare Pages** — drag the folder into a new project at <https://dash.cloudflare.com/?to=/:account/pages>
   - **Netlify** — drag the folder into a new site at <https://app.netlify.com/drop>
3. Share the resulting URL as a backup. Visitors can then drag/drop or pick log files and the parser runs entirely in their browser.

You can also force local mode at the home URL by appending `?local=1` to it. Opening the bundled `index.html` directly via `file://` also forces local mode.

## Free Always-Up URL via GitHub Pages

The repo ships with `.github/workflows/deploy-pages.yml`, a workflow that builds the renderer and publishes it to GitHub Pages on every push to `main`. Result: a free `https://<username>.github.io/<repo>/` URL that's always reachable, auto-updates when you push, and prompts visitors with **Update available — Update now** within ~60 seconds of a new build.

One-time setup:

1. Create a new repo on GitHub. Use a public repo for GitHub Free, or a private repo only if the account/organization plan supports GitHub Pages from private repositories. The published Pages site is public, so do not commit secrets or private host URLs.
2. From this folder, initialize git and push:
   ```powershell
   git init -b main
   git add .
   git commit -m "Initial Log Analyzer"
   git remote add origin https://github.com/<username>/<repo>.git
   git push -u origin main
   ```
3. On the repo's **Settings → Pages**, under **Build and deployment**, set **Source** to `GitHub Actions`.
4. Wait for the first **Deploy renderer to GitHub Pages** workflow run to finish (Actions tab); the deployed URL appears at the top of that page once it goes green.

After that, every `git push` to `main` redeploys, and any user with the page open will see the **Update available** banner appear automatically. The banner has **Update now** (reload to fresh build) and **Later** (dismiss until a newer version arrives) so nobody loses work mid-task.

Two notes:
- The static URL serves only the renderer. It auto-detects no server and parses files in the browser; server-only features (reference library, sample review logs, and fully grounded detail panels) are gated until the offline parser port is finished.
- Both `*.github.io` and `*.pages.dev` (Cloudflare Pages, same workflow concept) are usually allowed by corporate URL filters that block Tailscale Funnel hostnames, so this is the path through restricted networks at work.

## Test

```powershell
npm run typecheck
npm run build
```

## Required Project Data

- `app/` contains the web UI, parser server, and shared types.
- `exports/` contains the local TMDS/ICD grounding data used by decoder and reference output.
- `reference_assets/` contains packaged reference assets needed on other PCs, including the message-exchange diagram source.
- `sample_logs/` contains small smoke-test logs.
- `scripts/` contains source-discovery and export utilities for regenerating local grounding data.

Do not delete `exports/` or `reference_assets/`; the webapp uses them at runtime.
