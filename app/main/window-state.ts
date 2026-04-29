import { app } from "electron";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";

export interface WindowState {
  width: number;
  height: number;
  x?: number;
  y?: number;
  maximized: boolean;
}

const defaultState: WindowState = {
  width: 1680,
  height: 980,
  maximized: false,
};

function statePath(): string {
  const root = join(app.getPath("userData"), "window-state.json");
  mkdirSync(dirname(root), { recursive: true });
  return root;
}

export function loadWindowState(): WindowState {
  try {
    const path = statePath();
    if (!existsSync(path)) return defaultState;
    const parsed = JSON.parse(readFileSync(path, "utf-8")) as Partial<WindowState>;
    return {
      width: Number(parsed.width || defaultState.width),
      height: Number(parsed.height || defaultState.height),
      x: typeof parsed.x === "number" ? parsed.x : undefined,
      y: typeof parsed.y === "number" ? parsed.y : undefined,
      maximized: Boolean(parsed.maximized),
    };
  } catch {
    return defaultState;
  }
}

export function saveWindowState(state: WindowState): void {
  try {
    writeFileSync(statePath(), JSON.stringify(state, null, 2), "utf-8");
  } catch {
    // best effort
  }
}
