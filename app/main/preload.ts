import { contextBridge, ipcRenderer, webUtils } from "electron";
import type { NativeApi } from "../shared/native-api";
import type { SearchConfig } from "../shared/types";

function subscribe(channel: string, listener: (...args: unknown[]) => void): () => void {
  const wrapped = (_event: unknown, ...args: unknown[]) => listener(...args);
  ipcRenderer.on(channel, wrapped);
  return () => ipcRenderer.removeListener(channel, wrapped);
}

contextBridge.exposeInMainWorld("tmds", {
  pickInputPaths: () => ipcRenderer.invoke("workspace:pick-inputs"),
  openReferenceLibraryWindow: () => ipcRenderer.invoke("workspace:open-reference-window"),
  openTimeConvertTool: () => ipcRenderer.invoke("workspace:open-time-convert-tool"),
  loadSampleSession: () => ipcRenderer.invoke("workspace:load-samples"),
  loadReviewSampleSession: () => ipcRenderer.invoke("workspace:load-review-sample"),
  ingestDroppedPaths: (paths: string[]) => ipcRenderer.invoke("workspace:ingest-paths", paths),
  getLineDetail: (lineId: string, sessionId?: string) => ipcRenderer.invoke("workspace:get-line-detail", { lineId, sessionId }),
  warmLineDetails: (lineIds: string[], sessionId?: string) => ipcRenderer.invoke("workspace:warm-line-details", { lineIds, sessionId }),
  getPathForDroppedFile: (file: File) => webUtils.getPathForFile(file),
  updateSearch: (config: SearchConfig) => ipcRenderer.invoke("workspace:update-search", config),
  onWorkspaceMenuCommand: (listener) => {
    const disposers = [
      subscribe("workspace:menu-open-inputs", (paths: string[]) => listener("open-inputs", paths)),
      subscribe("workspace:menu-load-foundation", () => listener("load-foundation")),
      subscribe("workspace:menu-load-review-sample", () => listener("load-review-sample")),
      subscribe("workspace:menu-open-finder", () => listener("open-finder")),
    ];
    return () => disposers.forEach((dispose) => dispose());
  },
  onWorkspaceProgress: (listener) => subscribe("workspace:progress", listener),
} satisfies NativeApi);

declare global {
  interface Window {
    tmds: NativeApi;
  }
}
