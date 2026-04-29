import type { DetailModel, SearchConfig, SessionData, WorkspaceProgress } from "./types";

export type WorkspaceMenuCommand = "open-inputs" | "load-foundation" | "load-review-sample" | "open-finder";

export type AuthRole = "Administrator" | "User";

export type AuthState = {
  configured: boolean;
  authenticated: boolean;
  username?: string;
  rememberedUsername?: string;
  keepSignedIn?: boolean;
  role?: AuthRole;
  adminUsername: string;
  availableUsernames: string[];
  pendingPasswordResetCount?: number;
};

export type AuthResult = AuthState & {
  ok: boolean;
  error?: string;
};

export type AdminUserSummary = {
  username: string;
  role: AuthRole;
  builtIn: boolean;
  current: boolean;
  passwordResetRequired?: boolean;
};

export type AdminUsersResult = {
  ok: boolean;
  error?: string;
  users: AdminUserSummary[];
};

export interface NativeApi {
  pickInputPaths(): Promise<string[]>;
  openReferenceLibraryWindow(): Promise<void>;
  openTimeConvertTool(): Promise<void>;
  loadSampleSession(): Promise<SessionData>;
  loadReviewSampleSession(): Promise<SessionData>;
  ingestDroppedPaths(paths: string[]): Promise<SessionData>;
  getLineDetail(lineId: string, sessionId?: string): Promise<DetailModel | null>;
  warmLineDetails(lineIds: string[], sessionId?: string): Promise<void>;
  getPathForDroppedFile(file: File): string;
  updateSearch(config: SearchConfig): Promise<void>;
  onWorkspaceMenuCommand(listener: (command: WorkspaceMenuCommand, paths?: string[]) => void): () => void;
  onWorkspaceProgress(listener: (progress: WorkspaceProgress) => void): () => void;
}
