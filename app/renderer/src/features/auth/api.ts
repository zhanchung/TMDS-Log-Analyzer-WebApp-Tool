import type {
  AdminSessionSummary,
  AdminUserSummary,
  AuthResult,
  AuthRole,
  AuthState,
  UserStatus,
} from "./types";

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method,
    credentials: "same-origin",
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  let payload: unknown = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    const error =
      (payload && typeof payload === "object" && "error" in payload && typeof (payload as { error?: string }).error === "string"
        ? (payload as { error: string }).error
        : null) || `Request failed with status ${response.status}`;
    const result: Record<string, unknown> = { ok: false, error };
    if (payload && typeof payload === "object") {
      Object.assign(result, payload);
    }
    return result as T;
  }
  return (payload ?? {}) as T;
}

export function fetchAuthState(): Promise<AuthState> {
  return request<AuthState>("GET", "/api/auth/state");
}

export function fetchUserStatus(username: string): Promise<UserStatus> {
  return request<UserStatus>("GET", `/api/auth/user-status?username=${encodeURIComponent(username)}`);
}

export function login(username: string, password: string): Promise<AuthResult> {
  return request<AuthResult>("POST", "/api/auth/login", { username, password });
}

export function setPassword(username: string, password: string): Promise<AuthResult> {
  return request<AuthResult>("POST", "/api/auth/set-password", { username, password });
}

export function requestPasswordReset(username: string): Promise<{ ok: boolean; error?: string }> {
  return request<{ ok: boolean; error?: string }>("POST", "/api/auth/request-reset", { username });
}

export function logout(): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>("POST", "/api/auth/logout");
}

export function renameSelf(newUsername: string): Promise<AuthResult> {
  return request<AuthResult>("POST", "/api/auth/rename", { newUsername });
}

export function changePassword(currentPassword: string, newPassword: string): Promise<AuthResult> {
  return request<AuthResult>("POST", "/api/auth/change-password", { currentPassword, newPassword });
}

export function adminRenameUser(currentUsername: string, newUsername: string): Promise<{ ok: boolean; error?: string }> {
  return request("POST", `/api/admin/users/${encodeURIComponent(currentUsername)}/rename`, { newUsername });
}

export function listAdminUsers(): Promise<{ ok: boolean; error?: string; users: AdminUserSummary[] }> {
  return request("GET", "/api/admin/users");
}

export function listAdminSessions(): Promise<{ ok: boolean; error?: string; sessions: AdminSessionSummary[] }> {
  return request("GET", "/api/admin/sessions");
}

export function adminCreateUser(username: string, role: AuthRole): Promise<{ ok: boolean; error?: string }> {
  return request("POST", "/api/admin/users", { username, role });
}

export function adminDeleteUser(username: string): Promise<{ ok: boolean; error?: string }> {
  return request("DELETE", `/api/admin/users/${encodeURIComponent(username)}`);
}

export function adminResetPassword(username: string): Promise<{ ok: boolean; error?: string }> {
  return request("POST", `/api/admin/users/${encodeURIComponent(username)}/reset`);
}

export function adminSetRole(username: string, role: AuthRole): Promise<{ ok: boolean; error?: string }> {
  return request("POST", `/api/admin/users/${encodeURIComponent(username)}/role`, { role });
}
