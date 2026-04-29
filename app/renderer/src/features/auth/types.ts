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
  requiresPasswordCreation?: boolean;
};

export type UserStatus = {
  exists: boolean;
  requiresPasswordCreation: boolean;
  requiresPasswordReset: boolean;
  builtIn?: boolean;
};

export type AdminUserSummary = {
  username: string;
  role: AuthRole;
  builtIn: boolean;
  current: boolean;
  passwordResetRequired?: boolean;
  passwordResetRequestedAt?: string;
  lastLoginAt?: string;
};

export type AdminSessionSummary = {
  username: string;
  role: AuthRole;
  createdAt: string;
  lastSeenAt: string;
};

export const PASSWORD_MIN = 4;
export const PASSWORD_MAX = 16;
