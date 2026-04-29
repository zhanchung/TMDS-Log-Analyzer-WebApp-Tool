import { useCallback, useEffect, useMemo, useState } from "react";
import {
  adminCreateUser,
  adminDeleteUser,
  adminRenameUser,
  adminResetPassword,
  adminSetRole,
  listAdminSessions,
  listAdminUsers,
} from "./api";
import type {
  AdminSessionSummary,
  AdminUserSummary,
  AuthRole,
  AuthState,
} from "./types";

type Props = {
  state: AuthState;
  onClose: () => void;
  onAuthStateRefresh?: () => void;
};

function formatDate(value: string | undefined): string {
  if (!value) return "-";
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function getStatusLabel(user: AdminUserSummary): string {
  if (user.passwordResetRequestedAt) return "Reset requested";
  if (user.passwordResetRequired) return "Awaiting password";
  return "Active";
}

function getStatusClass(user: AdminUserSummary): string {
  if (user.passwordResetRequestedAt) return "admin-flag warn";
  if (user.passwordResetRequired) return "admin-flag info";
  return "admin-flag ok";
}

export function AdminPanel({ state, onClose, onAuthStateRefresh }: Props) {
  const [users, setUsers] = useState<AdminUserSummary[]>([]);
  const [sessions, setSessions] = useState<AdminSessionSummary[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newRole, setNewRole] = useState<AuthRole>("User");
  const [renameTarget, setRenameTarget] = useState("");
  const [renameDraft, setRenameDraft] = useState("");

  const refresh = useCallback(async () => {
    setError("");
    try {
      const [usersResult, sessionsResult] = await Promise.all([listAdminUsers(), listAdminSessions()]);
      if (!usersResult.ok) {
        setError(usersResult.error ?? "Could not load users.");
        return;
      }
      setUsers(usersResult.users);
      if (sessionsResult.ok) {
        setSessions(sessionsResult.sessions);
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Could not load admin data.");
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const pendingResets = useMemo(
    () => users.filter((user) => Boolean(user.passwordResetRequestedAt)),
    [users],
  );

  const adminCount = useMemo(
    () => users.filter((user) => user.role === "Administrator").length,
    [users],
  );

  async function withBusy(action: () => Promise<{ ok: boolean; error?: string }>) {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      const result = await action();
      if (!result.ok) {
        setError(result.error ?? "Action failed.");
      }
      await refresh();
      if (result.ok) onAuthStateRefresh?.();
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Action failed.");
    } finally {
      setBusy(false);
    }
  }

  async function handleCreate(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = newUsername.trim();
    if (!trimmed) {
      setError("Enter a username for the new account.");
      return;
    }
    await withBusy(async () => {
      const result = await adminCreateUser(trimmed, newRole);
      if (result.ok) {
        setNewUsername("");
        setNewRole("User");
      }
      return result;
    });
  }

  function startRename(user: AdminUserSummary) {
    setRenameTarget(user.username);
    setRenameDraft(user.username);
    setError("");
  }

  function cancelRename() {
    setRenameTarget("");
    setRenameDraft("");
  }

  async function submitRename(currentUsername: string) {
    const trimmed = renameDraft.trim();
    if (!trimmed) {
      setError("Enter a username.");
      return;
    }
    if (trimmed === currentUsername) {
      cancelRename();
      return;
    }
    await withBusy(async () => {
      const result = await adminRenameUser(currentUsername, trimmed);
      if (result.ok) {
        cancelRename();
      }
      return result;
    });
  }

  return (
    <div className="admin-panel-backdrop admin-console-backdrop" role="dialog" aria-modal="true">
      <div className="admin-panel admin-console">
        <header className="admin-console-hero">
          <div>
            <p className="admin-kicker">Administration</p>
            <h2>User management</h2>
            <p>Signed in as <strong>{state.username}</strong>. Manage accounts, roles, password resets, and active sessions.</p>
          </div>
          <button className="ghost" type="button" onClick={onClose}>Close</button>
        </header>

        <section className="admin-summary-grid">
          <div className="admin-summary-card">
            <span className="admin-summary-label">Total accounts</span>
            <strong>{users.length}</strong>
          </div>
          <div className="admin-summary-card">
            <span className="admin-summary-label">Admins</span>
            <strong>{adminCount}</strong>
          </div>
          <div className="admin-summary-card">
            <span className="admin-summary-label">Online users</span>
            <strong>{sessions.length}</strong>
          </div>
          <div className="admin-summary-card">
            <span className="admin-summary-label">Reset requests</span>
            <strong>{pendingResets.length}</strong>
          </div>
        </section>

        {error ? <div className="auth-error" role="alert">{error}</div> : null}

        <section className="admin-section admin-create-section">
          <div className="admin-section-head">
            <div>
              <h3>Create account</h3>
              <p className="admin-meta">New users choose a password the first time they sign in.</p>
            </div>
          </div>
          <form className="admin-create-form admin-create-form-wide" onSubmit={handleCreate}>
            <label>
              <span>Username</span>
              <input
                type="text"
                value={newUsername}
                onChange={(event) => setNewUsername(event.target.value)}
                disabled={busy}
                maxLength={48}
                placeholder="e.g. Jane Smith"
              />
            </label>
            <label>
              <span>Role</span>
              <select value={newRole} onChange={(event) => setNewRole(event.target.value as AuthRole)} disabled={busy}>
                <option value="User">User</option>
                <option value="Administrator">Administrator</option>
              </select>
            </label>
            <button type="submit" className="primary" disabled={busy}>Add account</button>
          </form>
        </section>

        {pendingResets.length ? (
          <section className="admin-section">
            <div className="admin-section-head">
              <div>
                <h3>Password reset requests</h3>
                <p className="admin-meta">Reset the password, then the user can create a new one at sign-in.</p>
              </div>
            </div>
            <div className="admin-user-list">
              {pendingResets.map((user) => (
                <article key={user.username} className="admin-user-card compact">
                  <div>
                    <strong>{user.username}</strong>
                    <p className="admin-meta">Requested {formatDate(user.passwordResetRequestedAt)}</p>
                  </div>
                  <button
                    type="button"
                    className="primary"
                    disabled={busy}
                    onClick={() => void withBusy(() => adminResetPassword(user.username))}
                  >
                    Reset password
                  </button>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        <section className="admin-section">
          <div className="admin-section-head">
            <div>
              <h3>Accounts</h3>
              <p className="admin-meta">Rename users inline. Spaces are allowed between name parts.</p>
            </div>
          </div>
          <div className="admin-user-list">
            {users.map((user) => {
              const isRenaming = renameTarget === user.username;
              return (
                <article key={user.username} className="admin-user-card">
                  <div className="admin-user-main">
                    <div className="admin-user-copy">
                      {isRenaming ? (
                        <label className="admin-inline-edit">
                          <span>New username</span>
                          <input
                            type="text"
                            value={renameDraft}
                            onChange={(event) => setRenameDraft(event.target.value)}
                            disabled={busy}
                            maxLength={48}
                            autoFocus
                          />
                        </label>
                      ) : (
                        <>
                          <strong>{user.username}</strong>
                          <span className="admin-meta">
                            {user.builtIn ? "Built-in administrator" : user.role}
                            {user.current ? " | you" : ""}
                          </span>
                        </>
                      )}
                    </div>
                    <span className={getStatusClass(user)}>{getStatusLabel(user)}</span>
                  </div>

                  <div className="admin-user-controls">
                    <label>
                      <span>Role</span>
                      {user.builtIn ? (
                        <strong className="admin-role-static">{user.role}</strong>
                      ) : (
                        <select
                          value={user.role}
                          disabled={busy}
                          onChange={(event) =>
                            void withBusy(() => adminSetRole(user.username, event.target.value as AuthRole))
                          }
                        >
                          <option value="User">User</option>
                          <option value="Administrator">Administrator</option>
                        </select>
                      )}
                    </label>
                    <div>
                      <span className="admin-meta">Last login</span>
                      <strong className="admin-role-static">{formatDate(user.lastLoginAt)}</strong>
                    </div>
                  </div>

                  <div className="admin-user-actions">
                    {isRenaming ? (
                      <>
                        <button type="button" className="primary" disabled={busy} onClick={() => void submitRename(user.username)}>
                          Save name
                        </button>
                        <button type="button" className="ghost" disabled={busy} onClick={cancelRename}>
                          Cancel
                        </button>
                      </>
                    ) : (
                      <button type="button" className="ghost" disabled={busy} onClick={() => startRename(user)}>
                        Rename
                      </button>
                    )}
                    {!user.builtIn ? (
                      <>
                        <button
                          type="button"
                          className="ghost"
                          disabled={busy}
                          onClick={() => void withBusy(() => adminResetPassword(user.username))}
                        >
                          Reset password
                        </button>
                        <button
                          type="button"
                          className="ghost danger"
                          disabled={busy || user.current}
                          onClick={() => {
                            if (!confirm(`Delete user "${user.username}"? This cannot be undone.`)) return;
                            void withBusy(() => adminDeleteUser(user.username));
                          }}
                        >
                          Delete
                        </button>
                      </>
                    ) : null}
                  </div>
                </article>
              );
            })}
          </div>
        </section>

        <section className="admin-section">
          <div className="admin-section-head">
            <div>
              <h3>Online users ({sessions.length})</h3>
              <p className="admin-meta">One row per signed-in user, even if the browser has multiple sessions.</p>
            </div>
          </div>
          {sessions.length ? (
            <div className="admin-user-list">
              {sessions.map((session) => (
                <article key={`${session.username}-${session.role}`} className="admin-user-card compact">
                  <div>
                    <strong>{session.username}</strong>
                    <p className="admin-meta">{session.role}</p>
                  </div>
                  <div className="admin-session-times">
                    <span>Since {formatDate(session.createdAt)}</span>
                    <span>Last active {formatDate(session.lastSeenAt)}</span>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <p className="admin-meta">No active sessions.</p>
          )}
        </section>
      </div>
    </div>
  );
}
