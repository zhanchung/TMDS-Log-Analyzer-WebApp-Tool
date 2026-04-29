import { useState, type FormEvent } from "react";
import { changePassword, renameSelf } from "./api";
import type { AuthState } from "./types";
import { PASSWORD_MAX, PASSWORD_MIN } from "./types";

type Props = {
  state: AuthState;
  onClose: () => void;
  onUpdated: (next: AuthState) => void;
};

function passwordHasValidLength(value: string): boolean {
  return value.length >= PASSWORD_MIN && value.length <= PASSWORD_MAX;
}

function getPasswordChangeErrorMessage(error: string | undefined): string {
  if (error === "Unknown API endpoint.") {
    return "TMDS server needs to be stopped and started again before password changes can be used.";
  }
  return error ?? "Password change failed.";
}

export function AccountPanel({ state, onClose, onUpdated }: Props) {
  const [draftName, setDraftName] = useState(state.username ?? "");
  const [renameError, setRenameError] = useState("");
  const [passwordError, setPasswordError] = useState("");
  const [passwordSuccess, setPasswordSuccess] = useState("");
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmNewPassword, setConfirmNewPassword] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSaveName(event: FormEvent) {
    event.preventDefault();
    const trimmed = draftName.trim();
    if (!trimmed) {
      setRenameError("Enter a username.");
      return;
    }
    if (trimmed === state.username) {
      onClose();
      return;
    }
    setBusy(true);
    setRenameError("");
    try {
      const result = await renameSelf(trimmed);
      if (result.ok && result.authenticated) {
        onUpdated(result);
        onClose();
        return;
      }
      setRenameError(result.error ?? "Rename failed.");
    } catch (caughtError) {
      setRenameError(caughtError instanceof Error ? caughtError.message : "Rename failed.");
    } finally {
      setBusy(false);
    }
  }

  async function handleChangePassword(event: FormEvent) {
    event.preventDefault();
    setPasswordError("");
    setPasswordSuccess("");
    if (!passwordHasValidLength(currentPassword) || !passwordHasValidLength(newPassword)) {
      setPasswordError(`Password must be ${PASSWORD_MIN} to ${PASSWORD_MAX} characters.`);
      return;
    }
    if (newPassword !== confirmNewPassword) {
      setPasswordError("New passwords do not match.");
      return;
    }
    setBusy(true);
    try {
      const result = await changePassword(currentPassword, newPassword);
      if (result.ok && result.authenticated) {
        onUpdated(result);
        setCurrentPassword("");
        setNewPassword("");
        setConfirmNewPassword("");
        setPasswordSuccess("Password updated.");
        return;
      }
      setPasswordError(getPasswordChangeErrorMessage(result.error));
    } catch (caughtError) {
      setPasswordError(caughtError instanceof Error ? caughtError.message : "Password change failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="admin-panel-backdrop admin-console-backdrop" role="dialog" aria-modal="true">
      <div className="admin-panel account-console">
        <header className="admin-console-hero">
          <div>
            <p className="admin-kicker">Account</p>
            <h2>Profile settings</h2>
            <p>Signed in as <strong>{state.username}</strong>. Update your display name or change your password.</p>
          </div>
          <button type="button" className="ghost" onClick={onClose}>Close</button>
        </header>

        <section className="admin-section account-profile-card">
          <div>
            <h3>Rename account</h3>
            <p className="admin-meta">Use letters, numbers, spaces, dot, underscore, or dash. Spaces must be between name parts.</p>
          </div>
          <form className="auth-form account-form" onSubmit={handleSaveName}>
            <label className="auth-field">
              <span>Username</span>
              <input
                type="text"
                value={draftName}
                onChange={(event) => setDraftName(event.target.value)}
                disabled={busy}
                maxLength={48}
                autoFocus
              />
            </label>
            {renameError ? <div className="auth-error" role="alert">{renameError}</div> : null}
            <div className="auth-actions">
              <button type="submit" className="primary" disabled={busy}>
                {busy ? "Saving..." : "Save name"}
              </button>
              <button type="button" className="ghost" onClick={onClose} disabled={busy}>Cancel</button>
            </div>
          </form>
        </section>

        <section className="admin-section account-profile-card">
          <div>
            <h3>Change password</h3>
            <p className="admin-meta">Enter your current password first. This keeps password changes self-service without letting another signed-in browser overwrite it.</p>
          </div>
          <form className="auth-form account-form" onSubmit={handleChangePassword}>
            <label className="auth-field">
              <span>Current password</span>
              <input
                type="password"
                value={currentPassword}
                minLength={PASSWORD_MIN}
                maxLength={PASSWORD_MAX}
                onChange={(event) => setCurrentPassword(event.target.value)}
                disabled={busy}
                autoComplete="current-password"
              />
            </label>
            <div className="account-password-grid">
              <label className="auth-field">
                <span>New password ({PASSWORD_MIN}-{PASSWORD_MAX} characters)</span>
                <input
                  type="password"
                  value={newPassword}
                  minLength={PASSWORD_MIN}
                  maxLength={PASSWORD_MAX}
                  onChange={(event) => setNewPassword(event.target.value)}
                  disabled={busy}
                  autoComplete="new-password"
                />
              </label>
              <label className="auth-field">
                <span>Confirm new password</span>
                <input
                  type="password"
                  value={confirmNewPassword}
                  minLength={PASSWORD_MIN}
                  maxLength={PASSWORD_MAX}
                  onChange={(event) => setConfirmNewPassword(event.target.value)}
                  disabled={busy}
                  autoComplete="new-password"
                />
              </label>
            </div>
            {passwordError ? <div className="auth-error" role="alert">{passwordError}</div> : null}
            {passwordSuccess ? <div className="auth-success" role="status">{passwordSuccess}</div> : null}
            <div className="auth-actions">
              <button type="submit" className="primary" disabled={busy}>
                {busy ? "Saving..." : "Update password"}
              </button>
            </div>
          </form>
        </section>
      </div>
    </div>
  );
}
