import { useEffect, useMemo, useState } from "react";
import { fetchUserStatus, login, requestPasswordReset, setPassword } from "./api";
import type { AuthState, UserStatus } from "./types";
import { PASSWORD_MAX, PASSWORD_MIN } from "./types";

type Props = {
  state: AuthState;
  onSignedIn: (next: AuthState) => void;
};

type Mode = "enter-password" | "create-password" | "request-reset-sent";

export function LoginScreen({ state, onSignedIn }: Props) {
  const usernames = useMemo(() => state.availableUsernames ?? [], [state.availableUsernames]);
  const [selectedUsername, setSelectedUsername] = useState(usernames[0] ?? "");
  const [password, setPasswordValue] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [status, setStatus] = useState<UserStatus | null>(null);
  const [mode, setMode] = useState<Mode>("enter-password");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!usernames.length) {
      setSelectedUsername("");
      return;
    }
    if (!usernames.includes(selectedUsername)) {
      setSelectedUsername(usernames[0]);
    }
  }, [usernames, selectedUsername]);

  useEffect(() => {
    let cancelled = false;
    setError("");
    setPasswordValue("");
    setConfirmPassword("");
    if (!selectedUsername) {
      setStatus(null);
      setMode("enter-password");
      return () => {
        cancelled = true;
      };
    }
    void fetchUserStatus(selectedUsername).then((result) => {
      if (cancelled) return;
      setStatus(result);
      setMode(result.requiresPasswordCreation ? "create-password" : "enter-password");
    });
    return () => {
      cancelled = true;
    };
  }, [selectedUsername]);

  function passwordHasValidLength(value: string): boolean {
    return value.length >= PASSWORD_MIN && value.length <= PASSWORD_MAX;
  }

  async function handleSignIn(event: React.FormEvent) {
    event.preventDefault();
    if (!selectedUsername) {
      setError("Pick a username.");
      return;
    }
    if (!passwordHasValidLength(password)) {
      setError(`Password must be ${PASSWORD_MIN} to ${PASSWORD_MAX} characters.`);
      return;
    }
    setBusy(true);
    setError("");
    try {
      const result = await login(selectedUsername, password);
      if (result.ok && result.authenticated) {
        onSignedIn(result);
        return;
      }
      if (result.requiresPasswordCreation) {
        setMode("create-password");
        setStatus({ exists: true, requiresPasswordCreation: true, requiresPasswordReset: true });
        setError(result.error ?? "Set a new password to continue.");
        return;
      }
      setError(result.error ?? "Sign in failed.");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Sign in failed.");
    } finally {
      setBusy(false);
    }
  }

  async function handleCreatePassword(event: React.FormEvent) {
    event.preventDefault();
    if (!selectedUsername) {
      setError("Pick a username.");
      return;
    }
    if (!passwordHasValidLength(password)) {
      setError(`Password must be ${PASSWORD_MIN} to ${PASSWORD_MAX} characters.`);
      return;
    }
    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const result = await setPassword(selectedUsername, password);
      if (result.ok && result.authenticated) {
        onSignedIn(result);
        return;
      }
      setError(result.error ?? "Could not create password.");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Could not create password.");
    } finally {
      setBusy(false);
    }
  }

  async function handleRequestReset() {
    if (!selectedUsername) {
      setError("Pick a username first.");
      return;
    }
    if (selectedUsername === state.adminUsername) {
      setError("Administrator cannot request a reset.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const result = await requestPasswordReset(selectedUsername);
      if (result.ok) {
        setMode("request-reset-sent");
      } else {
        setError(result.error ?? "Could not send reset request.");
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Could not send reset request.");
    } finally {
      setBusy(false);
    }
  }

  if (!usernames.length) {
    return (
      <div className="auth-shell">
        <div className="auth-modal">
          <h2>No accounts yet</h2>
          <p>The administrator has not created any user accounts. Sign in as the administrator first to add users.</p>
          <p className="auth-hint">Administrator account: <code>{state.adminUsername}</code></p>
        </div>
      </div>
    );
  }

  return (
    <div className="auth-shell">
      <div className="auth-modal">
        <h2>Log Analyzer</h2>
        <p>Sign in to continue.</p>
        <form className="auth-form" onSubmit={mode === "create-password" ? handleCreatePassword : handleSignIn}>
          <label className="auth-field">
            <span>Username</span>
            <select
              value={selectedUsername}
              onChange={(event) => setSelectedUsername(event.target.value)}
              disabled={busy}
            >
              {usernames.map((name) => (
                <option key={name} value={name}>{name}</option>
              ))}
            </select>
          </label>

          {mode === "create-password" ? (
            <>
              <p className="auth-hint">
                {status?.requiresPasswordReset
                  ? "Your password was reset. Choose a new one to continue."
                  : "First time signing in. Choose a password to continue."}
              </p>
              <label className="auth-field">
                <span>New password ({PASSWORD_MIN}-{PASSWORD_MAX} characters)</span>
                <input
                  type="password"
                  value={password}
                  minLength={PASSWORD_MIN}
                  maxLength={PASSWORD_MAX}
                  autoFocus
                  onChange={(event) => setPasswordValue(event.target.value)}
                  disabled={busy}
                />
              </label>
              <label className="auth-field">
                <span>Confirm password</span>
                <input
                  type="password"
                  value={confirmPassword}
                  minLength={PASSWORD_MIN}
                  maxLength={PASSWORD_MAX}
                  onChange={(event) => setConfirmPassword(event.target.value)}
                  disabled={busy}
                />
              </label>
            </>
          ) : mode === "request-reset-sent" ? (
            <p className="auth-hint">
              Reset request sent to the administrator. They&apos;ll reset your password, then pick your username again to set a new one.
            </p>
          ) : (
            <label className="auth-field">
              <span>Password ({PASSWORD_MIN}-{PASSWORD_MAX} characters)</span>
              <input
                type="password"
                value={password}
                minLength={PASSWORD_MIN}
                maxLength={PASSWORD_MAX}
                autoFocus
                onChange={(event) => setPasswordValue(event.target.value)}
                disabled={busy}
              />
            </label>
          )}

          {error ? <div className="auth-error" role="alert">{error}</div> : null}

          <div className="auth-actions">
            {mode === "request-reset-sent" ? (
              <button type="button" className="primary" onClick={() => setMode("enter-password")} disabled={busy}>
                Back to sign in
              </button>
            ) : (
              <>
                <button type="submit" className="primary" disabled={busy || !selectedUsername}>
                  {busy ? "Working..." : mode === "create-password" ? "Create password & sign in" : "Sign in"}
                </button>
                {mode !== "create-password" && selectedUsername !== state.adminUsername ? (
                  <button type="button" className="ghost" onClick={handleRequestReset} disabled={busy}>
                    Forgot password?
                  </button>
                ) : null}
              </>
            )}
          </div>
        </form>
      </div>
    </div>
  );
}
