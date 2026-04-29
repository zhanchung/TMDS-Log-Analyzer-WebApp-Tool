#!/usr/bin/env node
// Generates a new salt + scrypt hash for the built-in administrator password.
// Paste the printed values into app/main/main.ts (BUILTIN_ADMIN_SALT_HEX
// and BUILTIN_ADMIN_PASSWORD_HASH_HEX) and rebuild with `npm run build`.

import { randomBytes, scryptSync } from "node:crypto";
import readline from "node:readline";

function prompt(question, hidden = false) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    if (hidden) {
      const stdin = process.stdin;
      const onData = (char) => {
        char = char.toString();
        if (char === "\n" || char === "\r" || char === "") {
          stdin.removeListener("data", onData);
        } else {
          process.stdout.clearLine(0);
          process.stdout.cursorTo(0);
          process.stdout.write(question + "*".repeat(rl.line.length));
        }
      };
      stdin.on("data", onData);
    }
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

async function main() {
  console.log("");
  console.log("TMDS administrator password reset helper");
  console.log("This prints new salt + hash values for the BUILTIN admin (jchung).");
  console.log("Paste them into app/main/main.ts, then rebuild and restart the server.");
  console.log("");

  const password = await prompt("New admin password (4-16 chars): ", true);
  if (password.length < 4 || password.length > 16) {
    console.error("Password length must be 4-16 characters.");
    process.exit(1);
  }
  const confirm = await prompt("Confirm password: ", true);
  if (password !== confirm) {
    console.error("Passwords did not match.");
    process.exit(1);
  }

  const saltHex = randomBytes(16).toString("hex");
  const hashHex = scryptSync(password, Buffer.from(saltHex, "hex"), 64).toString("hex");

  console.log("");
  console.log("Replace these two constants in app/main/main.ts:");
  console.log("");
  console.log(`const BUILTIN_ADMIN_SALT_HEX = "${saltHex}";`);
  console.log(`const BUILTIN_ADMIN_PASSWORD_HASH_HEX = "${hashHex}";`);
  console.log("");
  console.log("Then run: npm run build");
  console.log("Then run TMDS-Server-Switch.bat twice (off, on) so the server picks up the new password.");
  console.log("");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
