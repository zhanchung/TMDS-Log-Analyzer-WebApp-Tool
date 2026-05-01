export type DecodedGenisysSocketFrame = {
  headerCode: number | null;
  headerLabel: string;
  protocolDirection: "request" | "response" | "unknown";
  serverAddress: number | null;
  crcHex: string | null;
  payloadPairs: Array<{ address: number; data: number }>;
  rawPayloadBytes: number[];
  issues: string[];
};

export const genisysHeaderLabels = new Map<number, string>([
  [0xf1, "Acknowledge / No Data"],
  [0xf2, "Indication Data"],
  [0xf3, "Control Checkback"],
  [0xf9, "Common Controls"],
  [0xfa, "Acknowledge"],
  [0xfb, "Poll"],
  [0xfc, "Control"],
  [0xfd, "Recall"],
  [0xfe, "Execute"],
]);

export function parseSocketHexByte(value: string): number | null {
  const normalized = String(value ?? "").trim().replace(/^0x/i, "");
  if (!/^[0-9a-f]{2}$/i.test(normalized)) {
    return null;
  }
  return Number.parseInt(normalized, 16);
}

export function formatHexByte(value: number): string {
  return value.toString(16).toUpperCase().padStart(2, "0");
}

export function formatHexWord(value: number): string {
  return value.toString(16).toUpperCase().padStart(4, "0");
}

export function formatByteBinary(value: number): string {
  return value.toString(2).padStart(8, "0");
}

export function describeGenericHexByteRows(payloadBytes: string[], contextLabel: string): string[] {
  if (!payloadBytes.length) {
    return [`No ${contextLabel} bytes captured.`];
  }
  return payloadBytes.map((value, index) => {
    const parsed = parseSocketHexByte(value);
    if (parsed === null) {
      return `${index + 1}. offset ${index} - ${value} - invalid hex octet`;
    }
    const ascii = parsed >= 0x20 && parsed <= 0x7e ? `, ASCII '${String.fromCharCode(parsed)}'` : "";
    return `${index + 1}. offset ${index} - 0x${formatHexByte(parsed)} - decimal ${parsed}, binary ${formatByteBinary(parsed)}${ascii}`;
  });
}

export function extractBracketedHexBytes(raw: string): string[] {
  const bytes: string[] = [];
  const pattern = /<\s*([A-F0-9]{2})\s*>/gi;
  let match = pattern.exec(raw);
  while (match) {
    bytes.push(match[1].toUpperCase());
    match = pattern.exec(raw);
  }
  return bytes;
}

export function getFirstGenisysByteFromRaw(raw: string): string {
  const bracketed = extractBracketedHexBytes(raw)[0];
  if (bracketed) return bracketed;
  const afterData = /\b(?:DATA|XMT|RCV):\s*(?:[A-Z0-9_]+,\s*)?([A-F0-9]{2})\b/i.exec(raw)?.[1];
  if (afterData) return afterData.toUpperCase();
  const standalone = /\b(F[0-9A-F])\b/i.exec(raw)?.[1];
  return standalone ? standalone.toUpperCase() : "";
}

export function decodeGenisysSocketFrame(payloadBytes: string[]): DecodedGenisysSocketFrame {
  const numericBytes = payloadBytes.map(parseSocketHexByte);
  const issues: string[] = [];
  if (numericBytes.some((value) => value === null)) {
    issues.push("one or more socket-frame bytes were not valid hex octets");
  }
  const bytes = numericBytes.filter((value): value is number => value !== null);
  if (!bytes.length) {
    return {
      headerCode: null,
      headerLabel: "Unknown",
      protocolDirection: "unknown",
      serverAddress: null,
      crcHex: null,
      payloadPairs: [],
      rawPayloadBytes: [],
      issues: issues.length ? issues : ["no socket-frame bytes were parsed"],
    };
  }

  const headerCode = bytes[0] ?? null;
  const headerLabel = headerCode !== null ? (genisysHeaderLabels.get(headerCode) ?? `Unknown header 0x${formatHexByte(headerCode)}`) : "Unknown";
  const protocolDirection =
    headerCode !== null && headerCode >= 0xf1 && headerCode <= 0xf3
      ? "response"
      : headerCode !== null && headerCode >= 0xf9 && headerCode <= 0xfe
        ? "request"
        : "unknown";

  // F9 Common Controls is a broadcast single-byte frame — no station address, no CRC, no F6 terminator.
  if (headerCode === 0xf9) {
    return { headerCode, headerLabel, protocolDirection, serverAddress: null, crcHex: null, payloadPairs: [], rawPayloadBytes: [], issues };
  }

  const terminatorIndex = bytes.lastIndexOf(0xf6);
  if (terminatorIndex === -1) {
    issues.push("frame terminator 0xF6 was not present");
  }
  const bodyBytes = bytes.slice(1, terminatorIndex === -1 ? undefined : terminatorIndex);
  const unescapedBody: number[] = [];
  for (let index = 0; index < bodyBytes.length; index += 1) {
    const value = bodyBytes[index];
    if (value === 0xf0) {
      const escapedNibble = bodyBytes[index + 1];
      if (escapedNibble === undefined) {
        issues.push("frame ended with escape byte 0xF0 and no escaped value");
        break;
      }
      unescapedBody.push(0xf0 | escapedNibble);
      index += 1;
      continue;
    }
    unescapedBody.push(value);
  }

  const serverAddress = unescapedBody[0] ?? null;
  const hasCrc = headerCode !== 0xf1 && !(headerCode === 0xfb && unescapedBody.length < 2);
  let payloadBytesOnly = serverAddress === null ? [] : unescapedBody.slice(1);
  let crcHex: string | null = null;
  if (hasCrc) {
    if (payloadBytesOnly.length < 2) {
      issues.push("frame did not contain enough bytes for a Genisys CRC");
      payloadBytesOnly = [];
    } else {
      const crcLow = payloadBytesOnly[payloadBytesOnly.length - 2];
      const crcHigh = payloadBytesOnly[payloadBytesOnly.length - 1];
      crcHex = `0x${formatHexWord((crcHigh << 8) | crcLow)}`;
      payloadBytesOnly = payloadBytesOnly.slice(0, -2);
    }
  }

  const payloadPairs: Array<{ address: number; data: number }> = [];
  for (let index = 0; index + 1 < payloadBytesOnly.length; index += 2) {
    payloadPairs.push({ address: payloadBytesOnly[index], data: payloadBytesOnly[index + 1] });
  }
  if (payloadBytesOnly.length % 2 === 1) {
    issues.push("frame payload ended with an unmatched address/data byte");
  }

  return {
    headerCode,
    headerLabel,
    protocolDirection,
    serverAddress,
    crcHex,
    payloadPairs,
    rawPayloadBytes: payloadBytesOnly,
    issues,
  };
}
