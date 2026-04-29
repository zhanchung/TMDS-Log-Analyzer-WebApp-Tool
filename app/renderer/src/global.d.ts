import type { NativeApi } from "../../shared/native-api";

declare global {
  interface Window {
    tmds: NativeApi;
  }
  const __TMDS_BUILD_VERSION__: string;
  const __TMDS_BUILD_AT__: string;
}

export {};
