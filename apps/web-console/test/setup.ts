import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach } from "vitest";

afterEach(() => {
  cleanup();
});

afterAll(() => {
  if (!process.env.DEBUG_VITEST_HANDLES) {
    return;
  }

  const handles = (process as typeof process & {
    _getActiveHandles?: () => unknown[];
  })._getActiveHandles?.() ?? [];
  console.error(
    "[web-console][vitest] active handles:",
    handles.map((handle) => {
      const candidate = handle as
        | { constructor?: { name?: string }; hasRef?: () => boolean }
        | undefined;
      return {
        type: candidate?.constructor?.name ?? typeof handle,
        hasRef:
          typeof candidate?.hasRef === "function" ? candidate.hasRef() : undefined,
      };
    }),
  );
});
