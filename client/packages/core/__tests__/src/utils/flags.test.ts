import { afterEach, expect, test, vi } from 'vitest';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
});

function loadFlags(window: object) {
  vi.stubGlobal('window', window);
  vi.resetModules();
  return import('../../../src/utils/flags');
}

const off = {
  devBackend: false,
  instantLogs: false,
  devtoolLocalDashboard: false,
};

test('flags are read from localStorage', async () => {
  const stored = { devBackend: '1', __instantLogging: '1' };
  const flags = await loadFlags({
    localStorage: { getItem: (k: string) => stored[k] ?? null },
  });
  expect({ ...flags }).toEqual({
    ...off,
    devBackend: true,
    instantLogs: true,
  });
});

test('flags are off when the browser refuses localStorage', async () => {
  const flags = await loadFlags({
    get localStorage() {
      throw new DOMException('denied', 'SecurityError');
    },
  });
  expect({ ...flags }).toEqual(off);
});

test.each([undefined, null])(
  'flags are off when localStorage is %s',
  async (localStorage) => {
    const flags = await loadFlags({ localStorage });
    expect({ ...flags }).toEqual(off);
  },
);
