let devBackend = false;
let instantLogs = false;
let devtoolLocalDashboard = false;

try {
  if (typeof window !== 'undefined' && window.localStorage) {
    devBackend = !!window.localStorage.getItem('devBackend');
    instantLogs = !!window.localStorage.getItem('__instantLogging');
    devtoolLocalDashboard = !!window.localStorage.getItem('__devtoolLocalDash');
  }
} catch {
  // The browser refuses localStorage, as with site data blocked
}

export { devBackend, instantLogs, devtoolLocalDashboard };
