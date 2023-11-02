export function createLogger(processId: string) {
  return {
    log: (message: string, label?: string) => console.log(`(${processId}${label && '-' + label || ''}) ${message}`),
    error: (message: string, err: Error, label?: string) => console.error(`(${processId}${label && '-' + label || ''}) ${message}`, err),
  }
};

export function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
