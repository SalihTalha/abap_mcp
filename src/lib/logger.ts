type LogLevel = 'error' | 'warn' | 'info' | 'debug';

export function createLogger(name: string) {
  return {
    error: (message: string, meta?: Record<string, unknown>) => 
      log('error', name, message, meta),
    warn: (message: string, meta?: Record<string, unknown>) => 
      log('warn', name, message, meta),
    info: (message: string, meta?: Record<string, unknown>) => 
      log('info', name, message, meta),
    debug: (message: string, meta?: Record<string, unknown>) => 
      log('debug', name, message, meta)
  };
}

function log(level: LogLevel, name: string, message: string, meta?: Record<string, unknown>) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    service: name,
    message,
    ...meta
  };

  // ALL levels write to stderr.
  // The MCP server communicates over stdout (StdioServerTransport); writing
  // anything to stdout corrupts the JSON-RPC stream and causes parse errors
  // in the client.
  process.stderr.write(JSON.stringify(logEntry) + '\n');
}

export type Logger = ReturnType<typeof createLogger>;
