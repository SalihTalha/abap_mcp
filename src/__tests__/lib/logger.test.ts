import { createLogger } from '../../lib/logger';

describe('logger – stderr-only output', () => {
  let stderrSpy: jest.SpyInstance;
  let stdoutSpy: jest.SpyInstance;

  beforeEach(() => {
    stderrSpy = jest.spyOn(process.stderr, 'write').mockImplementation(() => true);
    stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const levels = ['error', 'warn', 'info', 'debug'] as const;

  levels.forEach(level => {
    it(`${level}() writes to stderr, never to stdout`, () => {
      const logger = createLogger('TestService');
      logger[level]('test message', { extra: 'data' });

      expect(stderrSpy).toHaveBeenCalledTimes(1);
      expect(stdoutSpy).not.toHaveBeenCalled();
    });

    it(`${level}() output is valid JSON`, () => {
      const logger = createLogger('TestService');
      logger[level]('hello world');

      const written = stderrSpy.mock.calls[0][0] as string;
      expect(() => JSON.parse(written)).not.toThrow();
    });

    it(`${level}() JSON contains expected fields`, () => {
      const logger = createLogger('MySvc');
      logger[level]('my message', { key: 'value' });

      const entry = JSON.parse(stderrSpy.mock.calls[0][0] as string);
      expect(entry.level).toBe(level);
      expect(entry.service).toBe('MySvc');
      expect(entry.message).toBe('my message');
      expect(entry.timestamp).toBeDefined();
    });
  });
});
