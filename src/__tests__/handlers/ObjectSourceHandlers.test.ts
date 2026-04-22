import { ObjectSourceHandlers } from '../../handlers/ObjectSourceHandlers';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import type { ADTClient } from 'abap-adt-api';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMockClient(overrides: Partial<ADTClient> = {}): ADTClient {
  return {
    getObjectSource: jest.fn(),
    setObjectSource: jest.fn(),
    ...overrides,
  } as unknown as ADTClient;
}

// ---------------------------------------------------------------------------
// Tool definitions – regression guard
// ---------------------------------------------------------------------------

describe('ObjectSourceHandlers – tool definitions', () => {
  const handler = new ObjectSourceHandlers(makeMockClient());
  const tools = handler.getTools();

  it('exports exactly two tools', () => {
    expect(tools).toHaveLength(2);
  });

  it('exports getObjectSource with correct shape', () => {
    const tool = tools.find(t => t.name === 'getObjectSource');
    expect(tool).toBeDefined();
    expect(tool!.inputSchema.required).toContain('objectSourceUrl');
  });

  it('exports setObjectSource with correct shape', () => {
    const tool = tools.find(t => t.name === 'setObjectSource');
    expect(tool).toBeDefined();
    expect(tool!.inputSchema.required).toContain('objectSourceUrl');
    expect(tool!.inputSchema.required).toContain('source');
    expect(tool!.inputSchema.required).toContain('lockHandle');
  });
});

// ---------------------------------------------------------------------------
// handle() routing
// ---------------------------------------------------------------------------

describe('ObjectSourceHandlers – handle() routing', () => {
  it('throws MethodNotFound for an unknown tool name', async () => {
    const handler = new ObjectSourceHandlers(makeMockClient());
    await expect(handler.handle('unknownTool', {})).rejects.toMatchObject({
      code: ErrorCode.MethodNotFound,
    });
  });
});

// ---------------------------------------------------------------------------
// getObjectSource – CURRENT (non-chunked) behavior
// ---------------------------------------------------------------------------

describe('getObjectSource – baseline behavior', () => {
  it('returns status:success and the full source string', async () => {
    const source = 'REPORT z_hello.\nWRITE "Hello".';
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(source) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/sap/bc/adt/programs/programs/z_hello/source/main',
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');
    expect(parsed.source).toBe(source);
  });

  it('passes objectSourceUrl and options through to ADTClient', async () => {
    const mockGet = jest.fn().mockResolvedValue('');
    const client = makeMockClient({ getObjectSource: mockGet });
    const handler = new ObjectSourceHandlers(client);
    const url = '/sap/bc/adt/programs/programs/z_test/source/main';
    const options = { version: 'active' };

    await handler.handle('getObjectSource', { objectSourceUrl: url, options });

    expect(mockGet).toHaveBeenCalledWith(url, options);
  });

  it('returns content array with a single text block', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue('X') });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
    });

    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe('text');
  });

  it('throws McpError(InternalError) when ADTClient rejects', async () => {
    const client = makeMockClient({
      getObjectSource: jest.fn().mockRejectedValue(new Error('connection refused')),
    });
    const handler = new ObjectSourceHandlers(client);

    await expect(
      handler.handle('getObjectSource', { objectSourceUrl: '/any/url' })
    ).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('connection refused'),
    });
  });

  it('handles errors without a message property gracefully', async () => {
    const client = makeMockClient({
      getObjectSource: jest.fn().mockRejectedValue({ response: { data: { message: 'SAP error' } } }),
    });
    const handler = new ObjectSourceHandlers(client);

    await expect(
      handler.handle('getObjectSource', { objectSourceUrl: '/any/url' })
    ).rejects.toMatchObject({
      code: ErrorCode.InternalError,
    });
  });
});

// ---------------------------------------------------------------------------
// setObjectSource – CURRENT behavior
// ---------------------------------------------------------------------------

describe('setObjectSource – baseline behavior', () => {
  it('returns status:success and updated:true on success', async () => {
    const client = makeMockClient({ setObjectSource: jest.fn().mockResolvedValue(undefined) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('setObjectSource', {
      objectSourceUrl: '/sap/bc/adt/programs/programs/z_hello/source/main',
      source: 'REPORT z_hello.',
      lockHandle: 'lock-abc-123',
      transport: 'DEVK900001',
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');
    expect(parsed.updated).toBe(true);
  });

  it('passes all four arguments to ADTClient in the correct order', async () => {
    const mockSet = jest.fn().mockResolvedValue(undefined);
    const client = makeMockClient({ setObjectSource: mockSet });
    const handler = new ObjectSourceHandlers(client);

    const url = '/sap/bc/adt/programs/programs/z_test/source/main';
    const source = 'REPORT z_test.';
    const lockHandle = 'lock-xyz';
    const transport = 'DEVK900002';

    await handler.handle('setObjectSource', { objectSourceUrl: url, source, lockHandle, transport });

    expect(mockSet).toHaveBeenCalledWith(url, source, lockHandle, transport);
  });

  it('works without optional transport argument', async () => {
    const mockSet = jest.fn().mockResolvedValue(undefined);
    const client = makeMockClient({ setObjectSource: mockSet });
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('setObjectSource', {
      objectSourceUrl: '/any/url',
      source: 'REPORT z_x.',
      lockHandle: 'lock-1',
    });

    expect(mockSet).toHaveBeenCalledWith('/any/url', 'REPORT z_x.', 'lock-1', undefined);
  });

  it('throws McpError(InternalError) when ADTClient rejects', async () => {
    const client = makeMockClient({
      setObjectSource: jest.fn().mockRejectedValue(new Error('lock expired')),
    });
    const handler = new ObjectSourceHandlers(client);

    await expect(
      handler.handle('setObjectSource', {
        objectSourceUrl: '/any/url',
        source: 'X',
        lockHandle: 'lock-1',
      })
    ).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('lock expired'),
    });
  });

  it('returns content array with a single text block', async () => {
    const client = makeMockClient({ setObjectSource: jest.fn().mockResolvedValue(undefined) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('setObjectSource', {
      objectSourceUrl: '/any/url',
      source: 'X',
      lockHandle: 'lock-1',
    });

    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe('text');
  });
});

// ---------------------------------------------------------------------------
// getObjectSource – response stays within 1 MB MCP limit when chunked
// ---------------------------------------------------------------------------

describe('getObjectSource – response size with chunking', () => {
  it('a single chunk of 50 000 chars stays well under 1 MB', async () => {
    const source = 'X'.repeat(500_000);
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(source) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 0,
    });

    const text = result.content[0].text;
    expect(Buffer.byteLength(text, 'utf8')).toBeLessThan(900 * 1024);
  });
});

// ---------------------------------------------------------------------------
// getObjectSource – CHUNKED behavior
// ---------------------------------------------------------------------------

describe('getObjectSource – chunked behavior', () => {
  const FULL_SOURCE = 'A'.repeat(120_000); // 120k chars, simulates a large program

  it('returns only the first chunk when chunkSize is provided', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(FULL_SOURCE) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 0,
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');
    expect(parsed.source).toBe(FULL_SOURCE.slice(0, 50_000));
    expect(parsed.chunked).toBe(true);
    expect(parsed.chunkIndex).toBe(0);
    expect(parsed.totalSize).toBe(120_000);
    expect(parsed.totalChunks).toBe(3);
    expect(parsed.hasMore).toBe(true);
  });

  it('returns a middle chunk correctly', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(FULL_SOURCE) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 1,
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.source).toBe(FULL_SOURCE.slice(50_000, 100_000));
    expect(parsed.chunkIndex).toBe(1);
    expect(parsed.hasMore).toBe(true);
  });

  it('returns the last chunk with hasMore:false', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(FULL_SOURCE) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 2,
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.source).toBe(FULL_SOURCE.slice(100_000));
    expect(parsed.chunkIndex).toBe(2);
    expect(parsed.hasMore).toBe(false);
  });

  it('returns full source with no chunking metadata when chunkSize is omitted', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(FULL_SOURCE) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.source).toBe(FULL_SOURCE);
    expect(parsed.chunked).toBeUndefined();
    expect(parsed.totalChunks).toBeUndefined();
    expect(parsed.hasMore).toBeUndefined();
  });

  it('returns full source in a single chunk when source fits within chunkSize', async () => {
    const smallSource = 'REPORT z_small.';
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(smallSource) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 0,
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.source).toBe(smallSource);
    expect(parsed.chunked).toBe(true);
    expect(parsed.totalChunks).toBe(1);
    expect(parsed.hasMore).toBe(false);
  });

  it('returns empty source for an out-of-bounds chunkIndex', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue(FULL_SOURCE) });
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('getObjectSource', {
      objectSourceUrl: '/any/url',
      chunkSize: 50_000,
      chunkIndex: 99,
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.source).toBe('');
    expect(parsed.hasMore).toBe(false);
  });

  it('only calls ADTClient once per chunked request (slicing is done locally)', async () => {
    const mockGet = jest.fn().mockResolvedValue(FULL_SOURCE);
    const client = makeMockClient({ getObjectSource: mockGet });
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('getObjectSource', { objectSourceUrl: '/any/url', chunkSize: 50_000, chunkIndex: 0 });
    await handler.handle('getObjectSource', { objectSourceUrl: '/any/url', chunkSize: 50_000, chunkIndex: 1 });

    // Two separate tool calls → two ADTClient calls (no server-side caching expected)
    expect(mockGet).toHaveBeenCalledTimes(2);
  });

  it('validates that chunkSize must be a positive number', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue('X') });
    const handler = new ObjectSourceHandlers(client);

    await expect(
      handler.handle('getObjectSource', { objectSourceUrl: '/any/url', chunkSize: 0, chunkIndex: 0 })
    ).rejects.toMatchObject({ code: ErrorCode.InternalError });

    await expect(
      handler.handle('getObjectSource', { objectSourceUrl: '/any/url', chunkSize: -1, chunkIndex: 0 })
    ).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('validates that chunkIndex must be a non-negative integer', async () => {
    const client = makeMockClient({ getObjectSource: jest.fn().mockResolvedValue('X') });
    const handler = new ObjectSourceHandlers(client);

    await expect(
      handler.handle('getObjectSource', { objectSourceUrl: '/any/url', chunkSize: 1000, chunkIndex: -1 })
    ).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('tool schema includes chunkSize and chunkIndex as optional fields', () => {
    const handler = new ObjectSourceHandlers(makeMockClient());
    const tool = handler.getTools().find(t => t.name === 'getObjectSource')!;
    expect(tool.inputSchema.properties).toHaveProperty('chunkSize');
    expect(tool.inputSchema.properties).toHaveProperty('chunkIndex');
    // They must NOT appear in required
    expect(tool.inputSchema.required).not.toContain('chunkSize');
    expect(tool.inputSchema.required).not.toContain('chunkIndex');
  });
});
