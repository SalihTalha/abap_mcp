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

  it('exports exactly three tools', () => {
    expect(tools).toHaveLength(3);
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

// ---------------------------------------------------------------------------
// Shared fixture for patch tests
// ---------------------------------------------------------------------------

const PATCH_SOURCE = [
  'REPORT z_demo.',                          // line 1
  '',                                        // line 2
  'DATA: lv_rate TYPE p DECIMALS 2.',        // line 3
  'DATA: lv_amount TYPE p DECIMALS 2.',      // line 4
  'DATA: lv_result TYPE p DECIMALS 2.',      // line 5
  '',                                        // line 6
  'START-OF-SELECTION.',                     // line 7
  '  lv_rate   = \'0.19\'.',                 // line 8
  '  lv_amount = 1000.',                     // line 9
  '  lv_result = lv_amount * lv_rate.',      // line 10
  '  WRITE lv_result.',                      // line 11
].join('\n');

function makePatchClient(source = PATCH_SOURCE) {
  return makeMockClient({
    getObjectSource: jest.fn().mockResolvedValue(source),
    setObjectSource: jest.fn().mockResolvedValue(undefined),
  });
}

// ---------------------------------------------------------------------------
// patchObjectSource – tool definition
// ---------------------------------------------------------------------------

describe('patchObjectSource – tool definition', () => {
  it('is exported as a third tool', () => {
    const handler = new ObjectSourceHandlers(makeMockClient());
    const tools = handler.getTools();
    expect(tools).toHaveLength(3);
    const tool = tools.find(t => t.name === 'patchObjectSource');
    expect(tool).toBeDefined();
  });

  it('requires objectSourceUrl and lockHandle', () => {
    const handler = new ObjectSourceHandlers(makeMockClient());
    const tool = handler.getTools().find(t => t.name === 'patchObjectSource')!;
    expect(tool.inputSchema.required).toContain('objectSourceUrl');
    expect(tool.inputSchema.required).toContain('lockHandle');
  });

  it('transport is optional', () => {
    const handler = new ObjectSourceHandlers(makeMockClient());
    const tool = handler.getTools().find(t => t.name === 'patchObjectSource')!;
    expect(tool.inputSchema.required).not.toContain('transport');
  });
});

// ---------------------------------------------------------------------------
// patchObjectSource – lineChanges (patch type 1)
// ---------------------------------------------------------------------------

describe('patchObjectSource – lineChanges', () => {
  it('replaces a single line by 1-based line number', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 8, newContent: "  lv_rate   = '0.21'." }],
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');
    expect(parsed.linesChanged).toBe(1);

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[7]).toBe("  lv_rate   = '0.21'.");   // 0-indexed → line 8
  });

  it('applies multiple lineChanges in a single write', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [
        { lineNumber: 8,  newContent: "  lv_rate   = '0.21'." },
        { lineNumber: 9,  newContent: '  lv_amount = 2000.' },
      ],
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[7]).toBe("  lv_rate   = '0.21'.");
    expect(lines[8]).toBe('  lv_amount = 2000.');
    // setObjectSource called exactly once — not once per change
    expect(client.setObjectSource).toHaveBeenCalledTimes(1);
  });

  it('preserves all unchanged lines', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 8, newContent: "  lv_rate = '0.21'." }],
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[0]).toBe('REPORT z_demo.');
    expect(lines[10]).toBe('  WRITE lv_result.');
  });

  it('throws when lineNumber is out of range', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 999, newContent: 'X' }],
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('throws when lineNumber is less than 1', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 0, newContent: 'X' }],
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('passes lockHandle and transport to setObjectSource', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-abc',
      transport: 'DEVK900001',
      lineChanges: [{ lineNumber: 1, newContent: 'REPORT z_new.' }],
    });

    expect(client.setObjectSource).toHaveBeenCalledWith(
      '/any/url',
      expect.any(String),
      'lock-abc',
      'DEVK900001',
    );
  });
});

// ---------------------------------------------------------------------------
// patchObjectSource – searchReplace (patch type 2)
// ---------------------------------------------------------------------------

describe('patchObjectSource – searchReplace', () => {
  it('replaces the first occurrence of a search string', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [{ search: "'0.19'", replacement: "'0.21'" }],
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(written).toContain("'0.21'");
    expect(written).not.toContain("'0.19'");
  });

  it('replaces all occurrences when replaceAll is true', async () => {
    const source = 'A = 1.\nB = 1.\nC = 1.';
    const client = makePatchClient(source);
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [{ search: '1', replacement: '2', replaceAll: true }],
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(written).toBe('A = 2.\nB = 2.\nC = 2.');
  });

  it('replaces only the first occurrence by default (replaceAll omitted)', async () => {
    const source = 'A = 1.\nB = 1.\nC = 1.';
    const client = makePatchClient(source);
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [{ search: '1', replacement: '9' }],
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(written).toBe('A = 9.\nB = 1.\nC = 1.');
  });

  it('applies multiple search-replace operations in order', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [
        { search: "'0.19'", replacement: "'0.21'" },
        { search: '1000', replacement: '2000' },
      ],
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(written).toContain("'0.21'");
    expect(written).toContain('2000');
    expect(client.setObjectSource).toHaveBeenCalledTimes(1);
  });

  it('throws with a clear message when search string is not found', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [{ search: 'THIS_DOES_NOT_EXIST', replacement: 'X' }],
    })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('THIS_DOES_NOT_EXIST'),
    });
  });

  it('does not call setObjectSource when search is not found', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      searchReplace: [{ search: 'NOT_THERE', replacement: 'X' }],
    })).rejects.toThrow();

    expect(client.setObjectSource).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// patchObjectSource – rangeChange (patch type 3)
// ---------------------------------------------------------------------------

describe('patchObjectSource – rangeChange', () => {
  it('replaces a range of lines with new content', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: {
        startLine: 8,
        endLine: 10,
        newContent: "  lv_rate   = '0.21'.\n  lv_amount = 2000.\n  lv_result = lv_amount * lv_rate.",
      },
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.status).toBe('success');

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[7]).toBe("  lv_rate   = '0.21'.");
    expect(lines[8]).toBe('  lv_amount = 2000.');
    expect(lines[9]).toBe('  lv_result = lv_amount * lv_rate.');
  });

  it('preserves lines before the range', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 8, endLine: 10, newContent: 'REPLACED' },
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[0]).toBe('REPORT z_demo.');   // line 1 untouched
    expect(lines[6]).toBe('START-OF-SELECTION.');  // line 7 untouched
  });

  it('preserves lines after the range', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 8, endLine: 10, newContent: 'REPLACED' },
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    const lines = written.split('\n');
    expect(lines[lines.length - 1]).toBe('  WRITE lv_result.');  // line 11 untouched
  });

  it('can replace a single line via range (startLine === endLine)', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 1, endLine: 1, newContent: 'REPORT z_replaced.' },
    });

    const written = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(written.split('\n')[0]).toBe('REPORT z_replaced.');
  });

  it('throws when startLine > endLine', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 10, endLine: 5, newContent: 'X' },
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('throws when startLine is less than 1', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 0, endLine: 5, newContent: 'X' },
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('throws when endLine exceeds total lines', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      rangeChange: { startLine: 1, endLine: 9999, newContent: 'X' },
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });
});

// ---------------------------------------------------------------------------
// patchObjectSource – mutual exclusion and validation
// ---------------------------------------------------------------------------

describe('patchObjectSource – input validation', () => {
  it('throws when no patch type is provided', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('throws when more than one patch type is provided', async () => {
    const handler = new ObjectSourceHandlers(makePatchClient());

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges:  [{ lineNumber: 1, newContent: 'X' }],
      searchReplace: [{ search: 'A', replacement: 'B' }],
    })).rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('returns totalLines in the success response', async () => {
    const client = makePatchClient();
    const handler = new ObjectSourceHandlers(client);

    const result = await handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 1, newContent: 'REPORT z_x.' }],
    });

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.totalLines).toBe(11);
  });

  it('propagates ADTClient read errors as McpError(InternalError)', async () => {
    const client = makeMockClient({
      getObjectSource: jest.fn().mockRejectedValue(new Error('read failed')),
      setObjectSource: jest.fn(),
    });
    const handler = new ObjectSourceHandlers(client);

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 1, newContent: 'X' }],
    })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('read failed'),
    });
  });

  it('propagates ADTClient write errors as McpError(InternalError)', async () => {
    const client = makeMockClient({
      getObjectSource: jest.fn().mockResolvedValue(PATCH_SOURCE),
      setObjectSource: jest.fn().mockRejectedValue(new Error('write failed')),
    });
    const handler = new ObjectSourceHandlers(client);

    await expect(handler.handle('patchObjectSource', {
      objectSourceUrl: '/any/url',
      lockHandle: 'lock-1',
      lineChanges: [{ lineNumber: 1, newContent: 'X' }],
    })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('write failed'),
    });
  });
});
