import { QueryHandlers } from '../../handlers/QueryHandlers';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import type { ADTClient } from 'abap-adt-api';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const COLUMNS = [
  { name: 'MANDT',   type: 'C', description: 'Client',      keyAttribute: true,  colType: 'C', isKeyFigure: false, length: 3  },
  { name: 'MATNR',   type: 'C', description: 'Material',    keyAttribute: true,  colType: 'C', isKeyFigure: false, length: 18 },
  { name: 'MTART',   type: 'C', description: 'Mat. type',   keyAttribute: false, colType: 'C', isKeyFigure: false, length: 4  },
  { name: 'MAKTX',   type: 'C', description: 'Description', keyAttribute: false, colType: 'C', isKeyFigure: false, length: 40 },
];

function makeRows(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    MANDT: '100',
    MATNR: `MAT${String(i).padStart(5, '0')}`,
    MTART: 'FERT',
    MAKTX: `Material ${i}`,
  }));
}

function makeQueryResult(rowCount: number) {
  return { columns: COLUMNS, values: makeRows(rowCount) };
}

function makeMockClient(overrides: Partial<ADTClient> = {}): ADTClient {
  return {
    tableContents: jest.fn().mockResolvedValue(makeQueryResult(10)),
    runQuery:      jest.fn().mockResolvedValue(makeQueryResult(10)),
    ...overrides,
  } as unknown as ADTClient;
}

// ---------------------------------------------------------------------------
// Tool definitions – regression guard
// ---------------------------------------------------------------------------

describe('QueryHandlers – tool definitions', () => {
  const handler = new QueryHandlers(makeMockClient());
  const tools   = handler.getTools();

  it('exports exactly three tools', () => {
    expect(tools).toHaveLength(3);
  });

  it('has tableContents tool', () => {
    expect(tools.find(t => t.name === 'tableContents')).toBeDefined();
  });

  it('has runQuery tool', () => {
    expect(tools.find(t => t.name === 'runQuery')).toBeDefined();
  });

  it('has getTableMetadata tool', () => {
    expect(tools.find(t => t.name === 'getTableMetadata')).toBeDefined();
  });

  it('tableContents requires ddicEntityName', () => {
    const tool = tools.find(t => t.name === 'tableContents')!;
    expect(tool.inputSchema.required).toContain('ddicEntityName');
  });

  it('tableContents exposes limit and offset as optional', () => {
    const tool = tools.find(t => t.name === 'tableContents')!;
    expect(tool.inputSchema.properties).toHaveProperty('limit');
    expect(tool.inputSchema.properties).toHaveProperty('offset');
    expect(tool.inputSchema.required).not.toContain('limit');
    expect(tool.inputSchema.required).not.toContain('offset');
  });

  it('getTableMetadata requires ddicEntityName', () => {
    const tool = tools.find(t => t.name === 'getTableMetadata')!;
    expect(tool.inputSchema.required).toContain('ddicEntityName');
  });

  it('runQuery requires sqlQuery', () => {
    const tool = tools.find(t => t.name === 'runQuery')!;
    expect(tool.inputSchema.required).toContain('sqlQuery');
  });
});

// ---------------------------------------------------------------------------
// handle() routing
// ---------------------------------------------------------------------------

describe('QueryHandlers – routing', () => {
  it('throws MethodNotFound for unknown tool', async () => {
    const handler = new QueryHandlers(makeMockClient());
    await expect(handler.handle('noSuchTool', {}))
      .rejects.toMatchObject({ code: ErrorCode.MethodNotFound });
  });
});

// ---------------------------------------------------------------------------
// getTableMetadata
// ---------------------------------------------------------------------------

describe('getTableMetadata', () => {
  it('returns status:success and columns array', async () => {
    const client  = makeMockClient();
    const handler = new QueryHandlers(client);

    const result = await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.status).toBe('success');
    expect(Array.isArray(parsed.columns)).toBe(true);
  });

  it('returns column metadata fields (name, type, description, keyAttribute, length)', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });
    const parsed  = JSON.parse(result.content[0].text);
    const col     = parsed.columns[0];

    expect(col).toHaveProperty('name');
    expect(col).toHaveProperty('type');
    expect(col).toHaveProperty('description');
    expect(col).toHaveProperty('keyAttribute');
    expect(col).toHaveProperty('length');
  });

  it('correctly identifies key columns', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });
    const parsed  = JSON.parse(result.content[0].text);

    const keys    = parsed.columns.filter((c: any) => c.keyAttribute);
    const nonKeys = parsed.columns.filter((c: any) => !c.keyAttribute);
    expect(keys.length).toBeGreaterThan(0);
    expect(nonKeys.length).toBeGreaterThan(0);
  });

  it('includes totalColumns count', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed.totalColumns).toBe(COLUMNS.length);
  });

  it('calls tableContents with rowNumber:1 to minimise data transfer', async () => {
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(1));
    const client = makeMockClient({ tableContents: mockFn });
    const handler = new QueryHandlers(client);

    await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });

    expect(mockFn).toHaveBeenCalledWith('MARA', 1, expect.anything(), expect.anything());
  });

  it('throws McpError(InternalError) on ADTClient failure', async () => {
    const client = makeMockClient({
      tableContents: jest.fn().mockRejectedValue(new Error('not found')),
    });
    const handler = new QueryHandlers(client);

    await expect(handler.handle('getTableMetadata', { ddicEntityName: 'ZNOPE' }))
      .rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('returns a single text content block', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('getTableMetadata', { ddicEntityName: 'MARA' });

    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe('text');
  });
});

// ---------------------------------------------------------------------------
// tableContents – baseline (regression)
// ---------------------------------------------------------------------------

describe('tableContents – baseline', () => {
  it('returns status:success with rows and columns', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('tableContents', { ddicEntityName: 'MARA' });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed.status).toBe('success');
    expect(Array.isArray(parsed.rows)).toBe(true);
    expect(Array.isArray(parsed.columns)).toBe(true);
  });

  it('passes ddicEntityName through to ADTClient', async () => {
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(5));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    await handler.handle('tableContents', { ddicEntityName: 'MARC' });

    expect(mockFn.mock.calls[0][0]).toBe('MARC');
  });

  it('throws McpError(InternalError) on ADTClient failure', async () => {
    const client = makeMockClient({
      tableContents: jest.fn().mockRejectedValue(new Error('table locked')),
    });
    const handler = new QueryHandlers(client);

    await expect(handler.handle('tableContents', { ddicEntityName: 'MARA' }))
      .rejects.toMatchObject({ code: ErrorCode.InternalError });
  });
});

// ---------------------------------------------------------------------------
// tableContents – row limit guard
// ---------------------------------------------------------------------------

describe('tableContents – row limit guard', () => {
  it('defaults to 100 rows when limit is not specified', async () => {
    const mockFn  = jest.fn().mockResolvedValue(makeQueryResult(10));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    await handler.handle('tableContents', { ddicEntityName: 'MARA' });

    // The first numeric argument to tableContents is rowNumber
    expect(mockFn.mock.calls[0][1]).toBeLessThanOrEqual(101); // fetches limit+1 for hasMore
  });

  it('caps requests above MAX_ROWS (1000) at 1000', async () => {
    const mockFn  = jest.fn().mockResolvedValue(makeQueryResult(10));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 99999,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.limit).toBe(1000);
  });

  it('includes a warning in the response when the limit was capped', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 5000,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.warning).toMatch(/capped/i);
  });

  it('does not include a warning when limit is within range', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 50,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.warning).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// tableContents – pagination
// ---------------------------------------------------------------------------

describe('tableContents – pagination', () => {
  it('returns pagination metadata (limit, offset, returnedRows, hasMore)', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('tableContents', { ddicEntityName: 'MARA', limit: 5 });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed).toHaveProperty('limit');
    expect(parsed).toHaveProperty('offset');
    expect(parsed).toHaveProperty('returnedRows');
    expect(parsed).toHaveProperty('hasMore');
  });

  it('offset defaults to 0', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('tableContents', { ddicEntityName: 'MARA' });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed.offset).toBe(0);
  });

  it('slices rows correctly for a given offset', async () => {
    // ADTClient returns 10 rows (indices 0-9); we want offset=3, limit=4 → rows 3,4,5,6
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(10));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 4,
      offset: 3,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.rows).toHaveLength(4);
    expect(parsed.rows[0].MATNR).toBe('MAT00003');
    expect(parsed.rows[3].MATNR).toBe('MAT00006');
  });

  it('sets hasMore:true when more rows exist beyond the current page', async () => {
    // Returns 11 rows for limit=5, offset=0 → hasMore should be true
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(11));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 5,
      offset: 0,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.hasMore).toBe(true);
  });

  it('sets hasMore:false on the last page', async () => {
    // 10 rows total, limit=5, offset=5 → exactly 5 left, no more
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(10));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 5,
      offset: 5,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.hasMore).toBe(false);
  });

  it('returns empty rows and hasMore:false when offset exceeds available data', async () => {
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(3));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 10,
      offset: 50,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.rows).toHaveLength(0);
    expect(parsed.hasMore).toBe(false);
  });

  it('returnedRows reflects the actual number of rows in the response', async () => {
    const mockFn = jest.fn().mockResolvedValue(makeQueryResult(7));
    const handler = new QueryHandlers(makeMockClient({ tableContents: mockFn }));

    const result = await handler.handle('tableContents', {
      ddicEntityName: 'MARA',
      limit: 10,
      offset: 0,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.returnedRows).toBe(7);
  });
});

// ---------------------------------------------------------------------------
// runQuery – baseline + McpError fix
// ---------------------------------------------------------------------------

describe('runQuery – baseline and error handling', () => {
  it('returns status:success with rows and columns', async () => {
    const handler = new QueryHandlers(makeMockClient());
    const result  = await handler.handle('runQuery', { sqlQuery: 'SELECT * FROM MARA' });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed.status).toBe('success');
    expect(Array.isArray(parsed.rows)).toBe(true);
    expect(Array.isArray(parsed.columns)).toBe(true);
  });

  it('passes sqlQuery through to ADTClient', async () => {
    const mockFn  = jest.fn().mockResolvedValue(makeQueryResult(5));
    const handler = new QueryHandlers(makeMockClient({ runQuery: mockFn }));
    const sql     = 'SELECT MATNR FROM MARA WHERE MTART = \'FERT\'';

    await handler.handle('runQuery', { sqlQuery: sql });

    expect(mockFn.mock.calls[0][0]).toBe(sql);
  });

  it('throws McpError(InternalError) — not a raw Error — on ADTClient failure', async () => {
    const client = makeMockClient({
      runQuery: jest.fn().mockRejectedValue(new Error('syntax error')),
    });
    const handler = new QueryHandlers(client);

    const rejection = handler.handle('runQuery', { sqlQuery: 'INVALID SQL' });

    // Must be an McpError, not a plain Error
    await expect(rejection).rejects.toBeInstanceOf(McpError);
    await expect(rejection).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: expect.stringContaining('syntax error'),
    });
  });

  it('caps runQuery row limit at MAX_ROWS (1000)', async () => {
    const mockFn  = jest.fn().mockResolvedValue(makeQueryResult(10));
    const handler = new QueryHandlers(makeMockClient({ runQuery: mockFn }));

    const result = await handler.handle('runQuery', {
      sqlQuery: 'SELECT * FROM MARA',
      rowNumber: 50000,
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.limit).toBeLessThanOrEqual(1000);
  });
});
