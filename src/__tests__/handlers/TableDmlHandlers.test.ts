import { TableDmlHandlers } from '../../handlers/TableDmlHandlers';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import type { ADTClient } from 'abap-adt-api';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Simulate the class source as it might look in any state —
// the new implementation doesn't depend on a specific placeholder being present.
const RUNNER_SOURCE_WITH_PLACEHOLDER = [
  'CLASS zcl_mcp_dyn_sql DEFINITION PUBLIC FINAL CREATE PUBLIC.',
  '  PUBLIC SECTION.',
  '    INTERFACES if_oo_adt_classrun.',
  'ENDCLASS.',
  '',
  'CLASS zcl_mcp_dyn_sql IMPLEMENTATION.',
  '  METHOD if_oo_adt_classrun~main.',
  '    " MCP DML Runner — managed by MCP server. Do not edit manually.',
  '    out->write( \'IDLE\' ).',
  '  ENDMETHOD.',
  'ENDCLASS.',
].join('\n');

// Source in "crashed" state — injected SQL still present, no placeholder.
// The new implementation must handle this without error.
const RUNNER_SOURCE_CRASHED = [
  'CLASS zcl_mcp_dyn_sql DEFINITION PUBLIC FINAL CREATE PUBLIC.',
  '  PUBLIC SECTION.',
  '    INTERFACES if_oo_adt_classrun.',
  'ENDCLASS.',
  '',
  'CLASS zcl_mcp_dyn_sql IMPLEMENTATION.',
  '  METHOD if_oo_adt_classrun~main.',
  "    DATA(lv_sql) = `INSERT INTO ztable VALUES ('stale')`."],
  '    out->write( |OK:1| ).',
  '  ENDMETHOD.',
  'ENDCLASS.',
].join('\n');

const MOCK_LOCK = {
  LOCK_HANDLE: 'lock-handle-abc',
  CORRNR: '', CORRUSER: '', CORRTEXT: '',
  IS_LOCAL: 'X', IS_LINK_UP: '', MODIFICATION_SUPPORT: 'MODIFY',
};

const ACTIVATION_OK = { success: true, messages: [], inactive: [] };

function makeMockClient(overrides: Partial<ADTClient> = {}): ADTClient {
  return {
    lock:            jest.fn().mockResolvedValue(MOCK_LOCK),
    unLock:          jest.fn().mockResolvedValue(''),
    getObjectSource: jest.fn().mockResolvedValue(RUNNER_SOURCE_WITH_PLACEHOLDER),
    setObjectSource: jest.fn().mockResolvedValue(undefined),
    activate:        jest.fn().mockResolvedValue(ACTIVATION_OK),
    runClass:        jest.fn().mockResolvedValue('OK:1'),
    ...overrides,
  } as unknown as ADTClient;
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('TableDmlHandlers – tool definition', () => {
  const handler = new TableDmlHandlers(makeMockClient());
  const tools   = handler.getTools();

  it('exports exactly one tool', () => {
    expect(tools).toHaveLength(1);
  });

  it('tool is named executeDml', () => {
    expect(tools[0].name).toBe('executeDml');
  });

  it('requires sql parameter', () => {
    expect(tools[0].inputSchema.required).toContain('sql');
  });

  it('transport is optional', () => {
    expect(tools[0].inputSchema.required).not.toContain('transport');
  });
});

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

describe('TableDmlHandlers – routing', () => {
  it('throws MethodNotFound for unknown tool', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('noSuchTool', {}))
      .rejects.toMatchObject({ code: ErrorCode.MethodNotFound });
  });
});

// ---------------------------------------------------------------------------
// executeDml – happy path
// ---------------------------------------------------------------------------

describe('executeDml – success', () => {
  it('returns status:success and rowsAffected on OK runner output', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    const result  = await handler.handle('executeDml', {
      sql: "INSERT INTO ztable VALUES ('001', 'test')",
    });
    const parsed = JSON.parse(result.content[0].text);

    expect(parsed.status).toBe('success');
    expect(parsed.rowsAffected).toBe(1);
  });

  it('parses rowsAffected from runner output correctly (multi-row)', async () => {
    const client  = makeMockClient({ runClass: jest.fn().mockResolvedValue('OK:5') });
    const handler = new TableDmlHandlers(client);
    const result  = await handler.handle('executeDml', { sql: "DELETE FROM ztable" });
    const parsed  = JSON.parse(result.content[0].text);

    expect(parsed.rowsAffected).toBe(5);
  });

  it('returns a single text content block', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    const result  = await handler.handle('executeDml', { sql: "UPDATE ztable SET f = 'x'" });

    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe('text');
  });

  it('succeeds even when the class source is in a crashed state (no placeholder)', async () => {
    const client  = makeMockClient({
      getObjectSource: jest.fn().mockResolvedValue(RUNNER_SOURCE_CRASHED),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .resolves.toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// executeDml – ADTClient call sequence
// ---------------------------------------------------------------------------

describe('executeDml – ADTClient call sequence', () => {
  it('locks the runner class before reading source', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" });

    const lockOrder   = (client.lock as jest.Mock).mock.invocationCallOrder[0];
    const sourceOrder = (client.getObjectSource as jest.Mock).mock.invocationCallOrder[0];
    expect(lockOrder).toBeLessThan(sourceOrder);
  });

  it('injects the SQL into the runner source (replaces entire method body)', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    const sql     = "UPDATE ztable SET col = 'value'";
    await handler.handle('executeDml', { sql });

    const firstWrite = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(firstWrite).toContain(sql);
  });

  it('preserves the CLASS DEFINITION section when injecting SQL', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "UPDATE ztable SET col = 'x'" });

    const firstWrite = (client.setObjectSource as jest.Mock).mock.calls[0][1] as string;
    expect(firstWrite).toContain('CLASS zcl_mcp_dyn_sql DEFINITION');
    expect(firstWrite).toContain('INTERFACES if_oo_adt_classrun');
  });

  it('activates the class after injecting the SQL', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "DELETE FROM ztable WHERE key = '1'" });

    const writeOrder    = (client.setObjectSource as jest.Mock).mock.invocationCallOrder[0];
    const activateOrder = (client.activate as jest.Mock).mock.invocationCallOrder[0];
    expect(writeOrder).toBeLessThan(activateOrder);
  });

  it('calls runClass after activation', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" });

    const activateOrder = (client.activate as jest.Mock).mock.invocationCallOrder[0];
    const runOrder      = (client.runClass as jest.Mock).mock.invocationCallOrder[0];
    expect(activateOrder).toBeLessThan(runOrder);
  });

  it('restores the method body to idle state after running', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    const sql     = "INSERT INTO ztable VALUES ('x')";
    await handler.handle('executeDml', { sql });

    // Second write should NOT contain the injected SQL
    const secondWrite = (client.setObjectSource as jest.Mock).mock.calls[1][1] as string;
    expect(secondWrite).not.toContain(sql);
  });

  it('idle restore still contains the CLASS DEFINITION', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" });

    const secondWrite = (client.setObjectSource as jest.Mock).mock.calls[1][1] as string;
    expect(secondWrite).toContain('CLASS zcl_mcp_dyn_sql DEFINITION');
  });

  it('always unlocks even after a successful run', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" });

    expect(client.unLock).toHaveBeenCalledTimes(1);
    expect(client.unLock).toHaveBeenCalledWith(
      expect.stringContaining('zcl_mcp_dyn_sql'),
      MOCK_LOCK.LOCK_HANDLE,
    );
  });

  it('passes the lockHandle to setObjectSource', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" });

    const calls = (client.setObjectSource as jest.Mock).mock.calls;
    expect(calls[0][2]).toBe(MOCK_LOCK.LOCK_HANDLE);
    expect(calls[1][2]).toBe(MOCK_LOCK.LOCK_HANDLE);
  });

  it('passes transport to setObjectSource when provided', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await handler.handle('executeDml', {
      sql: "INSERT INTO ztable VALUES ('x')",
      transport: 'DEVK900042',
    });

    expect((client.setObjectSource as jest.Mock).mock.calls[0][3]).toBe('DEVK900042');
  });
});

// ---------------------------------------------------------------------------
// executeDml – SQL validation
// ---------------------------------------------------------------------------

describe('executeDml – SQL validation', () => {
  async function expectValidationError(sql: string) {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql }))
      .rejects.toMatchObject({ code: ErrorCode.InternalError });
  }

  it('rejects empty sql', () => expectValidationError(''));
  it('rejects whitespace-only sql', () => expectValidationError('   '));

  it('rejects SELECT statements', () => expectValidationError('SELECT * FROM ztable'));
  it('rejects SELECT regardless of case', () => expectValidationError('select * from ztable'));
  it('rejects SELECT with leading whitespace', () => expectValidationError('  SELECT 1'));

  it('rejects sql containing backtick characters', () =>
    expectValidationError('INSERT INTO ztable VALUES (`key`)'));

  it('does not call lock when validation fails', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await expect(handler.handle('executeDml', { sql: 'SELECT 1' })).rejects.toThrow();
    expect(client.lock).not.toHaveBeenCalled();
  });

  it('accepts INSERT', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .resolves.toBeDefined();
  });

  it('accepts UPDATE', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "UPDATE ztable SET f = 'x'" }))
      .resolves.toBeDefined();
  });

  it('accepts DELETE', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "DELETE FROM ztable WHERE k = '1'" }))
      .resolves.toBeDefined();
  });

  it('accepts MODIFY (ABAP Open SQL)', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "MODIFY ztable FROM @ls_row" }))
      .resolves.toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// executeDml – Z/Y table namespace validation
// ---------------------------------------------------------------------------

describe('executeDml – Z/Y namespace validation', () => {
  async function expectNamespaceError(sql: string) {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql }))
      .rejects.toMatchObject({
        code:    ErrorCode.InternalError,
        message: expect.stringMatching(/customer namespace|Z\*|Y\*/i),
      });
  }

  it('rejects INSERT INTO a SAP standard table (MARA)', () =>
    expectNamespaceError("INSERT INTO mara VALUES ('x')"));

  it('rejects UPDATE on a SAP standard table (T001)', () =>
    expectNamespaceError("UPDATE t001 SET bukrs = 'TEST'"));

  it('rejects DELETE FROM a SAP standard table (BKPF)', () =>
    expectNamespaceError("DELETE FROM bkpf WHERE bukrs = '0001'"));

  it('rejects MODIFY on a SAP standard table (EKKO)', () =>
    expectNamespaceError("MODIFY ekko FROM @ls_row"));

  it('accepts INSERT INTO a Z table', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .resolves.toBeDefined();
  });

  it('accepts INSERT INTO a Y table', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "INSERT INTO ytable VALUES ('x')" }))
      .resolves.toBeDefined();
  });

  it('does not call lock when namespace validation fails', async () => {
    const client  = makeMockClient();
    const handler = new TableDmlHandlers(client);
    await expect(handler.handle('executeDml', { sql: "INSERT INTO mara VALUES ('x')" }))
      .rejects.toThrow();
    expect(client.lock).not.toHaveBeenCalled();
  });

  it('includes the rejected table name in the error message', async () => {
    const handler = new TableDmlHandlers(makeMockClient());
    await expect(handler.handle('executeDml', { sql: "DELETE FROM bseg WHERE ..." }))
      .rejects.toMatchObject({ message: expect.stringContaining('BSEG') });
  });
});

// ---------------------------------------------------------------------------
// executeDml – error handling
// ---------------------------------------------------------------------------

describe('executeDml – error handling', () => {
  it('throws McpError when runner output starts with ERR:', async () => {
    const client  = makeMockClient({
      runClass: jest.fn().mockResolvedValue('ERR:Table ZTABLE not found'),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toMatchObject({
        code:    ErrorCode.InternalError,
        message: expect.stringContaining('Table ZTABLE not found'),
      });
  });

  it('still unlocks when runner returns ERR:', async () => {
    const client  = makeMockClient({ runClass: jest.fn().mockResolvedValue('ERR:oops') });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toThrow();
    expect(client.unLock).toHaveBeenCalledTimes(1);
  });

  it('restores idle method body even when runner returns ERR:', async () => {
    const client  = makeMockClient({ runClass: jest.fn().mockResolvedValue('ERR:oops') });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toThrow();

    // Second write restores idle body — should not contain injected SQL
    const restoreCall = (client.setObjectSource as jest.Mock).mock.calls[1];
    expect(restoreCall[1]).not.toContain("INSERT INTO ztable VALUES ('x')");
  });

  it('throws McpError when activation fails and includes activation messages', async () => {
    const client  = makeMockClient({
      activate: jest.fn().mockResolvedValue({
        success:  false,
        messages: [{ shortText: 'Syntax error in line 3', type: 'E', line: 3, objDescr: '', href: '', forceSupported: false }],
        inactive: [],
      }),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toMatchObject({
        code:    ErrorCode.InternalError,
        message: expect.stringContaining('Syntax error'),
      });
  });

  it('unlocks when activation fails', async () => {
    const client  = makeMockClient({
      activate: jest.fn().mockResolvedValue({ success: false, messages: [], inactive: [] }),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toThrow();
    expect(client.unLock).toHaveBeenCalledTimes(1);
  });

  it('throws McpError when lock fails', async () => {
    const client  = makeMockClient({
      lock: jest.fn().mockRejectedValue(new Error('object locked by another user')),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toMatchObject({ code: ErrorCode.InternalError });
  });

  it('throws McpError when METHOD signature is not found in class source', async () => {
    const client  = makeMockClient({
      getObjectSource: jest.fn().mockResolvedValue(
        'CLASS zcl_mcp_dyn_sql DEFINITION. ENDCLASS.\nCLASS zcl_mcp_dyn_sql IMPLEMENTATION. ENDCLASS.'
      ),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toMatchObject({
        code:    ErrorCode.InternalError,
        message: expect.stringContaining('METHOD'),
      });
  });

  it('unlocks when METHOD signature is not found', async () => {
    const client  = makeMockClient({
      getObjectSource: jest.fn().mockResolvedValue('CLASS impl. ENDCLASS.'),
    });
    const handler = new TableDmlHandlers(client);

    await expect(handler.handle('executeDml', { sql: "INSERT INTO ztable VALUES ('x')" }))
      .rejects.toThrow();
    expect(client.unLock).toHaveBeenCalledTimes(1);
  });
});
