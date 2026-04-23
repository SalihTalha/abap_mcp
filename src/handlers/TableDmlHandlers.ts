import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { BaseHandler } from './BaseHandler';
import type { ToolDefinition } from '../types/tools';

// ---------------------------------------------------------------------------
// Runner class configuration
// ---------------------------------------------------------------------------

const RUNNER_CLASS = (process.env.MCP_DYN_SQL_CLASS ?? 'ZCL_MCP_DYN_SQL').toUpperCase();
const OBJECT_URL   = `/sap/bc/adt/oo/classes/${RUNNER_CLASS.toLowerCase()}`;
const SOURCE_URL   = `${OBJECT_URL}/source/main`;

// Matches the complete body between METHOD if_oo_adt_classrun~main. … ENDMETHOD.
// Captures three groups: (1) METHOD line, (2) body, (3) ENDMETHOD.
const METHOD_BODY_REGEX =
  /(METHOD\s+if_oo_adt_classrun~main\s*\.)([\s\S]*?)(ENDMETHOD\.)/i;

// ---------------------------------------------------------------------------
// Method body templates
// The MCP server owns these — the ABAP class source never needs manual edits.
// ---------------------------------------------------------------------------

/** Written to the class before execution — contains the actual DML statement. */
function buildActiveBody(sql: string): string {
  return `
    DATA(lv_sql)  = \`${sql}\`.
    DATA lv_table TYPE string.
    DATA lv_first TYPE c LENGTH 1.

    FIND REGEX '(?:INSERT\\s+INTO|DELETE\\s+FROM|UPDATE|MODIFY)\\s+(\\w+)'
      IN to_upper( lv_sql )
      SUBMATCHES lv_table.

    IF lv_table IS INITIAL.
      out->write( 'ERR:Could not determine target table from SQL statement' ).
      RETURN.
    ENDIF.

    lv_first = lv_table(1).
    IF lv_first <> 'Z' AND lv_first <> 'Y'.
      out->write( |ERR:Table { lv_table } is not in the customer namespace (Z*/Y*). SAP standard tables cannot be modified.| ).
      RETURN.
    ENDIF.

    TRY.
      DATA(lo_con)  = cl_sql_connection=>get_connection( ).
      DATA(lo_stmt) = lo_con->create_statement( ).
      DATA(lv_rows) = lo_stmt->execute_update( lv_sql ).
      COMMIT WORK AND WAIT.
      out->write( |OK:{ lv_rows }| ).
    CATCH cx_sql_exception INTO DATA(lx).
      ROLLBACK WORK.
      out->write( |ERR:{ lx->get_text( ) }| ).
    ENDTRY.
  `;
}

/** Written back after execution — safe idle state, no injected SQL. */
const IDLE_BODY = `
    " MCP DML Runner — managed by MCP server. Do not edit manually.
    out->write( 'IDLE' ).
  `;

// ---------------------------------------------------------------------------
// Source manipulation helpers
// ---------------------------------------------------------------------------

function injectSql(fullSource: string, sql: string): string {
  return fullSource.replace(METHOD_BODY_REGEX, `$1\n${buildActiveBody(sql)}\n  $3`);
}

function restoreIdle(fullSource: string): string {
  return fullSource.replace(METHOD_BODY_REGEX, `$1\n${IDLE_BODY}\n  $3`);
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export class TableDmlHandlers extends BaseHandler {
  getTools(): ToolDefinition[] {
    return [
      {
        name: 'executeDml',
        description: [
          'Executes a DML statement (INSERT, UPDATE, DELETE, MODIFY) against the SAP database.',
          `Requires the runner class ${RUNNER_CLASS} to exist in the SAP system (package $TMP).`,
          'The complete method body is written by the MCP server on every call — manual edits',
          'to the class source are not needed and will be overwritten.',
          'Only Z* and Y* (customer namespace) tables are permitted.',
          'SELECT statements are rejected. Backtick characters in SQL are not supported.',
          'Expect ~3–5 seconds per call due to the activation step.',
          'Only one DML operation can run at a time — concurrent calls will fail to acquire the lock.',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            sql: {
              type: 'string',
              description: [
                'DML statement to execute: INSERT, UPDATE, DELETE, or MODIFY.',
                'Must target a Z* or Y* table.',
                'Must not contain backtick (`) characters.',
              ].join(' '),
            },
            transport: {
              type: 'string',
              description: 'Transport request number. Usually not needed for $TMP classes.',
            },
          },
          required: ['sql'],
        },
      },
    ];
  }

  async handle(toolName: string, args: any): Promise<any> {
    switch (toolName) {
      case 'executeDml':
        return this.handleExecuteDml(args);
      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown DML tool: ${toolName}`);
    }
  }

  // -------------------------------------------------------------------------
  // executeDml
  // -------------------------------------------------------------------------

  async handleExecuteDml(args: any): Promise<any> {
    const startTime = performance.now();
    const { sql, transport } = args;

    // Validate before touching SAP
    this.validateSql(sql);

    // Acquire lock
    let lockHandle: string;
    try {
      const lockResult = await this.adtclient.lock(OBJECT_URL);
      lockHandle = lockResult.LOCK_HANDLE;
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to lock ${RUNNER_CLASS}: ${error.message || 'Unknown error'}. ` +
        'The class may already be locked by another user or process.'
      );
    }

    try {
      // Read current source to preserve the CLASS DEFINITION section
      const originalSource = await this.adtclient.getObjectSource(SOURCE_URL);

      // Verify the method signature exists (sanity check on the class structure)
      if (!METHOD_BODY_REGEX.test(originalSource)) {
        throw new McpError(
          ErrorCode.InternalError,
          `Could not locate the METHOD if_oo_adt_classrun~main signature in ${RUNNER_CLASS}. ` +
          'Ensure the class implements IF_OO_ADT_CLASSRUN correctly.'
        );
      }

      // Inject SQL — replaces entire method body regardless of current content
      const patchedSource = injectSql(originalSource, sql);
      await this.adtclient.setObjectSource(SOURCE_URL, patchedSource, lockHandle, transport);

      // Activate
      const activation = await this.adtclient.activate(RUNNER_CLASS, OBJECT_URL);
      if (!activation.success) {
        const messages = activation.messages
          .map(m => m.shortText)
          .filter(Boolean)
          .join('; ');
        throw new McpError(
          ErrorCode.InternalError,
          `Activation of ${RUNNER_CLASS} failed. ` +
          (messages ? `Details: ${messages}` : 'No details available.')
        );
      }

      // Execute
      const output = (await this.adtclient.runClass(RUNNER_CLASS)).trim();

      // Restore idle state (best-effort — failure here is logged, not re-thrown)
      try {
        const idleSource = restoreIdle(originalSource);
        await this.adtclient.setObjectSource(SOURCE_URL, idleSource, lockHandle, transport);
      } catch (restoreError: any) {
        this.logger.warn('Failed to restore runner to idle state', {
          class: RUNNER_CLASS,
          error: restoreError.message,
        });
      }

      // Parse runner output
      if (output.startsWith('OK:')) {
        const rowsAffected = parseInt(output.slice(3), 10);
        this.trackRequest(startTime, true);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              status:       'success',
              rowsAffected: isNaN(rowsAffected) ? 0 : rowsAffected,
              runner:       RUNNER_CLASS,
            }),
          }],
        };
      }

      if (output.startsWith('ERR:')) {
        throw new McpError(ErrorCode.InternalError, `DML execution failed: ${output.slice(4)}`);
      }

      throw new McpError(
        ErrorCode.InternalError,
        `Unexpected runner output: "${output}". Expected "OK:<rows>" or "ERR:<message>".`
      );

    } catch (error: any) {
      this.trackRequest(startTime, false);
      if (error instanceof McpError) throw error;
      throw new McpError(
        ErrorCode.InternalError,
        `executeDml failed: ${error.message || 'Unknown error'}`
      );
    } finally {
      try {
        await this.adtclient.unLock(OBJECT_URL, lockHandle!);
      } catch (unlockError: any) {
        this.logger.warn('Failed to unlock runner class', {
          class:  RUNNER_CLASS,
          handle: lockHandle!,
          error:  unlockError.message,
        });
      }
    }
  }

  // -------------------------------------------------------------------------
  // SQL validation
  // -------------------------------------------------------------------------

  private validateSql(sql: unknown): void {
    if (typeof sql !== 'string' || sql.trim().length === 0) {
      throw new McpError(ErrorCode.InternalError, 'sql must be a non-empty string.');
    }

    const trimmed = sql.trim().toUpperCase();

    if (trimmed.startsWith('SELECT')) {
      throw new McpError(
        ErrorCode.InternalError,
        'SELECT statements are not supported by executeDml. ' +
        'Use tableContents or runQuery to read data.'
      );
    }

    if (sql.includes('`')) {
      throw new McpError(
        ErrorCode.InternalError,
        'SQL must not contain backtick (`) characters — they would break the ABAP string literal.'
      );
    }

    this.validateCustomerNamespace(sql);
  }

  private validateCustomerNamespace(sql: string): void {
    const TABLE_REGEX = /(?:INSERT\s+INTO|DELETE\s+FROM|UPDATE|MODIFY)\s+(\w+)/i;
    const match = TABLE_REGEX.exec(sql);

    if (!match) return; // Let the ABAP class handle unrecognised patterns

    const tableName = match[1].toUpperCase();
    if (!tableName.startsWith('Z') && !tableName.startsWith('Y')) {
      throw new McpError(
        ErrorCode.InternalError,
        `Table "${tableName}" is not in the customer namespace. ` +
        'Only Z* and Y* tables are permitted. ' +
        'SAP standard tables cannot be modified through this tool.'
      );
    }
  }
}
