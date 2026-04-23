import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { BaseHandler } from './BaseHandler';
import type { ToolDefinition } from '../types/tools';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Hard ceiling on rows returned in any single call. */
const MAX_ROWS = 1000;
/** Default row limit when the caller does not specify one. */
const DEFAULT_LIMIT = 100;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export class QueryHandlers extends BaseHandler {
  getTools(): ToolDefinition[] {
    return [
      {
        name: 'getTableMetadata',
        description: [
          'Returns the column structure of an ABAP table or view without fetching any data rows.',
          'Use this before tableContents to understand field names, types, key attributes and lengths.',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            ddicEntityName: {
              type: 'string',
              description: 'Name of the DDIC table or view (e.g. MARA, MARC, ZMY_TABLE).',
            },
          },
          required: ['ddicEntityName'],
        },
      },
      {
        name: 'tableContents',
        description: [
          'Reads rows from an ABAP table or view.',
          `Maximum ${MAX_ROWS} rows per call (default ${DEFAULT_LIMIT}).`,
          'Use limit + offset for pagination.',
          'Returns rows, columns, pagination metadata (offset, limit, returnedRows, hasMore).',
          'For complex filtering use runQuery with an explicit WHERE clause instead.',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            ddicEntityName: {
              type: 'string',
              description: 'Name of the DDIC table or view.',
            },
            limit: {
              type: 'number',
              description: `Max rows to return. Defaults to ${DEFAULT_LIMIT}, capped at ${MAX_ROWS}.`,
            },
            offset: {
              type: 'number',
              description: 'Number of rows to skip before returning results. Defaults to 0.',
            },
            decode: {
              type: 'boolean',
              description: 'Decode numeric and date fields into native types. Defaults to true.',
            },
            sqlQuery: {
              type: 'string',
              description: 'Optional WHERE clause to filter rows (e.g. "MTART = \'FERT\'").',
            },
          },
          required: ['ddicEntityName'],
        },
      },
      {
        name: 'runQuery',
        description: [
          'Executes a free-form Open SQL SELECT statement against the SAP system.',
          `Maximum ${MAX_ROWS} rows per call (default ${DEFAULT_LIMIT}).`,
          'Only SELECT statements are supported — the ADT data preview endpoint is read-only.',
          'Returns rows, columns, and the effective row limit applied.',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            sqlQuery: {
              type: 'string',
              description: 'Open SQL SELECT statement to execute.',
            },
            rowNumber: {
              type: 'number',
              description: `Max rows to return. Defaults to ${DEFAULT_LIMIT}, capped at ${MAX_ROWS}.`,
            },
            decode: {
              type: 'boolean',
              description: 'Decode numeric and date fields into native types. Defaults to true.',
            },
          },
          required: ['sqlQuery'],
        },
      },
    ];
  }

  async handle(toolName: string, args: any): Promise<any> {
    switch (toolName) {
      case 'getTableMetadata':
        return this.handleGetTableMetadata(args);
      case 'tableContents':
        return this.handleTableContents(args);
      case 'runQuery':
        return this.handleRunQuery(args);
      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown query tool: ${toolName}`);
    }
  }

  // -------------------------------------------------------------------------
  // getTableMetadata
  // -------------------------------------------------------------------------

  async handleGetTableMetadata(args: any): Promise<any> {
    const startTime = performance.now();
    try {
      // Fetch exactly 1 row — we only care about the column descriptors, not data
      const result = await this.adtclient.tableContents(
        args.ddicEntityName,
        1,
        true,
        ''
      );
      this.trackRequest(startTime, true);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            status: 'success',
            tableName:    args.ddicEntityName,
            totalColumns: result.columns.length,
            columns:      result.columns,
          }),
        }],
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to get table metadata for "${args.ddicEntityName}": ${error.message || 'Unknown error'}`
      );
    }
  }

  // -------------------------------------------------------------------------
  // tableContents  (with pagination + row limit guard)
  // -------------------------------------------------------------------------

  async handleTableContents(args: any): Promise<any> {
    const startTime = performance.now();

    // Resolve and cap limit
    const requestedLimit = args.limit ?? DEFAULT_LIMIT;
    const wasCapped      = requestedLimit > MAX_ROWS;
    const limit          = Math.min(requestedLimit, MAX_ROWS);
    const offset         = args.offset ?? 0;

    try {
      // Fetch offset + limit + 1 rows so we can determine hasMore without
      // a second round-trip. The +1 row is never returned to the caller.
      const fetchCount = offset + limit + 1;
      const result = await this.adtclient.tableContents(
        args.ddicEntityName,
        fetchCount,
        args.decode ?? true,
        args.sqlQuery ?? ''
      );

      const allRows     = result.values;
      const pageRows    = allRows.slice(offset, offset + limit);
      const hasMore     = allRows.length > offset + limit;

      this.trackRequest(startTime, true);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            status:       'success',
            columns:      result.columns,
            rows:         pageRows,
            offset,
            limit,
            returnedRows: pageRows.length,
            hasMore,
            ...(wasCapped && {
              warning: `Requested limit (${requestedLimit}) was capped at the maximum of ${MAX_ROWS} rows.`,
            }),
          }),
        }],
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to retrieve table contents for "${args.ddicEntityName}": ${error.message || 'Unknown error'}`
      );
    }
  }

  // -------------------------------------------------------------------------
  // runQuery  (with row limit guard + McpError)
  // -------------------------------------------------------------------------

  async handleRunQuery(args: any): Promise<any> {
    const startTime = performance.now();

    const requestedLimit = args.rowNumber ?? DEFAULT_LIMIT;
    const wasCapped      = requestedLimit > MAX_ROWS;
    const limit          = Math.min(requestedLimit, MAX_ROWS);

    try {
      const result = await this.adtclient.runQuery(
        args.sqlQuery,
        limit,
        args.decode ?? true
      );
      this.trackRequest(startTime, true);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            status:       'success',
            columns:      result.columns,
            rows:         result.values,
            returnedRows: result.values.length,
            limit,
            ...(wasCapped && {
              warning: `Requested rowNumber (${requestedLimit}) was capped at the maximum of ${MAX_ROWS} rows.`,
            }),
          }),
        }],
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to run query: ${error.message || 'Unknown error'}`
      );
    }
  }
}
