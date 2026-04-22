import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { BaseHandler } from './BaseHandler';
import type { ToolDefinition } from '../types/tools';

// ---------------------------------------------------------------------------
// Patch type definitions
// ---------------------------------------------------------------------------

interface LineChange {
  lineNumber: number;  // 1-based
  newContent: string;
}

interface SearchReplace {
  search: string;
  replacement: string;
  replaceAll?: boolean;
}

interface RangeChange {
  startLine: number;  // 1-based, inclusive
  endLine: number;    // 1-based, inclusive
  newContent: string;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export class ObjectSourceHandlers extends BaseHandler {
  getTools(): ToolDefinition[] {
    return [
      {
        name: 'getObjectSource',
        description: [
          'Retrieves source code for ABAP objects.',
          'Supports chunked reading for large files via the optional chunkSize and chunkIndex parameters.',
          'When chunkSize is provided the response includes chunking metadata (chunked, chunkIndex,',
          'totalSize, totalChunks, hasMore). Omit chunkSize to receive the full source in one call',
          '(only suitable for small files).',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            objectSourceUrl: { type: 'string' },
            options: { type: 'string' },
            chunkSize: {
              type: 'number',
              description: 'Maximum number of characters per chunk. Omit to return the full source.',
            },
            chunkIndex: {
              type: 'number',
              description: 'Zero-based index of the chunk to return. Defaults to 0.',
            },
          },
          required: ['objectSourceUrl']
        }
      },
      {
        name: 'setObjectSource',
        description: 'Sets (fully replaces) source code for ABAP objects. For large files prefer patchObjectSource to avoid sending the entire source.',
        inputSchema: {
          type: 'object',
          properties: {
            objectSourceUrl: { type: 'string' },
            source: { type: 'string' },
            lockHandle: { type: 'string' },
            transport: { type: 'string' }
          },
          required: ['objectSourceUrl', 'source', 'lockHandle']
        }
      },
      {
        name: 'patchObjectSource',
        description: [
          'Makes a targeted edit to an ABAP object source without sending the full file over the wire.',
          'The server reads the current source internally, applies the patch, and writes it back in one',
          'operation. Ideal for large files where setObjectSource would exceed the 1 MB MCP limit.',
          '',
          'Provide exactly ONE of the three patch types:',
          '',
          '• lineChanges   – replace individual lines by 1-based line number.',
          '• searchReplace – find exact text and replace it (optionally all occurrences).',
          '• rangeChange   – replace a contiguous block of lines (startLine–endLine, inclusive, 1-based).',
          '',
          'Returns { status, linesChanged, totalLines }.',
        ].join(' '),
        inputSchema: {
          type: 'object',
          properties: {
            objectSourceUrl: { type: 'string' },
            lockHandle:      { type: 'string' },
            transport:       { type: 'string' },
            lineChanges: {
              type: 'array',
              description: 'Replace specific lines by 1-based line number.',
              items: {
                type: 'object',
                properties: {
                  lineNumber: { type: 'number', description: '1-based line number to replace.' },
                  newContent: { type: 'string', description: 'Replacement text for the line (no trailing newline).' },
                },
                required: ['lineNumber', 'newContent'],
              },
            },
            searchReplace: {
              type: 'array',
              description: 'Find exact text and replace it. Applied in array order.',
              items: {
                type: 'object',
                properties: {
                  search:      { type: 'string', description: 'Exact text to find. Must exist in the source.' },
                  replacement: { type: 'string', description: 'Text to substitute.' },
                  replaceAll:  { type: 'boolean', description: 'Replace every occurrence. Defaults to false (first only).' },
                },
                required: ['search', 'replacement'],
              },
            },
            rangeChange: {
              type: 'object',
              description: 'Replace a contiguous block of lines (startLine–endLine, both 1-based inclusive).',
              properties: {
                startLine:  { type: 'number', description: '1-based first line of the range to replace.' },
                endLine:    { type: 'number', description: '1-based last line of the range to replace (inclusive).' },
                newContent: { type: 'string', description: 'Replacement text. May contain newlines for multi-line replacements.' },
              },
              required: ['startLine', 'endLine', 'newContent'],
            },
          },
          required: ['objectSourceUrl', 'lockHandle']
        }
      }
    ];
  }

  async handle(toolName: string, args: any): Promise<any> {
    switch (toolName) {
      case 'getObjectSource':
        return this.handleGetObjectSource(args);
      case 'setObjectSource':
        return this.handleSetObjectSource(args);
      case 'patchObjectSource':
        return this.handlePatchObjectSource(args);
      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown object source tool: ${toolName}`);
    }
  }

  // -------------------------------------------------------------------------
  // getObjectSource
  // -------------------------------------------------------------------------

  async handleGetObjectSource(args: any): Promise<any> {
    const startTime = performance.now();
    const { objectSourceUrl, options, chunkSize, chunkIndex = 0 } = args;

    if (chunkSize !== undefined) {
      if (typeof chunkSize !== 'number' || chunkSize <= 0) {
        throw new McpError(ErrorCode.InternalError, 'chunkSize must be a positive number');
      }
      if (typeof chunkIndex !== 'number' || chunkIndex < 0) {
        throw new McpError(ErrorCode.InternalError, 'chunkIndex must be a non-negative integer');
      }
    }

    try {
      const source = await this.adtclient.getObjectSource(objectSourceUrl, options);
      this.trackRequest(startTime, true);

      if (chunkSize === undefined) {
        return {
          content: [{ type: 'text', text: JSON.stringify({ status: 'success', source }) }]
        };
      }

      const totalSize = source.length;
      const totalChunks = Math.max(1, Math.ceil(totalSize / chunkSize));
      const start = chunkIndex * chunkSize;
      const chunk = source.slice(start, start + chunkSize);
      const hasMore = chunkIndex < totalChunks - 1;

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              status: 'success',
              source: chunk,
              chunked: true,
              chunkIndex,
              totalSize,
              totalChunks,
              hasMore,
            }),
          },
        ],
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to get object source: ${error.message || 'Unknown error'}`
      );
    }
  }

  // -------------------------------------------------------------------------
  // setObjectSource
  // -------------------------------------------------------------------------

  async handleSetObjectSource(args: any): Promise<any> {
    const startTime = performance.now();
    try {
      await this.adtclient.setObjectSource(
        args.objectSourceUrl,
        args.source,
        args.lockHandle,
        args.transport
      );
      this.trackRequest(startTime, true);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ status: 'success', updated: true })
          }
        ]
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to set object source: ${error.message || 'Unknown error'}`
      );
    }
  }

  // -------------------------------------------------------------------------
  // patchObjectSource
  // -------------------------------------------------------------------------

  async handlePatchObjectSource(args: any): Promise<any> {
    const startTime = performance.now();
    const { objectSourceUrl, lockHandle, transport, lineChanges, searchReplace, rangeChange } = args;

    // Validate: exactly one patch type must be provided
    const patchCount = [lineChanges, searchReplace, rangeChange].filter(p => p !== undefined).length;
    if (patchCount === 0) {
      throw new McpError(
        ErrorCode.InternalError,
        'patchObjectSource requires exactly one patch type: lineChanges, searchReplace, or rangeChange.'
      );
    }
    if (patchCount > 1) {
      throw new McpError(
        ErrorCode.InternalError,
        'patchObjectSource accepts only one patch type per call. Provide lineChanges, searchReplace, OR rangeChange — not multiple.'
      );
    }

    try {
      // Read the full source internally — this never crosses the MCP message boundary
      const source = await this.adtclient.getObjectSource(objectSourceUrl);

      let modified: string;
      let linesChanged: number;

      if (lineChanges !== undefined) {
        ({ source: modified, linesChanged } = this.applyLineChanges(source, lineChanges as LineChange[]));
      } else if (searchReplace !== undefined) {
        ({ source: modified, linesChanged } = this.applySearchReplace(source, searchReplace as SearchReplace[]));
      } else {
        ({ source: modified, linesChanged } = this.applyRangeChange(source, rangeChange as RangeChange));
      }

      await this.adtclient.setObjectSource(objectSourceUrl, modified, lockHandle, transport);
      this.trackRequest(startTime, true);

      const totalLines = modified.split('\n').length;
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ status: 'success', linesChanged, totalLines }),
          },
        ],
      };
    } catch (error: any) {
      this.trackRequest(startTime, false);
      // Re-throw McpErrors from patch validators unchanged
      if (error instanceof McpError) throw error;
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to patch object source: ${error.message || 'Unknown error'}`
      );
    }
  }

  // -------------------------------------------------------------------------
  // Patch type 1: lineChanges
  // -------------------------------------------------------------------------

  private applyLineChanges(source: string, changes: LineChange[]): { source: string; linesChanged: number } {
    const lines = source.split('\n');
    const totalLines = lines.length;

    for (const change of changes) {
      if (change.lineNumber < 1 || change.lineNumber > totalLines) {
        throw new McpError(
          ErrorCode.InternalError,
          `lineNumber ${change.lineNumber} is out of range. The file has ${totalLines} lines (1–${totalLines}).`
        );
      }
    }

    for (const change of changes) {
      lines[change.lineNumber - 1] = change.newContent;
    }

    return { source: lines.join('\n'), linesChanged: changes.length };
  }

  // -------------------------------------------------------------------------
  // Patch type 2: searchReplace
  // -------------------------------------------------------------------------

  private applySearchReplace(source: string, operations: SearchReplace[]): { source: string; linesChanged: number } {
    let current = source;
    let totalReplacements = 0;

    for (const op of operations) {
      if (!current.includes(op.search)) {
        throw new McpError(
          ErrorCode.InternalError,
          `Search string not found: "${op.search}". No changes were made.`
        );
      }

      if (op.replaceAll) {
        // Count occurrences before replacing
        const count = current.split(op.search).length - 1;
        current = current.split(op.search).join(op.replacement);
        totalReplacements += count;
      } else {
        current = current.replace(op.search, op.replacement);
        totalReplacements += 1;
      }
    }

    // Count affected lines (lines that differ between original and patched)
    const originalLines = source.split('\n');
    const patchedLines  = current.split('\n');
    const maxLen = Math.max(originalLines.length, patchedLines.length);
    let linesChanged = 0;
    for (let i = 0; i < maxLen; i++) {
      if (originalLines[i] !== patchedLines[i]) linesChanged++;
    }

    return { source: current, linesChanged };
  }

  // -------------------------------------------------------------------------
  // Patch type 3: rangeChange
  // -------------------------------------------------------------------------

  private applyRangeChange(source: string, range: RangeChange): { source: string; linesChanged: number } {
    const lines = source.split('\n');
    const totalLines = lines.length;

    if (range.startLine < 1) {
      throw new McpError(ErrorCode.InternalError, `startLine must be ≥ 1 (got ${range.startLine}).`);
    }
    if (range.endLine > totalLines) {
      throw new McpError(
        ErrorCode.InternalError,
        `endLine ${range.endLine} exceeds the file length of ${totalLines} lines.`
      );
    }
    if (range.startLine > range.endLine) {
      throw new McpError(
        ErrorCode.InternalError,
        `startLine (${range.startLine}) must be ≤ endLine (${range.endLine}).`
      );
    }

    const before      = lines.slice(0, range.startLine - 1);
    const after       = lines.slice(range.endLine);
    const replacement = range.newContent.split('\n');

    const linesChanged = range.endLine - range.startLine + 1;
    return {
      source: [...before, ...replacement, ...after].join('\n'),
      linesChanged,
    };
  }
}
