import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { BaseHandler } from './BaseHandler';
import type { ToolDefinition } from '../types/tools';

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
        description: 'Sets source code for ABAP objects',
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
      }
    ];
  }

  async handle(toolName: string, args: any): Promise<any> {
    switch (toolName) {
      case 'getObjectSource':
        return this.handleGetObjectSource(args);
      case 'setObjectSource':
        return this.handleSetObjectSource(args);
      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown object source tool: ${toolName}`);
    }
  }

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
            text: JSON.stringify({
              status: 'success',
              updated: true
            })
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
}
