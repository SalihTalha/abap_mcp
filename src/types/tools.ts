/**
 * Recursive JSON Schema node — permissive enough to describe any tool input
 * schema (nested objects, arrays, required fields, etc.) while remaining
 * compatible with the MCP SDK's expected shape.
 */
export interface JsonSchemaProperty {
  type: string;
  description?: string;
  optional?: boolean;
  // object
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  // array
  items?: JsonSchemaProperty;
  // enum
  enum?: (string | number | boolean)[];
}

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: {
    type: string;
    properties: Record<string, JsonSchemaProperty>;
    required?: string[];
  };
}
