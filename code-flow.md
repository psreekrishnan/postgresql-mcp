# PostgreSQL MCP Server Code Flow

## Architecture Overview

The server implements the Model Context Protocol (MCP) to provide PostgreSQL database access to Amazon Q chat.

## Key Components

### 1. PostgreSQLMCPServer Class
Main server class that handles MCP protocol communication and database operations.

### 2. MCP Protocol Handlers

#### `list_resources()`
- **Purpose**: Exposes database schemas as browsable resources
- **Flow**: 
  1. Connects to database
  2. Queries `information_schema.schemata`
  3. Returns each schema as a Resource with URI `postgresql://schema/{name}`

#### `read_resource(uri)`
- **Purpose**: Provides detailed schema information
- **Flow**:
  1. Parses schema name from URI
  2. Lists all tables in the schema
  3. Returns JSON with schema and table information

#### `list_tools()`
- **Purpose**: Declares available database tools to Amazon Q
- **Returns**: Three tools with JSON schemas:
  - `query_database`: Execute SELECT queries
  - `describe_table`: Get table structure
  - `list_tables`: List tables in schema

#### `call_tool(name, arguments)`
- **Purpose**: Executes database operations requested by Amazon Q
- **Flow**:
  1. Validates tool name and arguments
  2. Routes to appropriate database method
  3. Returns results as TextContent

### 3. Database Operations

#### Connection Management
```python
async def _ensure_connection(self):
    # Creates connection using asyncio executor for thread safety
```

#### Query Execution Pattern
All database operations follow this pattern:
```python
def _exec():
    with self.conn.cursor() as cur:
        cur.execute(query, params)
        return results
return await asyncio.get_event_loop().run_in_executor(None, _exec)
```

### 4. Security Features

- **Query Validation**: Only SELECT statements allowed
- **Automatic LIMIT**: Adds LIMIT clause if missing (max 100 rows)
- **Read-only Access**: No INSERT/UPDATE/DELETE operations
- **Connection Timeout**: 60-second timeout in MCP config

## Message Flow

1. **Amazon Q** sends MCP request via stdio
2. **MCP Server** receives JSON-RPC message
3. **Handler** processes request (list_tools, call_tool, etc.)
4. **Database** operation executed in thread executor
5. **Response** sent back to Amazon Q as JSON-RPC

## Error Handling

- Database connection errors logged to stderr
- Invalid queries return error responses
- Async/sync compatibility handled via executors
- Graceful fallback for missing data

## Threading Model

- **Main Thread**: Handles MCP protocol communication
- **Executor Thread**: Runs synchronous database operations
- **Async Wrapper**: Bridges sync psycopg with async MCP server

This design ensures thread safety while maintaining compatibility with both the async MCP protocol and synchronous PostgreSQL driver.