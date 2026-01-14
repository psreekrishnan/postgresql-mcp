#!/usr/bin/env python3
"""PostgreSQL MCP Server"""

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List, Optional
import psycopg
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Resource, Tool, TextContent, CallToolResult, ListResourcesResult, ListToolsResult, ReadResourceResult
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

class PostgreSQLMCPServer:
    def __init__(self, dsn: str, max_rows: int = 100):
        self.dsn = dsn
        self.max_rows = max_rows
        self.conn = None
        self.server = Server("postgresql-mcp-server")
        self._setup_handlers()

    def _setup_handlers(self):
        @self.server.list_resources()
        async def list_resources() -> ListResourcesResult:
            try:
                await self._ensure_connection()
                schemas = await self._list_schemas()
                resources = [
                    Resource(
                        uri=f"postgresql://schema/{schema}",
                        name=f"Schema: {schema}",
                        description=f"PostgreSQL schema '{schema}'",
                        mimeType="application/json"
                    ) for schema in schemas
                ]
                return ListResourcesResult(resources=resources)
            except Exception as e:
                logger.error(f"Error listing resources: {e}")
                return ListResourcesResult(resources=[])

        @self.server.read_resource()
        async def read_resource(uri: str) -> ReadResourceResult:
            try:
                if not uri.startswith("postgresql://schema/"):
                    raise ValueError(f"Unsupported resource URI: {uri}")
                
                schema_name = uri.replace("postgresql://schema/", "")
                await self._ensure_connection()
                tables = await self._list_tables(schema_name)
                
                return ReadResourceResult(
                    contents=[TextContent(type="text", text=json.dumps({"schema": schema_name, "tables": tables}, indent=2))]
                )
            except Exception as e:
                return ReadResourceResult(contents=[TextContent(type="text", text=f"Error: {str(e)}")])

        @self.server.list_tools()
        async def list_tools() -> ListToolsResult:
            tools = [
                Tool(
                    name="query_database",
                    description="Execute SELECT query",
                    inputSchema={
                        "type": "object",
                        "properties": {"sql": {"type": "string", "description": "SQL SELECT query"}},
                        "required": ["sql"]
                    }
                ),
                Tool(
                    name="describe_table",
                    description="Get table structure",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name"},
                            "table": {"type": "string", "description": "Table name"}
                        },
                        "required": ["schema", "table"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List tables in schema",
                    inputSchema={
                        "type": "object",
                        "properties": {"schema": {"type": "string", "description": "Schema name (optional)"}},
                    }
                )
            ]
            return ListToolsResult(tools=tools)

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> CallToolResult:
            try:
                await self._ensure_connection()
                
                if name == "query_database":
                    sql = arguments.get("sql", "")
                    if not sql:
                        raise ValueError("SQL query required")
                    result = await self._execute_query(sql)
                    return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, indent=2))])
                
                elif name == "describe_table":
                    schema = arguments.get("schema")
                    table = arguments.get("table")
                    if not schema or not table:
                        raise ValueError("Both schema and table required")
                    columns = await self._describe_table(schema, table)
                    return CallToolResult(content=[TextContent(type="text", text=json.dumps({"columns": columns}, indent=2))])
                
                elif name == "list_tables":
                    schema = arguments.get("schema")
                    tables = await self._list_tables(schema)
                    return CallToolResult(content=[TextContent(type="text", text=json.dumps({"tables": tables}, indent=2))])
                
                else:
                    raise ValueError(f"Unknown tool: {name}")
                    
            except Exception as e:
                return CallToolResult(content=[TextContent(type="text", text=f"Error: {str(e)}")], isError=True)

    async def _ensure_connection(self):
        # Always create a fresh connection to avoid transaction state issues
        if self.conn is not None:
            try:
                self.conn.close()
            except:
                pass
        self.conn = await asyncio.get_event_loop().run_in_executor(None, psycopg.connect, self.dsn)

    async def _list_schemas(self) -> List[str]:
        def _exec():
            with self.conn.cursor() as cur:
                cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                return [row[0] for row in cur.fetchall()]
        return await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def _list_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        def _exec():
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
            params = []
            if schema:
                query += " AND table_schema = %s"
                params.append(schema)
            query += " ORDER BY table_schema, table_name"
            
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]
        return await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def _describe_table(self, schema: str, table: str) -> List[Dict[str, Any]]:
        def _exec():
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (schema, table))
                return [{"name": row[0], "type": row[1], "nullable": row[2] == "YES", "default": row[3]} for row in cur.fetchall()]
        return await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def _execute_query(self, sql: str) -> Dict[str, Any]:
        def _exec():
            try:
                sql_clean = sql.strip()
                if not sql_clean.lower().startswith("select"):
                    raise ValueError("Only SELECT queries allowed")
                
                if "limit" not in sql_clean.lower():
                    sql_clean = f"{sql_clean.rstrip(';')} LIMIT {self.max_rows}"
                
                with self.conn.cursor() as cur:
                    cur.execute(sql_clean)
                    columns = [desc.name for desc in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    return {"columns": columns, "rows": [list(row) for row in rows], "row_count": len(rows)}
            except Exception as e:
                # Rollback transaction on error
                try:
                    self.conn.rollback()
                except:
                    pass
                raise e
        return await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def run(self):
        try:
            async with stdio_server() as (read_stream, write_stream):
                await self.server.run(read_stream, write_stream, self.server.create_initialization_options())
        except Exception as e:
            logger.error(f"Server error: {e}")
            # Test connection when run directly
            await self._test_connection()

    async def _test_connection(self):
        try:
            await self._ensure_connection()
            schemas = await self._list_schemas()
            print(f"✓ Connected to database. Found {len(schemas)} schemas: {schemas[:5]}")
        except Exception as e:
            print(f"✗ Database connection failed: {e}")

async def main():
    parser = argparse.ArgumentParser(description="PostgreSQL MCP Server")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--max-rows", type=int, default=100)
    parser.add_argument("--test", action="store_true", help="Test connection only")
    
    args = parser.parse_args()
    dsn = f"host={args.host} port={args.port} dbname={args.database} user={args.username} password={args.password}"
    
    server = PostgreSQLMCPServer(dsn, args.max_rows)
    
    if args.test:
        await server._test_connection()
    else:
        print("MCP Server starting... (use --test to test connection)", file=sys.stderr)
        await server.run()

if __name__ == "__main__":
    asyncio.run(main())