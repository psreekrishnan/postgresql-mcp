# PostgreSQL MCP Server

MCP-compliant server for PostgreSQL database integration with Amazon Q.

## Installation

```bash
pip install -r requirements.txt
```

## Setup

1. **Test connection:**
```bash
python server.py --host host.example.com --port 61025 --database example --username example_user --password example_pwd --test
```

2. **Configure Amazon Q MCP:**

Update `C:\Users\%USERNAME%\.aws\amazonq\mcp.json`:

```json
{
  "mcpServers": {
    "local-postgres-mcp": {
      "command": "C:\\Users\\sk\\AppData\\Local\\Programs\\Python\\Python313\\python.exe",
      "args": [
        "C:\\Workspace\\GitLab\\capacity-planning2\\postgresql-mcp\\server.py",
        "--host", "host.example.com",
        "--port", "61025",
        "--database", "example",
        "--username", "example_user",
        "--password", "example_pwd"
      ],
      "env": {"PYTHONUNBUFFERED": "1"},
      "timeout": 60000
    }
  }
}
```

3. **Restart IntelliJ IDEA** to load the new MCP server

## Using with Amazon Q Chat

Once configured, you can use these commands in Amazon Q chat:

**Query database:**
```
Query the database: SELECT * FROM "capacity_planning2".ports LIMIT 5
```

**List tables:**
```
List all tables in the capacity_planning2 schema
```

**Describe table structure:**
```
Describe the structure of the ports table in capacity_planning2 schema
```

**Browse schemas:**
```
Show me all available database schemas
```

## Available Tools

- **query_database**: Execute SELECT queries with automatic LIMIT
- **describe_table**: Get detailed table column information
- **list_tables**: List tables in specific schema or all schemas

## Security

- Read-only access (SELECT queries only)
- Automatic query limits (100 rows max)
- Connection timeout protection
