# Oracle Graph Server (PGX) Setup Guide

## One-Time Setup

### 1. Create Oracle Account
- Go to https://profile.oracle.com/
- Create a free account
- Verify your email

### 2. Login to Container Registry
```bash
docker login container-registry.oracle.com
# Username: your-oracle-email@example.com
# Password: your-oracle-password
```

### 3. Accept License
- Visit https://container-registry.oracle.com/
- Search for "graph-server"
- Click "database/graph-server"
- Accept Oracle Standard Terms and Restrictions

## Running the Application

### Start All Services
```bash
docker compose up -d
```

### Check Status
```bash
# View all services
docker compose ps

# View logs
docker compose logs -f

# Check specific service
docker compose logs graph-server
```

### Access Points
- **Oracle Database**: localhost:1521 (service: FREE)
- **PGX Server REST API**: http://localhost:7007
- **PGX Graph Studio UI**: http://localhost:7007/ui/

### Verify PGX is Running
```bash
# Check version
curl http://localhost:7007/version

# Check health
docker compose ps graph-server
```

## Troubleshooting

### PGX Container Won't Start
```bash
# Check if you're logged in
docker login container-registry.oracle.com

# Check logs
docker compose logs graph-server

# Restart
docker compose restart graph-server
```

### License Error
Make sure you've accepted the license at:
https://container-registry.oracle.com/

### Connection Refused
Wait 30-60 seconds for PGX to fully start after Oracle DB is ready.

## Using PGX

### Via Python REST Client
```python
from pgx_rest_client import PGXRestClient

pgx = PGXRestClient("http://localhost:7007")
version = pgx.get_version()
print(version)
```

### Via Graph Studio UI
1. Open http://localhost:7007/ui/
2. Login with Oracle DB credentials:
   - Username: graphuser
   - Password: GraphPassword123
3. Create graphs, run queries, visualize

### Via PGQL Queries
```python
# Load graph into PGX
pgx.load_graph_from_database(
    graph_name="pest_graph",
    jdbc_url="jdbc:oracle:thin:@oracle-db:1521/FREE",
    username="graphuser",
    password="GraphPassword123"
)

# Run PageRank
result = pgx.run_pagerank("pest_graph")

# Execute PGQL
result = pgx.execute_pgql(
    "pest_graph",
    "SELECT v.entity_id FROM MATCH (v:entity) LIMIT 10"
)
```

## Stopping Services

```bash
# Stop all
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```

## Resources

- [Oracle Graph Server Docs](https://docs.oracle.com/en/database/oracle/property-graph/)
- [PGX REST API Reference](https://docs.oracle.com/en/database/oracle/property-graph/24.4/spgdg/using-graph-server-pgx.html)
- [PGQL Language Reference](https://pgql-lang.org/)
