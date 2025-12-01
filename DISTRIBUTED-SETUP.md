# Distributed Database Setup Guide

## Architecture Overview

### Master-Slave with Horizontal Partitioning

- **Main Node** (Port 60751): Master node with complete database
  - Database: `stadvdb-mco2`
  - Contains all ~9.7M rows
  - Handles distributed operations routing

- **Node A** (Port 60752): Slave node for modern titles
  - Database: `stadvdb-mco2-a`
  - Contains titles with `startYear >= 2010`
  - Has federated access to Node B's data

- **Node B** (Port 60753): Slave node for classic titles
  - Database: `stadvdb-mco2-b`
  - Contains titles with `startYear < 2010`
  - Has federated access to Node A's data

## Setup Steps

### Step 1: Set up Node A Fragment
Run on Node A (stadvdb-mco2-a):
```bash
mysql -h ccscloud.dlsu.edu.ph -P 60752 -u g18 -p < Modifications/1-setup-node-a-fragment.sql
```

This will:
- Create `title_ft` table with startYear >= 2010 data
- Create federated `title_ft_node_b` pointing to Node B
- Create `title_ft_all` view for unified access

### Step 2: Set up Node B Fragment
Run on Node B (stadvdb-mco2-b):
```bash
mysql -h ccscloud.dlsu.edu.ph -P 60753 -u g18 -p < Modifications/1-setup-node-b-fragment.sql
```

This will:
- Create `title_ft` table with startYear < 2010 data
- Create federated `title_ft_node_a` pointing to Node A
- Create `title_ft_all` view for unified access

### Step 3: Update Main Node Procedures
Run on Main Node (stadvdb-mco2):
```bash
mysql -h ccscloud.dlsu.edu.ph -P 60751 -u g18 -p < Modifications/2-main-distributed-procedures.sql
```

This will create procedures that:
- Route INSERT to appropriate node based on startYear
- Handle UPDATE with fragment migration
- Propagate DELETE to correct node
- Execute SELECT/SEARCH across all nodes

### Step 4: Update Node A and B Procedures
The existing `a-modifiers.sql` and `b-modifiers.sql` should work as they delegate to Main.

### Step 5: Start Backend Servers
```bash
cd stadvdb-web/backend
npm start
```

This starts 3 backend servers:
- Port 60751: Connects to Main database
- Port 60752: Connects to Node A database
- Port 60753: Connects to Node B database

## How It Works

### Write Operations (INSERT/UPDATE/DELETE)
1. Client sends request to any backend server
2. Request is routed to Main node
3. Main node's stored procedure:
   - Writes to Main database (master copy)
   - Routes to Node A if startYear >= 2010
   - Routes to Node B if startYear < 2010
4. Both nodes are kept in sync

### Read Operations (SELECT/SEARCH)
1. Client sends request to any backend server
2. Request can be served by:
   - **Main node**: Has all data
   - **Node A**: Has >= 2010 data + federated access to Node B
   - **Node B**: Has < 2010 data + federated access to Node A
3. Distributed queries use UNION across nodes

### Load Balancing
- Frontend can connect to any of the 3 backend servers (60751, 60752, 60753)
- Each backend can handle requests independently
- Reads can be distributed across nodes
- Writes always go through Main node for consistency

## Benefits

1. **Horizontal Scalability**: Data is partitioned across nodes
2. **High Availability**: Multiple nodes can serve requests
3. **Load Distribution**: Reads can be distributed
4. **Data Locality**: Recent movies (>= 2010) on Node A, classics on Node B
5. **Consistency**: Main node ensures all writes are coordinated

## Frontend Configuration

Update `stadvdb-web/.env`:
```
REACT_APP_API_URL=http://localhost:60751
```

Or use a load balancer to distribute across all three ports.
