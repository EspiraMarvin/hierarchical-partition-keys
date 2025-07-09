#### HIERARCHICAL PARTITION KEYS IN GO. Database partitioning with Azure CosmosDB (equivalent of AWS DynamoDB & GCP Firestore)

##### USE CASES

1. > Multitenant architecture application
2. > Sharded systems (using a combination of both range based sharding and consistent hashing algorithm to achieve it the sharding)

##### General rule of thumb:

When using hierarchical partition keys, orders of keys should be structure based on their CARDINALITY (the no. of distinct values) and ACCESS PATTERNS

1. > Lowest cardinality key should come first - lesser unique values
2. > Highest cardinality key should come last - more unique values

##### Query Patterns Covered

1. > Point Read (most efficient)
2. > Session specific data
3. > User-Specific Data (Targeted Cross-Partition)
4. > Tenant-Wide Data (Efficient Cross-Partition)
5. > User or Session Across All Tenants (Fan-Out)

##### Run application

```bash
export COSMOS_DB_ENDPOINT=https://your-account.documents.azure.com:443/
export COSMOS_DB_DATABASE_NAME=<insert database name>
export COSMOS_DB_CONTAINER_NAME=<insert container name>

cd cosmosdb-go-hierarchical-partition-keys/query
go run main.go
```
