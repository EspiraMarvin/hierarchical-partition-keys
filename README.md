#### HEIRARCHICAL PARTITION KEYS IN GO. Database partitioning with Azure CosmosDB (equivalent of AWS DynamoDB & GCP Firestore)

##### USECASES

1. > Multitenant architecture application
2. > Sharded systems (using a combination of both range based and hash algorithm sharding)

##### General rule of thumb:

When using hierarchical partition keys, orders of keys should be structure based on their CARDINALITY (the no. of distinct values they have) and ACCESS PATTERNS

1. > Highest cardinality key should come first
2. > Lowest cardinality key should come last
