package main

import "time"

// user session data model with heirarchical partition keys
// key/column/field with highest cardinality comes first/level 1 as the
type UsersSession struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenantId"`  // level 1: Tenant Isolation
	UserID    string    `json:"userId"`    // level 2: User distribution
	SessionID string    `json:"sessionId"` // level 3: session granularity
	Activity  string    `json:"activity"`
	Timestamp time.Time `json:"timestamp"`
}

// configuration for Azure Cosmos DB connection
type Config struct {
	Endpoint      string
	DatabaseName  string
	ContainerName string
	RowCount      int
}
