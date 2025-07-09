package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
)

// user session data model with heirarchical partition keys
// key/column/field with highest cardinality comes first/level 1 as the
// sample partitioned keys /tenantId/userId/sessionId
type UserSession struct {
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

// sample tenant types with different characteristics
var tenantTypes = []struct {
	name     string
	userMin  int
	userMax  int
	sessions int
}{
	{"Global-Corp", 2000, 10000, 100},   // Very large enterprise
	{"Enterprise-Corp", 1000, 5000, 50}, // large enterprise
	{"MidMarket-Inc", 100, 500, 20},     // Mid-market company
	{"TechStartup-Co", 50, 200, 30},     // Growing startup
	{"LocalShops-SME", 10, 50, 5},       // Small business
}

// sample activities for realistic data generation
var activities = []string{
	"login",
	"logout",
	"view_dashboard",
	"create_document",
	"edit_document",
	"delete_document",
	"upload_file",
	"download_file",
	"send_message",
	"view_report",
	"export_data",
	"change_settings",
	"invite_user",
	"join_meeting",
	"schedule_event",
}

func main() {
	// parse command line flags
	var rowCount = flag.Int("rows", 10, "Number of rows to generate (default: 10)")
	var endpoint = flag.String("endpoint", "", "Azure Cosmos DB endpoint URL")
	var database = flag.String("database", "sampleDB", "Database name (default: sampleDB)")
	var container = flag.String("container", "UserSessions", "Container name (default: Usersessions)")
	flag.Parse()

	// get endpoint from env if not provided via flag
	endpointURL := *endpoint
	if endpointURL == "" {
		endpointURL = os.Getenv("COSMOS_ENDPOINT")
		if endpointURL == "" {
			log.Fatal("Please provide Azure Cosmos DB endpoint via -endpoint flag or COSMOS_ENDPOINT environment variable")
		}
	}

	config := Config{
		Endpoint:      endpointURL,
		DatabaseName:  *database,
		ContainerName: *container,
		RowCount:      *rowCount,
	}

	fmt.Printf("Starting data load with configuration:\n")
	fmt.Printf(" Endpoint: %s\n", config.Endpoint)
	fmt.Printf(" Database: %s\n", config.DatabaseName)
	fmt.Printf(" Container: %s\n", config.ContainerName)
	fmt.Printf(" Rows to generate: %d\n", config.RowCount)
	fmt.Println()

	// Initialize Azure Cosmos DB client
	client, err := createCosmosClient(config.Endpoint)
	if err != nil {
		log.Fatalf("Failed to create Cosmos DB client: %v", err)
	}

	// ensure database and container exists
	containerClient, err := ensureDatabaseAndContainer(client, config.DatabaseName, config.ContainerName)
	if err != nil {
		log.Fatalf("Failed to ensure database and container exist: %v", err)
	}

	// generate and load sample data
	err = loadSampleData(containerClient, config.RowCount)
	if err != nil {
		log.Fatalf("Failed to load sample data: %v", err)
	}

	fmt.Printf("Successfully loaded %d records into Azure Cosmos DB\n", config.RowCount)
}

// createCosmosClient creates and returns an Azrure Cosmos DB client
func createCosmosClient(endpoint string) (*azcosmos.Client, error) {

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}

	// create cosmos db client
	client, err := azcosmos.NewClient(endpoint, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return client, nil
}

// ensureDatabaseAndContainer creates the database and container if they don't exist
func ensureDatabaseAndContainer(client *azcosmos.Client, databaseName, containerName string) (*azcosmos.ContainerClient, error) {
	ctx := context.Background()

	fmt.Printf("Checking if database %s exists ...\n", databaseName)

	// create database if it doesn't exist
	databaseProperties := azcosmos.DatabaseProperties{
		ID: databaseName,
	}

	_, err := client.CreateDatabase(ctx, databaseProperties, nil)
	if err != nil {
		// check error incase of conflict with current state of resources // HTTP 409 error
		var respErr *azcore.ResponseError
		if !(errors.As(err, &respErr) && respErr.StatusCode == 409) {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
		fmt.Printf("Database %s alreadt exists\n", databaseName)
	} else {
		fmt.Printf("Created database %s\n", databaseName)
	}

	// get database client
	databaseClient, err := client.NewDatabase(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create database client: %w", err)
	}

	fmt.Printf("Checking if container %s exists...\n", containerName)

	// Define hierarchical partition key definition
	// this creates a 3-level hierarchy: /tennatId, /userId, /sessionId
	partitionKeyDef := azcosmos.PartitionKeyDefinition{
		Kind:    azcosmos.PartitionKeyKindMultiHash,
		Version: 2, //ver 2 is required for hierarchical partition keys
		Paths: []string{
			"/tenantId",  // Level 1: Tenant isolation
			"/userId",    // Level 2: User Distribution
			"/sessionId", // Level 3: Session granularity
		},
	}

	// create container properties
	containerProperties := azcosmos.ContainerProperties{
		ID:                     containerName,
		PartitionKeyDefinition: partitionKeyDef,
	}

	// create container with 400 RU/s throughput
	throughputProperties := azcosmos.NewManualThroughputProperties(400) // request unit/second

	_, err = databaseClient.CreateContainer(ctx, containerProperties, &azcosmos.CreateContainerOptions{
		ThroughputProperties: &throughputProperties,
	})
	if err != nil {
		// check if error is, because container already exists (HTTP 409 Conflict)
		var respErr *azcore.ResponseError
		if !(errors.As(err, &respErr) && respErr.StatusCode == 409) {
			return nil, fmt.Errorf("failed to create container: %w", err)
		}
		fmt.Printf("Container %s already exists\n", containerName)
	} else {
		fmt.Printf("Created container %s with heirarchical partition keys:\n", containerName)
		fmt.Printf(" Level 1:/ tenantId\n")
		fmt.Printf(" Level 2:/ userId\n")
		fmt.Printf(" Level 3:/ sessionId\n")
	}

	// get container client
	containerClient, err := databaseClient.NewContainer(containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create container client: %w", err)
	}

	return containerClient, nil
}

// loadSampleData generates and inserts sampler userSession records
func loadSampleData(containerClient *azcosmos.ContainerClient, rowCount int) error {
	ctx := context.Background()

	fmt.Printf("Generating %d sample records...\n", rowCount)

	successCount := 0
	errorCount := 0

	for i := range rowCount {
		// generate a sample UserSession record
		session := generateUserSession()

		//convert to json
		sessionJSON, err := json.Marshal(session)
		if err != nil {
			log.Printf("Failed to marshal session %d: %v", i+1, err)
			errorCount++
			continue
		}

		// create hierarchical partition key (TenantID, UserID, SessionID)
		partitionKey := azcosmos.NewPartitionKeyString(session.TenantID).AppendString(session.UserID).AppendString(session.SessionID)

		// insert the record using UpsertItem (insert or update if exists)
		_, err = containerClient.UpsertItem(ctx, partitionKey, sessionJSON, nil)
		if err != nil {
			log.Printf("Failed to insert session %d: %v", i+1, err)
			errorCount++
			continue
		}

		successCount++

		// progress indicator
		if (i+1)%10 == 0 || i+1 == rowCount {
			fmt.Printf(" Progress: %d/%d records processed\n", i+1, rowCount)
		}
	}

	fmt.Printf("\n📊 Load Summary:\n")
	fmt.Printf(" Successful inserts: %d\n", successCount)
	if errorCount > 0 {
		fmt.Printf(" Failed inserts: %d\n", errorCount)
		return fmt.Errorf("completed with %d errors out of %d total records", errorCount, rowCount)
	}
	return nil
}

// generateUserSession creates a realistic UserSessoin record with hierarchical partition key
func generateUserSession() UserSession {
	// select a random tenant type
	tenant := tenantTypes[rand.Intn(len(tenantTypes))]

	// generate user ID within the tenant's user range
	userNum := rand.Intn(tenant.userMax-tenant.userMin+1) + tenant.userMin
	userID := fmt.Sprintf("user-%d", userNum)

	// generate session id
	sessionID := fmt.Sprintf("session-%s", uuid.New().String()[:8]) // e.g output session-b08fa8a4

	// select random activity
	activity := activities[rand.Intn(len(activities))]

	// generate timestamp within the last 30 days
	now := time.Now()
	daysAgo := rand.Intn(30)
	hoursAgo := rand.Intn(24)
	minutesAgo := rand.Intn(60)
	timestamp := now.AddDate(0, 0, -daysAgo).Add(-time.Duration(hoursAgo) * time.Hour).Add(-time.Duration(minutesAgo) * time.Minute)

	return UserSession{
		ID:        uuid.NewString(),
		TenantID:  tenant.name,
		UserID:    userID,
		SessionID: sessionID,
		Activity:  activity,
		Timestamp: timestamp,
	}
}

func getEndpointFlagorEnv(flagName, envVar, usage string) string {
	flagValue := flag.String(flagName, "", usage)
	flag.Parse()

	if *flagValue != "" {
		return *flagValue
	}

	if envValue := os.Getenv(envVar); envValue != "" {
		return envValue
	}

	log.Fatal("Missing required endpoint. Provide it via -%s flag or %s environment variable.", flagName, envVar)
	return ""
}
