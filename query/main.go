package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type QueryResult struct {
	ID        string `json:"id"`
	TenantId  string `json:"tenantId"`
	UserId    string `json:"userId"`
	SessionId string `json:"sessionId"`
	Activity  string `json:"activity"`
	Timestamp string `json:"timestamp"`
}

var container *azcosmos.ContainerClient

func init() {
	endpoint := os.Getenv("COSMOS_DB_ENDPOINT")
	if endpoint == "" {
		log.Fatal("COSMOS_DB_ENDPOINT is not set")
	}

	dbName := os.Getenv("COSMOS_DB_DATABASE_NAME")
	if dbName == "" {
		log.Fatal("COSMOS_DB_DATABASE_NAME is not set")
	}

	containerName := os.Getenv("COSMOS_DB_CONTAINER_NAME")
	if containerName == "" {
		log.Fatal("COSMOS_DB_CONTAINER_NAME is not set")
	}

	_, err := getClient(endpoint)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Query with a full partition key
	tenantID := "MidMarket-Inc"
	userID := "user-192"
	sessionID := "session-5af6ab47"
	queryWithFullPartitionKey(tenantID, userID, sessionID)

	// Query with a partial partition key
	_tenantID := "LocalShops-SME"
	_userID := "user-42"
	// partial key
	queryWithTenantAndUserID(_tenantID, _userID)

	// Query with a single partition key parameter
	queryWithSinglePKParameter("tenantId", "Enterprise-Corp")
	queryWithSinglePKParameter("userId", "user-42")
	queryWithSinglePKParameter("sessionId", "session-0361ef4c")

	// Query/Execute a point read operation
	tenantID_ := "SmallBiz-LLC"
	userID_ := "user-42"
	sessionID_ := "session-0361ef4c"
	id := "c0ba6ff6-a622-4b30-bcd3-b92960336976" // This should be the ID of the item you want to read
	executePointRead(id, tenantID_, userID_, sessionID_)
}

// queryWithFullPartitionKey let`s you user the full partition key for querying
func queryWithFullPartitionKey(tenantID, userID, sessionID string) {
	query := "SELECT * FROM c WHERE c.tenantId = @tenantId AND c.userId = @userId AND c.sessionId = @sessionId"

	pkFull := azcosmos.NewPartitionKeyString(tenantID).AppendString(userID).AppendString(sessionID)

	pager := container.NewQueryItemsPager(query, pkFull, &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@tenantId", Value: tenantID},
			{Name: "@userId", Value: userID},
			{Name: "@sessionId", Value: sessionID},
		},
	})

	fmt.Println("Querying with full partition key:", pkFull)

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		for _, _item := range page.Items {
			var queryResult QueryResult
			err = json.Unmarshal(_item, &queryResult)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("ID", queryResult.ID)
			fmt.Println("Activity", queryResult.Activity)
			fmt.Println("Timestamp", queryResult.Timestamp)

			fmt.Println("RUs consumed", page.RequestCharge)
		}
	}
}

// queryWithTenantAndUserID lets you query with partial key, tenantId and userId
func queryWithTenantAndUserID(tenantID, userID string) {
	query := "SELECT * FROM c WHERE c.tenantId = @tenantId AND c.userId = @userId"

	// since we don't have the full partition key, we use an empty partition key
	emptyPartitionKey := azcosmos.NewPartitionKey()

	pager := container.NewQueryItemsPager(query, emptyPartitionKey, &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@tenantId", Value: tenantID},
			{Name: "@userId", Value: userID},
		},
	})
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Results for tenantId:", tenantID, "and userId:", userID)
		fmt.Println("==========================================")

		for _, _item := range page.Items {
			var queryResult QueryResult
			err = json.Unmarshal(_item, &queryResult)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Session ID:", queryResult.SessionId)
			fmt.Println("Activity:", queryResult.Activity)
			fmt.Println("Timestamp:", queryResult.Timestamp)

			fmt.Println("RUs consumed:", page.RequestCharge)

			fmt.Println("==========================================")
		}
	}
}

func queryWithSinglePKParameter(paramType, paramValue string) {
	if paramType != "tenantId" && paramType != "userId" && paramType != "sessionId" {
		log.Fatalf("Invalid parameter type: %s", paramType)
	}

	query := fmt.Sprintf("SELECT * FROM c WHERE c.%s = @param", paramType)
	emptyPartitionKey := azcosmos.NewPartitionKey()

	pager := container.NewQueryItemsPager(query, emptyPartitionKey, &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@param", Value: paramValue},
		},
	})

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Results for %s: %s\n", paramType, paramValue)
		fmt.Println("==========================================")

		for _, _item := range page.Items {
			var queryResult QueryResult
			err = json.Unmarshal(_item, &queryResult)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("ID:", queryResult.ID)
			fmt.Println("Tenant ID:", queryResult.TenantId)
			fmt.Println("User ID:", queryResult.UserId)
			fmt.Println("Session ID:", queryResult.SessionId)
			fmt.Println("Activity:", queryResult.Activity)
			fmt.Println("Timestamp:", queryResult.Timestamp)

			fmt.Println("RUs consumed:", page.RequestCharge)

			fmt.Println("==========================================")
		}
	}
}

func executePointRead(id, tenantId, userId, sessionId string) {
	// create a partition key using the full partition key values
	pk := azcosmos.NewPartitionKeyString(tenantId).AppendString(userId).AppendString(sessionId)

	// perform a point read operation
	resp, err := container.ReadItem(context.Background(), pk, id, nil)
	if err != nil {
		log.Fatalf("Failed to read item: %v", err)
	}

	var queryResult QueryResult
	err = json.Unmarshal(resp.Value, &queryResult)
	if err != nil {
		log.Fatalf("Failed to unmarshal response: %v", err)
	}

	fmt.Println("Point Read Result for:", id, tenantId, userId, sessionId)

	fmt.Println("Activity:", queryResult.Activity)
	fmt.Println("Timestamp:", queryResult.Timestamp)

	fmt.Println("RUs consumed:", resp.RequestCharge)
}

func getClient(endpoint string) (*azcosmos.Client, error) {
	creds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azcosmos.NewClient(endpoint, creds, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}
