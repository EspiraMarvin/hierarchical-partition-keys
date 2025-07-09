package main

import (
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
	queryWithTenantAndUserID(_tenantID, _userID)
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
		page, err := page.NextPage(context.Background())
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

// queryWithTenantAndUserID lets you query with tenantId and userId
func queryWithTenantAndUserID(tenantID, userID string) {
	query := "SELECT * FROM c WHERE c.tenantId = @tenantId AND c.userId = @userId"

	// since we don't have the full partition key, we use an empty partition key
	emptyPartitionKey := azcosmos.NewPartitionKey()

	page := container.NewQueryItemsPager(query, emptyPartitionKey, &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@tenantId", Value: tenantID},
			{Name: "@userId", Value: userID},
		},
	})
	for page.More() {
		page, err := page.NextPage(context.Background())
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
