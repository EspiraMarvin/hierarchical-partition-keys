package main

type QueryResult struct {
	ID        string `json:"id"`
	TenantId  string `json:"tenantId"`
	UserId    string `json:"userId"`
	SessionId string `json:"sessionId"`
	Activity  string `json:"activity"`
	Timestamp string `json:"timestamp"`
}
