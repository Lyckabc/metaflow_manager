package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"metaflow_manager/internal/workflow"
	"go.temporal.io/sdk/client"
)

// TriggerRequest is the JSON body for POST /trigger
type TriggerRequest struct {
	ServiceName string `json:"service_name"`
	RepoURL     string `json:"repo_url"`
	Branch      string `json:"branch"`
	BuildMode   string `json:"build_mode"`
}

// TriggerResponse is the JSON response for POST /trigger
type TriggerResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID     string `json:"run_id"`
}

// TriggerHandler starts ManagerWorkflow via Temporal client
func TriggerHandler(temporalClient WorkflowStarter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		var req TriggerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
			return
		}

		if req.ServiceName == "" {
			req.ServiceName = extractServiceNameFromRepo(req.RepoURL)
		}
		if req.RepoURL == "" {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "repo_url required"})
			return
		}
		if req.Branch == "" {
			req.Branch = "main"
		}
		if req.BuildMode == "" {
			req.BuildMode = "ci"
		}

		workflowID := "ci-" + strings.ReplaceAll(req.ServiceName, "/", "-")
		options := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: "ci-task-queue",
		}
		args := workflow.PipelineRequest{
			ServiceName:        req.ServiceName,
			RepoURL:            req.RepoURL,
			Branch:             req.Branch,
			BuildMode:          req.BuildMode,
			TemporalUIBaseURL:  os.Getenv("TEMPORAL_UI_BASE_URL"),
		}

		we, err := temporalClient.ExecuteWorkflow(context.Background(), options, workflow.ManagerWorkflowType, args)
		if err != nil {
			log.Printf("ExecuteWorkflow error: %v", err)
			WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		log.Printf("Workflow started: %s", we.GetID())
		WriteJSON(w, http.StatusAccepted, TriggerResponse{
			WorkflowID: we.GetID(),
			RunID:     we.GetRunID(),
		})
	}
}

func extractServiceNameFromRepo(repoURL string) string {
	// e.g. https://github.com/owner/repo -> owner/repo
	repoURL = strings.TrimSuffix(repoURL, ".git")
	parts := strings.Split(repoURL, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	return "unknown"
}
