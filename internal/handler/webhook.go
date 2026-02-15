package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/neunexus/metaflow_cicd/workflow"
	"go.temporal.io/sdk/client"
)

// GitHubPushPayload for push events
type GitHubPushPayload struct {
	Ref    string `json:"ref"`
	Repo   struct {
		FullName  string `json:"full_name"`
		CloneURL  string `json:"clone_url"`
	} `json:"repository"`
	After string `json:"after"` // commit SHA
}

// GitHubPullRequestPayload for pull_request events
type GitHubPullRequestPayload struct {
	Action string `json:"action"`
	PullRequest struct {
		Head struct {
			Ref  string `json:"ref"`
			SHA  string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository struct {
		FullName  string `json:"full_name"`
		CloneURL  string `json:"clone_url"`
	} `json:"repository"`
}

// ConvoyEvent wraps Convoy webhook delivery (Convoy may wrap GitHub payload)
type ConvoyEvent struct {
	Data struct {
		Raw string `json:"raw"`
	} `json:"data"`
	Event struct {
		Data json.RawMessage `json:"data"`
	} `json:"event"`
}

func verifyConvoySignature(secret, payload []byte, signature string) bool {
	if secret == nil || len(secret) == 0 {
		return true
	}
	// Simple format: X-Convoy-Signature: <hash>
	// Advanced: t=...,v1=...
	parts := strings.Split(signature, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, "v1=") {
			p = strings.TrimPrefix(p, "v1=")
		} else if strings.HasPrefix(p, "t=") {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		mac.Write(payload)
		expected := hex.EncodeToString(mac.Sum(nil))
		if hmac.Equal([]byte(p), []byte(expected)) {
			return true
		}
	}
	// Fallback: entire header as single hash
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(signature), []byte(expected))
}

// GitHubWebhookHandler handles POST /webhooks/github (GitHub or Convoy delivery)
func GitHubWebhookHandler(temporalClient client.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		secret := os.Getenv("CONVOY_ENDPOINT_SECRET")
		if secret == "" {
			secret = os.Getenv("GITHUB_WEBHOOK_SECRET")
		}

		body, err := readBody(r)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "read body: " + err.Error()})
			return
		}

		if secret != "" {
			sig := r.Header.Get("X-Convoy-Signature")
			if sig == "" {
				sig = r.Header.Get("X-Hub-Signature-256")
				if len(sig) >= 7 && strings.HasPrefix(sig, "sha256=") {
					sig = strings.TrimPrefix(sig, "sha256=")
				}
			}
			if sig == "" || !verifyConvoySignature([]byte(secret), body, sig) {
				log.Printf("Webhook signature verification failed")
				WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "signature verification failed"})
				return
			}
		}

		eventType := r.Header.Get("X-GitHub-Event")
		var req workflow.PipelineRequest
		var buildMode string

		// Try Convoy-wrapped format first
		var convoy ConvoyEvent
		if json.Unmarshal(body, &convoy) == nil && len(convoy.Event.Data) > 0 {
			body = convoy.Event.Data
		}

		if eventType == "pull_request" {
			var pr GitHubPullRequestPayload
			if err := json.Unmarshal(body, &pr); err != nil {
				WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pull_request payload: " + err.Error()})
				return
			}
			req = workflow.PipelineRequest{
				ServiceName:        pr.Repository.FullName,
				RepoURL:            pr.Repository.CloneURL,
				Branch:             pr.PullRequest.Head.Ref,
				GitHubOwner:        extractOwner(pr.Repository.FullName),
				GitHubRepo:         extractRepo(pr.Repository.FullName),
				GitHubSHA:          pr.PullRequest.Head.SHA,
				TemporalUIBaseURL:  os.Getenv("TEMPORAL_UI_BASE_URL"),
			}
			if pr.Action == "opened" || pr.Action == "synchronize" {
				buildMode = "ci"
			} else if pr.Action == "closed" {
				buildMode = "cd"
			} else {
				WriteJSON(w, http.StatusAccepted, map[string]string{"status": "ignored", "action": pr.Action})
				return
			}
		} else if eventType == "push" || eventType == "" {
			var push GitHubPushPayload
			if err := json.Unmarshal(body, &push); err != nil {
				WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid push payload: " + err.Error()})
				return
			}
			branch := strings.TrimPrefix(push.Ref, "refs/heads/")
			req = workflow.PipelineRequest{
				ServiceName:        push.Repo.FullName,
				RepoURL:            push.Repo.CloneURL,
				Branch:             branch,
				GitHubOwner:        extractOwner(push.Repo.FullName),
				GitHubRepo:         extractRepo(push.Repo.FullName),
				GitHubSHA:          push.After,
				TemporalUIBaseURL:  os.Getenv("TEMPORAL_UI_BASE_URL"),
			}
			buildMode = "cd"
		} else {
			WriteJSON(w, http.StatusAccepted, map[string]string{"status": "ignored", "event": eventType})
			return
		}

		req.BuildMode = buildMode

		workflowID := "ci-" + strings.ReplaceAll(req.ServiceName, "/", "-")
		options := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: "ci-task-queue",
		}

		we, err := temporalClient.ExecuteWorkflow(context.Background(), options, workflow.ManagerWorkflow, req)
		if err != nil {
			log.Printf("ExecuteWorkflow error: %v", err)
			WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		log.Printf("Workflow started from webhook: %s", we.GetID())
		WriteJSON(w, http.StatusAccepted, TriggerResponse{
			WorkflowID: we.GetID(),
			RunID:     we.GetRunID(),
		})
	}
}

func readBody(r *http.Request) ([]byte, error) {
	const maxBody = 1 << 20
	r.Body = http.MaxBytesReader(nil, r.Body, maxBody)
	return io.ReadAll(r.Body)
}

func extractOwner(fullName string) string {
	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) >= 1 {
		return parts[0]
	}
	return ""
}

func extractRepo(fullName string) string {
	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) >= 2 {
		return parts[1]
	}
	return fullName
}
