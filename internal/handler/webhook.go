package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"metaflow_manager/internal/workflow"
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
		Merged bool `json:"merged"`
		Head   struct {
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

// verifyConvoySignature verifies X-Convoy-Signature per Convoy docs.
// Simple: X-Convoy-Signature: <hex-hash>  → HMAC-SHA256(secret, payload)
// Advanced: t=<unix>,v1=<hash>,v0=<hash>   → HMAC-SHA256(secret, "{timestamp},{payload}")
// Tries hex first, then base64 (Convoy project encoding may vary).
func verifyConvoySignature(secret, payload []byte, signature string, encoding string) bool {
	if secret == nil || len(secret) == 0 {
		return true
	}
	for _, enc := range []string{encoding, "hex", "base64"} {
		if enc == "" {
			continue
		}
		if verifyWithEncoding(secret, payload, signature, enc) {
			return true
		}
	}
	return false
}

func verifyWithEncoding(secret, payload []byte, signature string, encoding string) bool {
	decodeSig := func(s string) ([]byte, bool) {
		s = strings.TrimSpace(s)
		if encoding == "base64" {
			b, err := base64.StdEncoding.DecodeString(s)
			return b, err == nil
		}
		b, err := hex.DecodeString(s)
		return b, err == nil
	}

	computeHMAC := func(data []byte) []byte {
		mac := hmac.New(sha256.New, secret)
		mac.Write(data)
		return mac.Sum(nil)
	}

	parts := strings.Split(signature, ",")
	if len(parts) > 1 {
		// Advanced format: t=1492774577,v1=...,v0=...
		var timestamp string
		var sigHashes []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if strings.HasPrefix(p, "t=") {
				timestamp = strings.TrimPrefix(p, "t=")
			} else if idx := strings.Index(p, "="); idx > 0 && (p[0] == 'v' || strings.HasPrefix(p, "v")) {
				sigHashes = append(sigHashes, p[idx+1:])
			}
		}
		if timestamp != "" && len(sigHashes) > 0 {
			signedPayload := []byte(timestamp + "," + string(payload))
			expected := computeHMAC(signedPayload)
			for _, sh := range sigHashes {
				decoded, ok := decodeSig(sh)
				if ok && hmac.Equal(decoded, expected) {
					return true
				}
			}
		}
		return false
	}

	// Simple format: single hash
	decoded, ok := decodeSig(signature)
	if !ok {
		return false
	}
	expected := computeHMAC(payload)
	return hmac.Equal(decoded, expected)
}

// WorkflowStarter is a minimal interface for starting workflows (allows mocking in tests).
type WorkflowStarter interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
}

// GitHubWebhookHandler handles POST /webhooks/github (GitHub or Convoy delivery)
func GitHubWebhookHandler(temporalClient WorkflowStarter) http.HandlerFunc {
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
			encoding := os.Getenv("CONVOY_SIGNATURE_ENCODING")
			if encoding == "" {
				encoding = "hex"
			}
			if sig == "" {
				sig = r.Header.Get("X-Hub-Signature-256")
				if len(sig) >= 7 && strings.HasPrefix(sig, "sha256=") {
					sig = strings.TrimPrefix(sig, "sha256=")
				}
			}
			if sig == "" || !verifyConvoySignature([]byte(secret), body, sig, encoding) {
				log.Printf("Webhook signature verification failed (sig=%q)", sig)
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

		// Convoy may not forward X-GitHub-Event; infer from payload when empty
		if eventType == "" {
			eventType = inferEventTypeFromPayload(body)
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
			if mode, ok := buildModeFromPullRequest(pr); ok {
				buildMode = mode
			} else {
				WriteJSON(w, http.StatusAccepted, map[string]string{"status": "ignored", "action": pr.Action})
				return
			}
		} else if eventType == "push" {
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

		// Unique workflow ID per branch+commit (avoids conflict when multiple triggers)
		shaShort := req.GitHubSHA
		if len(shaShort) > 7 {
			shaShort = shaShort[:7]
		}
		branchSafe := strings.ReplaceAll(req.Branch, "/", "-")
		workflowID := "ci-" + strings.ReplaceAll(req.ServiceName, "/", "-") + "-" + branchSafe + "-" + shaShort
		options := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: "ci-task-queue",
		}

		we, err := temporalClient.ExecuteWorkflow(context.Background(), options, workflow.ManagerWorkflowType, req)
		if err != nil {
			log.Printf("ExecuteWorkflow error: %v", err)
			WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		log.Printf("Workflow started from webhook: %s (event=%s, buildMode=%s)", we.GetID(), eventType, buildMode)
		WriteJSON(w, http.StatusAccepted, TriggerResponse{
			WorkflowID: we.GetID(),
			RunID:     we.GetRunID(),
		})
	}
}

// inferEventTypeFromPayload detects GitHub event type when X-GitHub-Event header is missing (e.g. Convoy).
func inferEventTypeFromPayload(body []byte) string {
	var m map[string]json.RawMessage
	if json.Unmarshal(body, &m) != nil {
		return ""
	}
	if _, ok := m["pull_request"]; ok {
		if _, hasAction := m["action"]; hasAction {
			return "pull_request"
		}
	}
	if _, hasRef := m["ref"]; hasRef {
		if _, hasAfter := m["after"]; hasAfter {
			return "push"
		}
	}
	return ""
}

func readBody(r *http.Request) ([]byte, error) {
	const maxBody = 1 << 20
	r.Body = http.MaxBytesReader(nil, r.Body, maxBody)
	return io.ReadAll(r.Body)
}

// buildModeFromPullRequest returns ("ci"|"cd", true) if action should trigger, else ("", false).
func buildModeFromPullRequest(pr GitHubPullRequestPayload) (string, bool) {
	switch {
	case pr.Action == "opened" || pr.Action == "synchronize":
		return "ci", true
	case pr.Action == "closed" && pr.PullRequest.Merged:
		return "cd", true
	default:
		return "", false
	}
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
