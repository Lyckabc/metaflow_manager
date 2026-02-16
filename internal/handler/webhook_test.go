package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"metaflow_manager/internal/workflow"
	"go.temporal.io/sdk/client"
)

// mockWorkflowRun implements client.WorkflowRun for testing.
type mockWorkflowRun struct {
	id    string
	runID string
}

func (m *mockWorkflowRun) GetID() string    { return m.id }
func (m *mockWorkflowRun) GetRunID() string { return m.runID }
func (m *mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}
func (m *mockWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}

// mockWorkflowStarter implements WorkflowStarter for testing.
type mockWorkflowStarter struct {
	lastReq workflow.PipelineRequest
}

func (m *mockWorkflowStarter) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, wf interface{}, args ...interface{}) (client.WorkflowRun, error) {
	if len(args) > 0 {
		if req, ok := args[0].(workflow.PipelineRequest); ok {
			m.lastReq = req
		}
	}
	return &mockWorkflowRun{id: "test-wf", runID: "test-run"}, nil
}

func TestExtractOwner(t *testing.T) {
	tests := []struct {
		fullName string
		want     string
	}{
		{"owner/repo", "owner"},
		{"org/sub/repo", "org"},
		{"single", "single"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractOwner(tt.fullName)
		if got != tt.want {
			t.Errorf("extractOwner(%q) = %q, want %q", tt.fullName, got, tt.want)
		}
	}
}

func TestExtractRepo(t *testing.T) {
	tests := []struct {
		fullName string
		want     string
	}{
		{"owner/repo", "repo"},
		{"org/sub", "sub"},
		{"single", "single"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractRepo(tt.fullName)
		if got != tt.want {
			t.Errorf("extractRepo(%q) = %q, want %q", tt.fullName, got, tt.want)
		}
	}
}

func TestBuildModeFromPullRequest(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		merged   bool
		wantMode string
		wantOK   bool
	}{
		{"synchronize -> ci", "synchronize", false, "ci", true},
		{"opened -> ci", "opened", false, "ci", true},
		{"closed+merged -> cd", "closed", true, "cd", true},
		{"closed+not merged -> ignore", "closed", false, "", false},
		{"reopened -> ignore", "reopened", false, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := GitHubPullRequestPayload{
				Action: tt.action,
				PullRequest: struct {
					Merged bool `json:"merged"`
					Head   struct {
						Ref  string `json:"ref"`
						SHA  string `json:"sha"`
					} `json:"head"`
					Base struct {
						Ref string `json:"ref"`
					} `json:"base"`
				}{
					Merged: tt.merged,
				},
			}
			gotMode, gotOK := buildModeFromPullRequest(pr)
			if gotMode != tt.wantMode || gotOK != tt.wantOK {
				t.Errorf("buildModeFromPullRequest() = (%q, %v), want (%q, %v)", gotMode, gotOK, tt.wantMode, tt.wantOK)
			}
		})
	}
}

func TestWebhookPullRequestBuildMode(t *testing.T) {
	// No webhook secret for simpler test
	os.Setenv("CONVOY_ENDPOINT_SECRET", "")
	os.Setenv("GITHUB_WEBHOOK_SECRET", "")
	defer func() {
		os.Unsetenv("CONVOY_ENDPOINT_SECRET")
		os.Unsetenv("GITHUB_WEBHOOK_SECRET")
	}()

	mock := &mockWorkflowStarter{}
	handler := GitHubWebhookHandler(mock)

	tests := []struct {
		name           string
		payload        string
		wantStatus     int
		wantMode       string
		noEventHeader  bool // simulate Convoy not forwarding X-GitHub-Event
	}{
		{
			name: "synchronize -> ci",
			payload: `{"action":"synchronize","number":4,"pull_request":{"merged":false,"head":{"ref":"feature","sha":"abc123"},"base":{"ref":"main"}},"repository":{"full_name":"owner/repo","clone_url":"https://github.com/owner/repo.git"}}`,
			wantStatus: http.StatusAccepted,
			wantMode:   "ci",
		},
		{
			name: "closed+merged -> cd",
			payload: `{"action":"closed","number":3,"pull_request":{"merged":true,"head":{"ref":"feature","sha":"abc123"},"base":{"ref":"main"}},"repository":{"full_name":"owner/repo","clone_url":"https://github.com/owner/repo.git"}}`,
			wantStatus: http.StatusAccepted,
			wantMode:   "cd",
		},
		{
			name: "closed+not merged -> ignored",
			payload: `{"action":"closed","number":3,"pull_request":{"merged":false,"head":{"ref":"feature","sha":"abc123"},"base":{"ref":"main"}},"repository":{"full_name":"owner/repo","clone_url":"https://github.com/owner/repo.git"}}`,
			wantStatus: http.StatusAccepted,
			wantMode:   "",
		},
		{
			name:       "synchronize without X-GitHub-Event (Convoy) -> ci",
			payload:    `{"action":"synchronize","number":1,"pull_request":{"merged":false,"head":{"ref":"feature","sha":"abc123"},"base":{"ref":"main"}},"repository":{"full_name":"Lyckabc/metaflow_manager","clone_url":"https://github.com/Lyckabc/metaflow_manager.git"}}`,
			wantStatus: http.StatusAccepted,
			wantMode:   "ci",
			noEventHeader: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(tt.payload))
			if !tt.noEventHeader {
				req.Header.Set("X-GitHub-Event", "pull_request")
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantMode != "" && mock.lastReq.BuildMode != tt.wantMode {
				t.Errorf("BuildMode = %q, want %q", mock.lastReq.BuildMode, tt.wantMode)
			}
		})
	}
}

func TestWebhookPushBuildMode(t *testing.T) {
	os.Setenv("CONVOY_ENDPOINT_SECRET", "")
	os.Setenv("GITHUB_WEBHOOK_SECRET", "")
	defer func() {
		os.Unsetenv("CONVOY_ENDPOINT_SECRET")
		os.Unsetenv("GITHUB_WEBHOOK_SECRET")
	}()

	mock := &mockWorkflowStarter{}
	handler := GitHubWebhookHandler(mock)

	payload := `{"ref":"refs/heads/main","repository":{"full_name":"owner/repo","clone_url":"https://github.com/owner/repo.git"},"after":"a1b2c3d"}`
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(payload))
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}
	if mock.lastReq.BuildMode != "cd" {
		t.Errorf("BuildMode = %q, want cd", mock.lastReq.BuildMode)
	}
}

func TestWebhookMethodNotAllowed(t *testing.T) {
	mock := &mockWorkflowStarter{}
	handler := GitHubWebhookHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/webhooks/github", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}
