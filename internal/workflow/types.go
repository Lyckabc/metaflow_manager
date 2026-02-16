// Package workflow provides types for Temporal workflow communication.
// These types must match metaflow_cicd/workflow for ExecuteWorkflow compatibility.
package workflow

// PipelineRequest is the input for ManagerWorkflow.
// Must match metaflow_cicd/workflow.PipelineRequest.
type PipelineRequest struct {
	ServiceName string
	RepoURL     string
	Branch      string
	BuildMode   string // "ci" (PR opened) or "cd" (PR merged)

	// GitHub metadata for commit status updates (Branch Protection)
	GitHubOwner       string
	GitHubRepo        string
	GitHubSHA         string
	TemporalUIBaseURL string
}

// RunResult is the output of ManagerWorkflow.
// Must match metaflow_cicd/workflow.RunResult.
type RunResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Success  bool
}

// ManagerWorkflowType is the Temporal workflow type name (registered in metaflow_cicd worker).
const ManagerWorkflowType = "ManagerWorkflow"
