package handler

import (
	"context"

	"metaflow_manager/internal/workflow"
	"go.temporal.io/sdk/client"
)

// BuildModeAwareWorkflowStarter routes workflow execution to the appropriate Temporal client
// based on BuildMode: ci -> metaflow-ci namespace, cd -> metaflow-cd namespace.
type BuildModeAwareWorkflowStarter struct {
	CIClient WorkflowStarter
	CDClient WorkflowStarter
}

// ExecuteWorkflow extracts BuildMode from args (PipelineRequest) and delegates to the matching client.
func (b *BuildModeAwareWorkflowStarter) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, wf interface{}, args ...interface{}) (client.WorkflowRun, error) {
	buildMode := "ci"
	if len(args) >= 1 {
		if pr, ok := args[0].(workflow.PipelineRequest); ok && pr.BuildMode != "" {
			buildMode = pr.BuildMode
		}
	}
	if buildMode == "cd" {
		return b.CDClient.ExecuteWorkflow(ctx, options, wf, args...)
	}
	return b.CIClient.ExecuteWorkflow(ctx, options, wf, args...)
}
