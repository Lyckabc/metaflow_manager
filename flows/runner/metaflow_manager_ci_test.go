// metaflow_manager_ci_test.go - Temporal SDK testsuite를 활용한 CI 테스트
package main

import (
	"context"
	"os"
	"testing"
	"time"

	wf "metaflow_manager/internal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"github.com/stretchr/testify/suite"
)

// mockWorkflowRunner implements WorkflowRunner for unit testing.
type mockWorkflowRunner struct {
	lastReq     wf.PipelineRequest
	lastOptions client.StartWorkflowOptions
	result      wf.RunResult
	err         error
}

func (m *mockWorkflowRunner) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	m.lastOptions = options
	if len(args) > 0 {
		if req, ok := args[0].(wf.PipelineRequest); ok {
			m.lastReq = req
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return &mockWorkflowRun{result: m.result}, nil
}

type mockWorkflowRun struct {
	result wf.RunResult
}

func (m *mockWorkflowRun) GetID() string    { return "test-workflow-id" }
func (m *mockWorkflowRun) GetRunID() string { return "test-run-id" }
func (m *mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	if v, ok := valuePtr.(*wf.RunResult); ok {
		*v = m.result
	}
	return nil
}
func (m *mockWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return m.Get(ctx, valuePtr)
}

// MockManagerWorkflow - ManagerWorkflow와 동일한 시그니처의 mock workflow (testsuite용).
func MockManagerWorkflow(ctx temporalworkflow.Context, req wf.PipelineRequest) (wf.RunResult, error) {
	return wf.RunResult{
		Success:  true,
		ExitCode: 0,
		Stdout:   "mock build ok",
		Stderr:   "",
	}, nil
}

// UnitTestSuite - Temporal WorkflowTestSuite을 활용한 workflow 단위 테스트.
type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *UnitTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_MockManagerWorkflow_Success() {
	req := wf.PipelineRequest{
		ServiceName: "metaflow_manager",
		RepoURL:     "https://github.com/Lyckabc/metaflow_manager",
		Branch:      "main",
		BuildMode:   "ci",
	}
	s.env.ExecuteWorkflow(MockManagerWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result wf.RunResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.True(result.Success)
	s.Equal(0, result.ExitCode)
	s.Equal("mock build ok", result.Stdout)
}

func (s *UnitTestSuite) Test_MockManagerWorkflow_WithAllFields() {
	req := wf.PipelineRequest{
		ServiceName:       "owner/repo",
		RepoURL:           "https://github.com/owner/repo",
		Branch:            "feature/x",
		BuildMode:         "cd",
		GitHubOwner:       "owner",
		GitHubRepo:        "repo",
		GitHubSHA:         "abc123",
		TemporalUIBaseURL: "http://localhost:8080",
	}
	s.env.ExecuteWorkflow(MockManagerWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result wf.RunResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.True(result.Success)
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func TestTriggerAndWaitWorkflow_Success(t *testing.T) {
	ctx := context.Background()
	mock := &mockWorkflowRunner{
		result: wf.RunResult{Success: true, ExitCode: 0},
	}

	err := triggerAndWaitWorkflow(ctx, mock, "metaflow_manager", "https://github.com/Lyckabc/metaflow_manager", "main", "ci", true)
	if err != nil {
		t.Fatalf("triggerAndWaitWorkflow: %v", err)
	}

	if mock.lastReq.ServiceName != "metaflow_manager" {
		t.Errorf("ServiceName = %q, want metaflow_manager", mock.lastReq.ServiceName)
	}
	if mock.lastReq.Branch != "main" {
		t.Errorf("Branch = %q, want main", mock.lastReq.Branch)
	}
	if mock.lastReq.BuildMode != "ci" {
		t.Errorf("BuildMode = %q, want ci", mock.lastReq.BuildMode)
	}
	if mock.lastOptions.TaskQueue != taskQueue {
		t.Errorf("TaskQueue = %q, want %q", mock.lastOptions.TaskQueue, taskQueue)
	}
}

func TestTriggerAndWaitWorkflow_ExecuteError(t *testing.T) {
	ctx := context.Background()
	mock := &mockWorkflowRunner{
		err: context.DeadlineExceeded,
	}

	err := triggerAndWaitWorkflow(ctx, mock, "metaflow_manager", "https://github.com/x/y", "main", "ci", false)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTriggerAndWaitWorkflow_WorkflowFailure(t *testing.T) {
	ctx := context.Background()
	mock := &mockWorkflowRunner{
		result: wf.RunResult{Success: false, ExitCode: 1, Stderr: "build failed"},
	}

	err := triggerAndWaitWorkflow(ctx, mock, "metaflow_manager", "https://github.com/x/y", "main", "ci", true)
	if err == nil {
		t.Fatal("expected error for failed workflow")
	}
}

// TestTemporalIntegrationWithDevServer - Temporal DevServer를 사용한 통합 테스트.
// SKIP_TEMPORAL_DEV_SERVER=1 이면 스킵 (CI 환경에서 DevServer 다운로드 비용 절감).
func TestTemporalIntegrationWithDevServer(t *testing.T) {
	if os.Getenv("SKIP_TEMPORAL_DEV_SERVER") == "1" {
		t.Skip("SKIP_TEMPORAL_DEV_SERVER=1, skipping DevServer integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	devServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{Version: "default"},
	})
	if err != nil {
		t.Skipf("DevServer start failed (temporal CLI may not be available): %v", err)
	}
	defer devServer.Stop()

	// Worker with mock ManagerWorkflow
	w := worker.New(devServer.Client(), taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(MockManagerWorkflow, temporalworkflow.RegisterOptions{
		Name: wf.ManagerWorkflowType,
	})
	go func() {
		_ = w.Run(worker.InterruptCh())
	}()
	// Give worker time to start
	time.Sleep(500 * time.Millisecond)

	// Trigger workflow via client
	err = runTemporalIntegration(ctx, devServer.FrontendHostPort(), "http://localhost:8059",
		defaultProjectName, defaultRepoURL, defaultBranch, defaultBuildMode,
		false, true) // createProject=false, wait=true
	if err != nil {
		t.Fatalf("runTemporalIntegration: %v", err)
	}
}
