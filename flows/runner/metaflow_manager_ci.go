// metaflow_manager_ci.go - metaflow_manager CI Test (Temporal SDK 연동)
//
// metaflow_cicd의 metaflow_ci.go를 참고하여 작성.
// metaflow_manager 빌드/테스트 및 Temporal SDK로 ManagerWorkflow 트리거 검증.
//
// Prerequisites (Temporal 모드):
//   - metaflow_cicd DB (projects, secrets)
//   - Temporal 서버
//   - metaflow_cicd Worker (ci-task-queue)
//
// Usage:
//
//	go run ./flows/runner/metaflow_manager_ci.go
//	go run ./flows/runner/metaflow_manager_ci.go -temporal
//	go run ./flows/runner/metaflow_manager_ci.go -temporal -wait
//
// Env:
//
//	TEMPORAL_ADDRESS - Temporal 서버 (default: localhost:7233)
//	METAFLOW_CICD_API_URL - API 서버 (프로젝트 등록용, default: http://localhost:8059)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"metaflow_manager/internal/workflow"
	"go.temporal.io/sdk/client"
)

const (
	defaultProjectName  = "metaflow_manager"
	defaultRepoURL      = "https://github.com/Lyckabc/metaflow_manager"
	defaultBranch      = "main"
	defaultBuildMode   = "ci"
	defaultTemporalAddr = "localhost:7233"
	defaultAPIURL      = "http://localhost:8059"
	ciConfigPath       = "flows/metaflow_manager.toml"
	taskQueue          = "ci-task-queue"
	workflowTimeout    = 20 * time.Minute
)

func main() {
	temporal := flag.Bool("temporal", false, "trigger Temporal workflow (integration test)")
	wait := flag.Bool("wait", false, "wait for workflow completion (requires -temporal)")
	branch := flag.String("branch", defaultBranch, "target branch")
	mode := flag.String("mode", defaultBuildMode, "build mode: ci or cd")
	createProject := flag.Bool("create-project", true, "create/update project via API (requires -temporal)")
	flag.Parse()

	fmt.Println("==============================================")
	fmt.Println(" metaflow_manager CI Test")
	fmt.Println("==============================================")

	// 1. Build
	fmt.Println("=== 1. Build ===")
	if err := runCmd("go", "build", "./..."); err != nil {
		log.Fatalf("Build failed: %v", err)
	}
	fmt.Println(" Build OK")
	fmt.Println()

	// 2. Test
	fmt.Println("=== 2. Test ===")
	if err := runCmd("go", "test", "./..."); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
	fmt.Println(" Test OK")
	fmt.Println()

	// 3. Temporal integration (optional)
	if !*temporal {
		fmt.Println("=== 3. Result ===")
		fmt.Println(" Build and test passed.")
		fmt.Println(" Use -temporal to run integration test (trigger ManagerWorkflow).")
		return
	}

	fmt.Println("=== 3. Temporal integration test ===")
	temporalAddr := os.Getenv("TEMPORAL_ADDRESS")
	if temporalAddr == "" {
		temporalAddr = defaultTemporalAddr
	}
	apiURL := os.Getenv("METAFLOW_CICD_API_URL")
	if apiURL == "" {
		apiURL = defaultAPIURL
	}

	projectName := defaultProjectName
	repoURL := defaultRepoURL

	if *createProject {
		fmt.Println(" Create/Update project...")
		if err := createOrUpdateProject(apiURL, projectName, repoURL, *branch, *mode); err != nil {
			log.Printf("Create project (may exist): %v", err)
		}
	}

	fmt.Println(" Connect Temporal & Trigger workflow...")
	c, err := client.Dial(client.Options{
		HostPort:  temporalAddr,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		log.Fatalf("Temporal client: %v", err)
	}
	defer c.Close()

	workflowID := "ci-" + strings.ReplaceAll(projectName, "/", "-") + "-" + *branch + "-test"
	options := client.StartWorkflowOptions{
		ID:                   workflowID,
		TaskQueue:            taskQueue,
		WorkflowRunTimeout:   workflowTimeout,
		WorkflowTaskTimeout:  time.Minute,
	}

	req := workflow.PipelineRequest{
		ServiceName:       projectName,
		RepoURL:           repoURL,
		Branch:            *branch,
		BuildMode:         *mode,
		TemporalUIBaseURL: os.Getenv("TEMPORAL_UI_BASE_URL"),
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, workflow.ManagerWorkflowType, req)
	if err != nil {
		log.Fatalf("ExecuteWorkflow: %v", err)
	}

	fmt.Printf(" Workflow ID: %s\n", we.GetID())
	fmt.Printf(" Run ID:     %s\n", we.GetRunID())

	if *wait {
		fmt.Println(" Waiting for workflow completion...")
		var result workflow.RunResult
		err = we.Get(context.Background(), &result)
		if err != nil {
			log.Fatalf("Workflow failed: %v", err)
		}
		fmt.Printf(" Success: %v, ExitCode: %d\n", result.Success, result.ExitCode)
		if result.Stdout != "" {
			fmt.Println("--- Stdout ---")
			fmt.Println(result.Stdout)
		}
		if result.Stderr != "" {
			fmt.Println("--- Stderr ---")
			fmt.Println(result.Stderr)
		}
		if !result.Success {
			os.Exit(result.ExitCode)
		}
		fmt.Println("\nIntegration test passed.")
	} else {
		fmt.Println("\n Check Temporal UI for execution status.")
		fmt.Println(" Use -wait to block until workflow completes.")
	}
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func createOrUpdateProject(apiURL, projectName, repoURL, branch, mode string) error {
	payload := map[string]interface{}{
		"project_name":    projectName,
		"main_repo_url":   repoURL,
		"target_branches": []string{"main", "dev", "^feature/.*", "^release/.*"},
		"ci_config_path":  ciConfigPath,
		"cd_config_path":  ciConfigPath,
		"description":     "metaflow_manager webhook/trigger service",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, apiURL+"/projects", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		return nil
	}
	var errBody bytes.Buffer
	_, _ = errBody.ReadFrom(resp.Body)
	errStr := errBody.String()
	if strings.Contains(errStr, "duplicate key") {
		return nil
	}
	return fmt.Errorf("POST /projects: %s %s", resp.Status, errStr)
}
