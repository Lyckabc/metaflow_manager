# Git Webhook → metaflow_manager → metaflow_cicd 워크플로우

Git webhook이 들어왔을 때 metaflow_manager의 처리 흐름과 metaflow_cicd와 주고받는 데이터를 정리합니다.

## 전체 흐름도

```
GitHub/Convoy Webhook (POST /webhooks/github)     수동 테스트 (POST /trigger)
        │                                                    │
        └──────────────────────┬─────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ metaflow_manager (WebhookAdapter)                              │
│  - 시그니처 검증 (webhook만, X-Convoy-Signature / X-Hub-Signature-256) │
│  - PipelineRequest 생성 (webhook: payload 파싱 / trigger: JSON body)  │
│  - Temporal ExecuteWorkflow(ManagerWorkflow, req)              │
└───────────────────────────────────────────────────────────────┘
        │
        │  Task Queue: ci-task-queue
        ▼
┌───────────────────────────────────────────────────────────────┐
│ metaflow_cicd (Worker)                                         │
│  ManagerWorkflow 실행                                          │
│    1. PreFlightCheckActivity(req) → RunnerInput                │
│    2. RunnerWorkflow(runnerInput) → RunResult                  │
│    3. (CI 성공 시) DockerBuildPushActivity                     │
│    4. GitHub Status 업데이트                                   │
└───────────────────────────────────────────────────────────────┘
```

---

## 1. metaflow_manager 워크플로우 (Git Webhook 수신 시)

### 1.1 진입점

- **엔드포인트**: `POST /webhooks/github`
- **소스**: Convoy (GitHub webhook → Convoy endpoint) 또는 GitHub 직접 webhook

### 1.2 처리 단계

| 단계 | 설명 | 참고 코드 |
|------|------|-----------|
| 1 | HTTP Method 검증 (POST만 허용) | `webhook.go:132-135` |
| 2 | 시그니처 검증 | `CONVOY_ENDPOINT_SECRET` 또는 `GITHUB_WEBHOOK_SECRET` 사용. `X-Convoy-Signature` 또는 `X-Hub-Signature-256` 검증 |
| 3 | Convoy 래핑 해제 | Convoy 포맷이면 `event.data` 추출 |
| 4 | 이벤트 타입별 파싱 | `X-GitHub-Event`: `push` 또는 `pull_request` |
| 5 | `PipelineRequest` 생성 | GitHub payload → workflow 입력 구조체 변환 |
| 6 | Workflow ID 생성 | `ci-{ServiceName}-{Branch}-{SHA7}` (예: `ci-owner-repo-main-a1b2c3d`) |
| 7 | Temporal 워크플로우 시작 | `ExecuteWorkflow(ManagerWorkflow, req)` |

### 1.3 이벤트별 BuildMode

| 이벤트 | Action | BuildMode |
|--------|--------|-----------|
| `push` | - | `cd` |
| `pull_request` | `opened`, `synchronize` | `ci` |
| `pull_request` | `closed` | `cd` |
| 그 외 | - | 무시 (202 Accepted) |

### 1.4 수동 트리거 (POST /trigger) — CI 테스트용

Git webhook 없이 워크플로우를 수동으로 시작할 때 사용합니다. `metaflow_cicd` 프로젝트 자체 CI 테스트에 활용됩니다.

| 항목 | 내용 |
|------|------|
| **엔드포인트** | `POST /trigger` |
| **Request Body** | `TriggerRequest` (JSON) |
| **Workflow ID** | `ci-{ServiceName}` (예: `ci-metaflow_cicd`) |

**TriggerRequest:**
```json
{
  "service_name": "metaflow_cicd",
  "repo_url": "https://github.com/Lyckabc/metaflow_cicd",
  "branch": "main",
  "build_mode": "ci"
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| `service_name` | string | N | 저장소 이름. 생략 시 `repo_url`에서 추출 |
| `repo_url` | string | Y | clone URL (projects.main_repo_url과 매칭) |
| `branch` | string | N | 기본값 `main` |
| `build_mode` | string | N | `ci` 또는 `cd`, 기본값 `ci` |

**참고:** `metaflow_cicd/scripts/test_pipeline.sh` — metaflow_cicd 프로젝트 CI를 직접 테스트하는 스크립트.

---

## 2. metaflow_manager → metaflow_cicd 전달 데이터

### 2.1 PipelineRequest (metaflow_manager가 metaflow_cicd에 전달)

`workflow.ManagerWorkflow`의 입력 인자로 전달됩니다.

| 필드 | 타입 | 설명 | push 이벤트 | pull_request 이벤트 |
|------|------|------|-------------|---------------------|
| `ServiceName` | string | 저장소 전체 이름 | `push.Repo.FullName` (예: `owner/repo`) | `pr.Repository.FullName` |
| `RepoURL` | string | clone URL | `push.Repo.CloneURL` | `pr.Repository.CloneURL` |
| `Branch` | string | 대상 브랜치 | `refs/heads/` 제거 후 (예: `main`) | `pr.PullRequest.Head.Ref` |
| `BuildMode` | string | `"ci"` 또는 `"cd"` | `cd` | `opened`/`synchronize` → `ci`, `closed` → `cd` |
| `GitHubOwner` | string | GitHub org/owner | `FullName`에서 추출 | 동일 |
| `GitHubRepo` | string | GitHub repo 이름 | `FullName`에서 추출 | 동일 |
| `GitHubSHA` | string | 커밋 SHA | `push.After` | `pr.PullRequest.Head.SHA` |
| `TemporalUIBaseURL` | string | Temporal UI URL (status target_url용) | `TEMPORAL_UI_BASE_URL` env | 동일 |

### 2.2 GitHub Payload → PipelineRequest 매핑

**push 이벤트:**
```json
{
  "ref": "refs/heads/main",
  "repository": {
    "full_name": "owner/repo",
    "clone_url": "https://github.com/owner/repo.git"
  },
  "after": "a1b2c3d4e5f6..."
}
```

**pull_request 이벤트:**
```json
{
  "action": "opened" | "synchronize" | "closed",
  "pull_request": {
    "head": { "ref": "feature-branch", "sha": "a1b2c3d..." },
    "base": { "ref": "main" }
  },
  "repository": {
    "full_name": "owner/repo",
    "clone_url": "https://github.com/owner/repo.git"
  }
}
```

---

## 3. metaflow_cicd 내부 데이터 흐름

### 3.1 ManagerWorkflow 처리

| 단계 | Activity/Workflow | 입력 | 출력 |
|------|-------------------|------|------|
| 1 | `UpdateGitHubStatusActivity` | `GitHubStatusInput` (pending) | - |
| 2 | `PreFlightCheckActivity` | `PipelineRequest` | `RunnerInput` |
| 3 | `RunnerWorkflow` (child) | `RunnerInput` | `RunResult` |
| 4 | (CI 성공 시) `DockerBuildPushActivity` | `PipelineConfig` | `DockerBuildResult` |
| 5 | `UpdateGitHubStatusActivity` | `GitHubStatusInput` (success/failure/error) | - |

### 3.2 PreFlightCheckActivity 출력: RunnerInput

`PipelineRequest`를 받아 DB 조회 후 `RunnerInput`을 생성합니다.

| 필드 | 설명 | 출처 |
|------|------|------|
| `ProjectName` | 프로젝트 이름 | DB `projects.project_name` |
| `GitURL` | Git clone URL | `projects.main_repo_url` 또는 `sources.host` |
| `AccessToken` | Git PAT (private repo용) | `sources.access_token` |
| `Branch` | 브랜치 | `PipelineRequest.Branch` |
| `ConfigPath` | CI/CD 설정 경로 | `projects.ci_config_path` / `cd_config_path` |
| `Secrets` | 프로젝트 시크릿 | DB `secrets` (project_id, scope=prod) |
| `SecretsMapping` | env → secret_key 매핑 | metaflow-ci.toml `[secrets_mapping]` |
| `BuildMode` | `ci` / `cd` | `PipelineRequest.BuildMode` |
| `GitHubOwner` | - | `PipelineRequest` |
| `GitHubRepo` | - | `PipelineRequest` |
| `GitHubSHA` | - | `PipelineRequest` |
| `TemporalUIBaseURL` | - | `PipelineRequest` |

### 3.3 RunnerWorkflow → RunResult

| 필드 | 설명 |
|------|------|
| `Stdout` | 표준 출력 |
| `Stderr` | 표준 에러 |
| `ExitCode` | 종료 코드 |
| `Success` | 성공 여부 |

---

## 4. metaflow_cicd → metaflow_manager (응답)

metaflow_manager는 워크플로우를 **비동기**로 시작하고 즉시 HTTP 응답을 반환합니다.

### 4.1 Webhook HTTP 응답

**성공 (202 Accepted):**
```json
{
  "workflow_id": "ci-owner-repo-main-a1b2c3d",
  "run_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

**실패:**
- `400`: 잘못된 payload
- `401`: 시그니처 검증 실패
- `500`: Temporal ExecuteWorkflow 오류

---

## 5. 요약

| 구분 | 내용 |
|------|------|
| **통신 방식** | metaflow_manager → metaflow_cicd: Temporal `ExecuteWorkflow` (동기 호출 아님, 워크플로우 큐잉) |
| **Task Queue** | `ci-task-queue` |
| **Workflow** | `ManagerWorkflow` |
| **주요 전달 데이터** | `PipelineRequest` (ServiceName, RepoURL, Branch, BuildMode, GitHubOwner, GitHubRepo, GitHubSHA, TemporalUIBaseURL) |
| **metaflow_cicd 내부** | `PipelineRequest` → PreFlight → `RunnerInput` → RunnerWorkflow → `RunResult` |

---

## 6. metaflow_cicd 샘플 프로젝트 (Self CI/CD)

`metaflow_cicd`는 이 CI/CD 파이프라인을 **자기 자신**에 적용하는 샘플 프로젝트입니다.

### 6.1 사전 요구사항

| 구성요소 | 설명 |
|----------|------|
| DB | metaflow_cicd DB (projects, secrets 테이블) |
| Temporal | Temporal 서버 (gRPC 7233) |
| metaflow_manager | Trigger + WebhookAdapter (POST /trigger, /webhooks/github) |
| metaflow_cicd Worker | ci-task-queue 처리 |
| API 서버 | metaflow_cicd API (POST /projects, /secrets) — 프로젝트/시크릿 등록용 |

### 6.2 DB 등록 (projects, secrets)

- **projects**: `main_repo_url` = `https://github.com/Lyckabc/metaflow_cicd`, `ci_config_path` = `flows/metaflow-ci.toml`
- **secrets**: metaflow-ci.toml `[secrets_mapping]`에 정의된 secret_key들 (REGISTRY_URL, REGISTRY_ID, REGISTRY_PASSWORD 등)

### 6.3 CI 테스트 스크립트

`metaflow_cicd/scripts/test_pipeline.sh` — metaflow_cicd 프로젝트의 전체 CI 파이프라인을 수동으로 테스트합니다.

```bash
# 호스트에서 (trigger가 localhost:8080에 노출된 경우)
./scripts/test_pipeline.sh

# trigger가 Docker 내부에만 있는 경우
TRIGGER_VIA_DOCKER=1 ./scripts/test_pipeline.sh
```
