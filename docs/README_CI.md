# metaflow_manager CI 가이드

metaflow_manager 프로젝트의 빌드, 테스트, CI 파이프라인 실행 방법을 정리합니다.

---

## 1. 개요

| 구분 | 설명 |
|------|------|
| **프로젝트** | metaflow_manager (Go) |
| **역할** | Webhook/Trigger 서비스 — GitHub webhook 수신 후 metaflow_cicd ManagerWorkflow 트리거 |
| **CI 설정** | `flows/metaflow_manager.toml` |
| **CI 러너** | `flows/runner/metaflow_manager_ci.go` |

---

## 2. 빌드 & 테스트

### 2.1 로컬 빌드

```bash
go build ./...
```

### 2.2 Docker 빌드

```bash
cd metaflow_manager
docker build -t registry.toji.homes/metaflow_manager:main-2602161631 .
```

- metaflow_cicd를 git clone하여 go mod replace 충족

### 2.3 단위 테스트

```bash
go test ./...
```

### 2.4 CI 러너 실행 (빌드 + 테스트)

```bash
go run ./flows/runner/metaflow_manager_ci.go
```

- `go build ./...` 수행
- `go test ./...` 수행
- 성공 시 종료 코드 0

---

## 3. CI 러너 상세

### 3.1 flows/runner/metaflow_manager_ci.go

metaflow_cicd의 `metaflow_ci.go`를 참고하여 작성된 CI 테스트 스크립트입니다.

| 단계 | 설명 |
|------|------|
| 1 | `go build ./...` — 빌드 검증 |
| 2 | `go test ./...` — 단위 테스트 실행 |
| 3 | (선택) `-temporal` — Temporal SDK로 ManagerWorkflow 트리거 (통합 테스트) |

### 3.2 사용법

```bash
# 기본: 빌드 + 테스트만
go run ./flows/runner/metaflow_manager_ci.go

# Temporal 통합 테스트 (Temporal 서버, metaflow_cicd Worker 필요)
go run ./flows/runner/metaflow_manager_ci.go -temporal

# 워크플로우 완료까지 대기
go run ./flows/runner/metaflow_manager_ci.go -temporal -wait

# 옵션
go run ./flows/runner/metaflow_manager_ci.go -branch main -mode ci -temporal
```

### 3.3 옵션

| 플래그 | 기본값 | 설명 |
|--------|--------|------|
| `-temporal` | false | Temporal workflow 트리거 (통합 테스트) |
| `-wait` | false | 워크플로우 완료까지 대기 (`-temporal` 필요) |
| `-branch` | main | 대상 브랜치 |
| `-mode` | ci | 빌드 모드 (`ci` 또는 `cd`) |
| `-create-project` | true | API로 프로젝트 등록 시도 (`-temporal` 시) |

### 3.4 환경 변수 (Temporal 모드)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TEMPORAL_ADDRESS` | localhost:7233 | Temporal 서버 주소 |
| `METAFLOW_CICD_API_URL` | http://localhost:8059 | metaflow_cicd API (프로젝트 등록용) |

### 3.5 Temporal 모드 사전 요구사항

- metaflow_cicd DB (projects, secrets)
- Temporal 서버
- metaflow_cicd Worker (ci-task-queue)

---

## 4. flows/metaflow_manager.toml

metaflow_cicd의 `metaflow-ci.toml`을 참고한 CI/CD 설정입니다.

| 섹션 | 내용 |
|------|------|
| `[project]` | name, language, version |
| `[build]` | entrypoint, pre_build, command |
| `[config]` | LOG_LEVEL 등 |
| `[secrets_mapping]` | DB secrets 매핑 |
| `[registry]` | Docker 빌드/푸시 설정 |

**주요 설정:**

```toml
[build]
entrypoint = "flows/runner/metaflow_manager_ci.go"
pre_build = "go mod download"
command = "go run flows/runner/metaflow_manager_ci.go"
```

metaflow_cicd 파이프라인에서 metaflow_manager를 빌드할 때 위 command가 실행됩니다.

---

## 5. 단위 테스트

### 5.1 테스트 파일

| 파일 | 설명 |
|------|------|
| `internal/handler/webhook_test.go` | Webhook 핸들러 테스트 |

### 5.2 테스트 항목

| 테스트 | 설명 |
|--------|------|
| `TestExtractOwner` | `extractOwner()` — owner/repo → owner |
| `TestExtractRepo` | `extractRepo()` — owner/repo → repo |
| `TestBuildModeFromPullRequest` | PR action별 BuildMode (synchronize→ci, closed+merged→cd, closed+!merged→무시) |
| `TestWebhookPullRequestBuildMode` | HTTP 요청으로 pull_request BuildMode 검증 |
| `TestWebhookPushBuildMode` | push 이벤트 BuildMode=cd 검증 |
| `TestWebhookMethodNotAllowed` | GET 등 비허용 메서드 405 검증 |

### 5.3 실행

```bash
go test ./internal/handler/... -v
```

---

## 6. metaflow_manager Self CI/CD

metaflow_manager도 metaflow_cicd 파이프라인의 **대상 프로젝트**로 등록할 수 있습니다.

### 6.1 DB 등록 (metaflow_cicd projects 테이블)

| 필드 | 값 |
|------|-----|
| `project_name` | metaflow_manager |
| `main_repo_url` | https://github.com/Lyckabc/metaflow_manager |
| `ci_config_path` | flows/metaflow_manager.toml |
| `cd_config_path` | flows/metaflow_manager.toml |

### 6.2 워크플로우

1. GitHub webhook (PR/push) → metaflow_manager → ManagerWorkflow 트리거
2. metaflow_cicd Worker가 `flows/metaflow_manager.toml` 기반으로 빌드/테스트 실행
3. CI 성공 시 Docker 빌드 & 푸시

---

## 7. 관련 문서

| 문서 | 설명 |
|------|------|
| [GIT_WEBHOOK_FLOW.md](./GIT_WEBHOOK_FLOW.md) | Git webhook → metaflow_manager → metaflow_cicd 전체 흐름 |
