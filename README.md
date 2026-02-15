# metaflow_manager

Trigger + WebhookAdapter 서비스. Convoy/GitHub webhook을 받아 Temporal `ManagerWorkflow`를 시작합니다.

## 역할

- **Trigger**: POST /trigger — JSON으로 워크플로우 파라미터를 받아 ManagerWorkflow 시작
- **WebhookAdapter**: POST /webhooks/github — GitHub payload를 변환하여 ManagerWorkflow 시작
- **Health**: GET /health — 헬스체크

## 워크플로우

```
webhook(convoy) → endpoint → metaflow_manager (Trigger, WebhookAdapter) → metaflow_cicd worker 실행
```

## 로컬 실행

```bash
cd /morphogen/neunexus/cicd/temporal
TEMPORAL_ADDRESS=127.0.0.1:7233 go run ./metaflow_manager/cmd/trigger
```

## Docker

```bash
docker compose up -d metaflow_manager
```

## 디렉터리 구조

```
metaflow_manager/
├── cmd/trigger/       # HTTP 서버 (Trigger + WebhookAdapter)
├── internal/handler/  # trigger.go, webhook.go, common.go
└── Dockerfile
```
