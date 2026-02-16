# metaflow_manager: Trigger (WebhookAdapter + Temporal workflow starter)
#
# Build context: metaflow_manager/ (현재 디렉터리)
# 사용법: cd metaflow_manager && docker build -t ...
# docker-compose: context: ./metaflow_manager
#
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app

COPY go.mod go.sum ./
COPY . .

RUN go mod download
RUN go build -o /trigger ./cmd/trigger

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /trigger /trigger
ENV TEMPORAL_ADDRESS=temporal:7233
ENV TRIGGER_LISTEN=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/trigger"]
