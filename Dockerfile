# metaflow_manager: Trigger (WebhookAdapter + Temporal workflow starter)
# Build context: temporal/ (parent) - needs metaflow_cicd for go mod replace
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app

# Copy metaflow_cicd (required by go mod replace)
COPY metaflow_cicd ./metaflow_cicd
# Copy metaflow_manager
COPY metaflow_manager/go.mod metaflow_manager/go.sum ./metaflow_manager/
COPY metaflow_manager ./metaflow_manager/

WORKDIR /app/metaflow_manager
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
