package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"metaflow_manager/internal/handler"
	"go.temporal.io/sdk/client"
)

func main() {
	hostPort := os.Getenv("TEMPORAL_ADDRESS")
	if hostPort == "" {
		hostPort = "temporal:7233"
	}
	if strings.HasPrefix(hostPort, "localhost:") {
		hostPort = "127.0.0.1" + hostPort[len("localhost"):]
	}

	ciClient, err := client.Dial(client.Options{HostPort: hostPort, Namespace: "metaflow-ci"})
	if err != nil {
		log.Fatalln("Temporal CI client:", err)
	}
	defer ciClient.Close()

	cdClient, err := client.Dial(client.Options{HostPort: hostPort, Namespace: "metaflow-cd"})
	if err != nil {
		log.Fatalln("Temporal CD client:", err)
	}
	defer cdClient.Close()

	starter := &handler.BuildModeAwareWorkflowStarter{CIClient: ciClient, CDClient: cdClient}

	listen := os.Getenv("TRIGGER_LISTEN")
	if listen == "" {
		listen = "0.0.0.0:8080"
	}

	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	http.HandleFunc("GET /webhooks/github", func(w http.ResponseWriter, r *http.Request) {
		handler.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	http.HandleFunc("POST /trigger", handler.TriggerHandler(starter))
	http.HandleFunc("POST /webhooks/github", handler.GitHubWebhookHandler(starter))

	log.Printf("Trigger listening on %s (POST /trigger, POST /webhooks/github, GET /health)", listen)
	log.Fatal(http.ListenAndServe(listen, nil))
}
