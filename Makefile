.PHONY: build build-linux build-all run server demo-basic demo-temporal demo-persistence demo-pathfinding demo-query demo-temporal-paths demo-embeddings demo-client demo-performance web-dev web-build docker docker-run compose-up compose-down clean

# Build the server binary for current platform
build:
	@cd server && go build -o ../gravecdb .
	@echo "Built gravecdb binary in root directory"

# Build for Linux (amd64)
build-linux:
	@cd server && GOOS=linux GOARCH=amd64 go build -o ../gravecdb-linux-amd64 .
	@echo "Built gravecdb-linux-amd64 binary in root directory"

# Build for Linux (arm64)
build-linux-arm:
	@cd server && GOOS=linux GOARCH=arm64 go build -o ../gravecdb-linux-arm64 .
	@echo "Built gravecdb-linux-arm64 binary in root directory"

# Build for all platforms
build-all: build build-linux build-linux-arm
	@echo "Built binaries for all platforms"

# Build and run the server
run: build
	@./gravecdb

# Run the visualization server
server:
	@cd server && go run .

# Run the frontend dev server
web-dev:
	@cd web-ui && npm run dev

# Build the frontend for production
web-build:
	@cd web-ui && npm run build
	@echo "Built frontend to web/dist"

# Run the basic demo
demo-basic:
	@cd examples/basic && go run main.go

# Run the temporal demo
demo-temporal:
	@cd examples/temporal && go run main.go

# Run the persistence demo
demo-persistence:
	@cd examples/persistence && go run main.go

# Run the pathfinding demo
demo-pathfinding:
	@cd examples/pathfinding && go run main.go

# Run the query language demo
demo-query:
	@cd examples/query && go run main.go

# Alias for demo-query
run-query: demo-query

# Run the temporal path-finding demo
demo-temporal-paths:
	@cd examples/temporal-paths && go run main.go

# Run the embeddings demo (requires Ollama)
demo-embeddings:
	@cd examples/embeddings && go run main.go

# Run the client library demo (requires server running)
demo-client:
	@cd examples/client-demo && go run main.go

# Run the performance comparison demo
demo-performance:
	@cd examples/performance && go run main.go

# Build Docker image
docker:
	@docker build -t gravecdb .
	@echo "Built gravecdb Docker image"

# Run Docker container
docker-run:
	@docker run -p 8080:8080 -v $(PWD)/data:/data gravecdb

# Start with Docker Compose (includes Ollama + auto-pulls embedding model)
compose-up:
	@docker compose up -d
	@echo "GravecDB + Ollama starting (model will be pulled automatically)..."

# Stop Docker Compose
compose-down:
	@docker compose down

# Clean build artifacts
clean:
	@rm -rf data/ gravecdb web/dist examples/*/data*
	@echo "Cleaned up data directory and binaries"
