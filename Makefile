.PHONY: build run server demo-basic demo-temporal demo-persistence demo-pathfinding demo-query demo-temporal-paths demo-client demo-performance web-dev web-build clean

# Build the server binary
build:
	@cd server && go build -o ../gravecdb main.go
	@echo "Built gravecdb binary in root directory"

# Build and run the server
run: build
	@./gravecdb

# Run the visualization server
server:
	@cd server && go run main.go

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

# Run the client library demo (requires server running)
demo-client:
	@cd examples/client-demo && go run main.go

# Run the performance comparison demo
demo-performance:
	@cd examples/performance && go run main.go

# Clean build artifacts
clean:
	@rm -rf data/ gravecdb web/dist examples/*/data*
	@echo "Cleaned up data directory and binaries"
