.PHONY: build run server demo-temporal demo-persistence web-dev web-build clean

# Build the server binary
build:
	@cd server && go build -o ../graph-db main.go
	@echo "Built graph-db binary in root directory"

# Build and run the server
run: build
	@./graph-db

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

# Clean build artifacts
clean:
	@rm -rf data/ graph-db web/dist
	@echo "Cleaned up data directory and binaries"
