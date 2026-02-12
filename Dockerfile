# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Install git for go mod download
RUN apk add --no-cache git

# Copy source code
COPY . .

# Download deps and build
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o gravecdb ./server/main.go

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/gravecdb /usr/local/bin/gravecdb

# Default data directory
VOLUME /data

# Default port
EXPOSE 8080

# Default config: bind to all interfaces, persist to /data
ENV GRAVECDB_DSN="gravecdb://0.0.0.0:8080/data"

CMD ["gravecdb"]
