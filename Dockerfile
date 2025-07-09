FROM golang:1.24.1-alpine AS builder

WORKDIR /app
COPY . .

# Install air for dev
RUN go install github.com/air-verse/air@latest

# Build binary for production
RUN go build -o ./bin/server ./cmd/server.go

# --- Development image ---
FROM golang:1.24.1-alpine AS dev
WORKDIR /app
COPY --from=builder /go/bin/air /go/bin/air
COPY . .
CMD ["air", "-c", ".server.toml"]

# --- Production image ---
FROM alpine:3.20 AS prod
WORKDIR /app
COPY --from=builder /app/bin/server /app/bin/server
CMD ["/app/bin/server"]