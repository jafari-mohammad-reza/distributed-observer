build:
	@go build -o ./bin/server ./cmd/server.go
run: build
	@./bin/server
dev:
	@air -c .server.toml
test:
	@go test ./... -v