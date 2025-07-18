build:
	@go build -o ./bin/observer ./cmd/distributed_observer.go
run: build
	@./bin/observer
dev:
	@air -c .observer.toml
test:
	@go test ./... -v