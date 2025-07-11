build:
	@go build -o ./bin/observer ./cmd/observer.go
run: build
	@./bin/observer
dev:
	@air -c .observer.toml
test:
	@go test ./... -v