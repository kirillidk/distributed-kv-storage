BUF ?= buf
BASE_BRANCH ?= main
GENERATED_DIRS := api/gen/go internal/repl/gen/go

.PHONY: ci format lint generate check-generated breaking vet test

ci: lint check-generated vet test

format:
	$(BUF) format -w

lint:
	$(BUF) format --diff --exit-code
	$(BUF) lint

generate:
	$(BUF) generate
	$(BUF) generate --template internal/repl/buf.gen.yaml

check-generated: generate
	git diff --exit-code -- $(GENERATED_DIRS)
	test -z "$$(git ls-files --others --exclude-standard $(GENERATED_DIRS))"

breaking:
	$(BUF) breaking --against '.git#branch=$(BASE_BRANCH)'

vet:
	go vet ./...

test:
	go test ./...
