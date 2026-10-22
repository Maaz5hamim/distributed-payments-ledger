COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down test integration smoke verify load fmt vet
up:
	$(COMPOSE) up -d --build
down:
	$(COMPOSE) down
test:
	go test -race ./...
integration:
	$(COMPOSE) run --rm test
smoke:
	python3 scripts/smoke.py
verify:
	python3 scripts/verify.py
load:
	$(COMPOSE) run --rm k6
fmt:
	gofmt -w cmd internal
vet:
	go vet ./...
