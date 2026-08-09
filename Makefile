PGDATA ?= .pgdata
PGPORT ?= 5433
PGUSER ?= pubsub
PGDB   ?= pubsub
PGBIN  ?= /Library/PostgreSQL/18/bin
DATABASE_URL ?= postgres://$(PGUSER)@localhost:$(PGPORT)/$(PGDB)?sslmode=disable

.PHONY: up down db-start db-stop db-init run tidy test

# Docker alternative (if available)
up:
	docker compose up -d

down:
	docker compose down

db-init:
	mkdir -p .pgsocket
	@if [ ! -f $(PGDATA)/PG_VERSION ]; then \
		$(PGBIN)/initdb -D $(PGDATA) -U $(PGUSER) --auth=trust --encoding=UTF8 --locale=C; \
		echo "port = $(PGPORT)" >> $(PGDATA)/postgresql.conf; \
		echo "listen_addresses = 'localhost'" >> $(PGDATA)/postgresql.conf; \
		echo "unix_socket_directories = '$(CURDIR)/.pgsocket'" >> $(PGDATA)/postgresql.conf; \
		printf '%s\n' \
			'local all all trust' \
			'host all all 127.0.0.1/32 trust' \
			'host all all ::1/128 trust' > $(PGDATA)/pg_hba.conf; \
	fi

db-start: db-init
	@$(PGBIN)/pg_ctl -D $(PGDATA) status >/dev/null 2>&1 || \
		$(PGBIN)/pg_ctl -D $(PGDATA) -l $(PGDATA)/pg.log start
	@$(PGBIN)/createdb -h localhost -p $(PGPORT) -U $(PGUSER) $(PGDB) 2>/dev/null || true
	@$(PGBIN)/psql -h localhost -p $(PGPORT) -U $(PGUSER) -d $(PGDB) -v ON_ERROR_STOP=1 \
		-c "SELECT 1 FROM topics LIMIT 1" >/dev/null 2>&1 || \
		$(PGBIN)/psql -h localhost -p $(PGPORT) -U $(PGUSER) -d $(PGDB) -f migrations/001_init.sql

db-stop:
	@$(PGBIN)/pg_ctl -D $(PGDATA) status >/dev/null 2>&1 && \
		$(PGBIN)/pg_ctl -D $(PGDATA) stop -m fast || true

run: db-start
	DATABASE_URL='$(DATABASE_URL)' go run ./cmd/broker

tidy:
	go mod tidy

test:
	go test ./...
