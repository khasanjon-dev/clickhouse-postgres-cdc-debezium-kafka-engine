# Development commands
up:
	docker compose up -d

down:
	docker compose down

down_v:
	docker compose down -v

build:
	docker compose up --build -d


# DEBEZIUM CONNECTOR
ddl: # only ddl_logs tables
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-configs/ddl_changes/v1.json

tables: # all tables
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-configs/synchronization/v1.json

