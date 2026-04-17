SERVICE=airflow
DAG_ID=sheffield_crime_pipeline

.PHONY: up down build logs trigger unpause create-user reset-password run

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

trigger:
	docker compose exec -T $(SERVICE) airflow dags trigger $(DAG_ID)

unpause:
	docker compose exec -T $(SERVICE) airflow dags unpause $(DAG_ID)

create-user:
	docker compose exec -T $(SERVICE) airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

reset-password:
	docker compose exec -T $(SERVICE) airflow users reset-password \
		--username admin \
		--password admin

run: up
	@echo "Waiting for Airflow to start..."
	sleep 20
	$(MAKE) create-user || true
	$(MAKE) reset-password
	$(MAKE) unpause
	$(MAKE) trigger
	@echo "Pipeline triggered. Visit http://localhost:8080"
	@echo "Airflow login: admin / admin"