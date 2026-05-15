.PHONY: dev api seed docker-run

dev:
	uvicorn api.main:app --reload --port 8080

api:
	uvicorn api.main:app --host 0.0.0.0 --port 8080

seed:
	python etl/generate_ahmedabad_seed.py

docker-run:
	docker build -t hicc-api -f docker/Dockerfile .
	docker run -p 8080:8080 --env-file .env hicc-api