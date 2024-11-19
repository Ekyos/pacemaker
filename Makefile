dev-up:
	docker-compose up -d

dev-down:
	docker-compose down

dev-restart:
	docker-compose down
	docker-compose up -d

db-logs:
	docker-compose logs -f mongodb

dev-clean:
	docker-compose down -v