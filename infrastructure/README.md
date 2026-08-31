Infrastructure: run only database containers

This repository is configured to run only the database containers (MySQL and MongoDB) via Docker Compose.

Start the databases:

```bash
cd infrastructure
docker-compose up -d
```

MySQL will be available on `localhost:3306` (user: `appuser`, password: `apppass`, database: `order_db` created by init scripts).
MongoDB will be available on `localhost:27017` (root user: `admin`, password: `admin`, database `product_db` initialized by init scripts).

Run microservices locally (example using Maven):

```bash
# From the service folder, e.g. order-service
cd ../order-service
mvn spring-boot:run

# For inventory-service, product-service, api-gateway, discovery-server, run similarly
```

Environment variables in each service's `application.properties` are set to use `localhost` by default, so services will connect to the containers when started locally.

If you prefer to tear down the DB containers:

```bash
cd infrastructure
docker-compose down
```
