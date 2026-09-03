# 🏗️ Spring Boot Microservices Architecture

> A production-grade, cloud-native microservices platform built with **Spring Boot 3.5**, **Spring Cloud 2025**, and modern distributed system patterns — demonstrating enterprise-level backend engineering, scalability, and resilience.

[![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)](https://openjdk.org/projects/jdk/17/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.4-brightgreen?logo=spring)](https://spring.io/projects/spring-boot)
[![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-2025.0.0-brightgreen?logo=spring)](https://spring.io/projects/spring-cloud)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docs.docker.com/compose/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-black?logo=apachekafka)](https://kafka.apache.org/)
[![Keycloak](https://img.shields.io/badge/Keycloak-OAuth2%20%2F%20OIDC-red?logo=keycloak)](https://www.keycloak.org/)

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Microservices Breakdown](#microservices-breakdown)
  - [Product Service](#1-product-service)
  - [Order Service](#2-order-service)
  - [Inventory Service](#3-inventory-service)
  - [Notification Service](#4-notification-service)
- [Infrastructure & Cross-Cutting Concerns](#infrastructure--cross-cutting-concerns)
  - [Service Discovery — Netflix Eureka](#step-1-service-discovery--netflix-eureka-server)
  - [API Gateway — Spring Cloud Gateway](#step-2-api-gateway--spring-cloud-gateway)
  - [Security — Keycloak OAuth2 / OIDC](#step-3-security--keycloak-oauth2--oidc)
  - [Resilience — Resilience4J Circuit Breaker](#step-4-resilience--resilience4j-circuit-breaker-retry--time-limiter)
  - [Event-Driven Architecture — Apache Kafka](#step-5-event-driven-architecture--apache-kafka)
  - [Observability — Prometheus & Grafana](#step-6-observability--prometheus--grafana)
  - [Containerization — Docker & Jib](#step-7-containerization--docker--jib)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)

---

## Architecture Overview

This project implements a **polyglot persistence microservices ecosystem** where each service owns its data, communicates via well-defined APIs, and is independently deployable. The architecture follows industry-standard cloud-native patterns:

```
                         ┌─────────────────────────────────────────────────────┐
                         │                   CLIENT (HTTP/REST)                 │
                         └───────────────────────┬─────────────────────────────┘
                                                 │ :8090
                         ┌───────────────────────▼─────────────────────────────┐
                         │           API GATEWAY (Spring Cloud Gateway)         │
                         │    OAuth2 JWT Validation │ Load Balancing │ Routing  │
                         └──────┬─────────────┬─────────────┬───────────────────┘
                                │             │             │
              ┌─────────────────▼──┐  ┌───────▼────────┐  ┌▼──────────────────┐
              │   Product Service  │  │  Order Service │  │  Inventory Service │
              │  MongoDB | :8082   │  │  MySQL | :8081 │  │  MySQL | :8083     │
              └────────────────────┘  └───────┬────────┘  └───────────────────-┘
                                              │ (Kafka)
                         ┌────────────────────▼─────────────────────────────────┐
                         │            Notification Service (Kafka Consumer)       │
                         └──────────────────────────────────────────────────────-┘

                         ┌────────────────────────────────────────────────────-─┐
                         │         Eureka Discovery Server  (:8761)              │
                         │   (All services register & discover each other here)  │
                         └───────────────────────────────────────────────────────┘

                         ┌─────────────────────────────────────────────────────-┐
                         │       Prometheus (:9090) + Grafana (:3000)            │
                         │              Metrics & Monitoring                     │
                         └───────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Category | Technology | Purpose |
|---|---|---|
| **Language** | Java 17 | LTS release with modern language features (records, sealed classes, pattern matching) |
| **Framework** | Spring Boot 3.5.4 | Core application framework with auto-configuration |
| **Cloud** | Spring Cloud 2025.0.0 | Service discovery, gateway, circuit breaker |
| **Service Discovery** | Netflix Eureka | Dynamic service registration and client-side load balancing |
| **API Gateway** | Spring Cloud Gateway (WebFlux) | Reactive API gateway with routing, filtering, and load balancing |
| **Security** | Keycloak + Spring Security OAuth2 | Identity provider with JWT-based resource server security |
| **Resilience** | Resilience4J | Circuit Breaker, Retry, and Time Limiter patterns |
| **Messaging** | Apache Kafka + Zookeeper | Asynchronous event streaming for event-driven architecture |
| **Database (Orders)** | MySQL 8.0 + Spring Data JPA | Relational persistence with Hibernate ORM |
| **Database (Products)** | MongoDB 6.0 + Spring Data MongoDB | Document store for flexible product catalog |
| **Database (Inventory)** | MySQL 8.0 + Spring Data JPA | Relational inventory tracking |
| **HTTP Client** | Spring WebFlux WebClient | Non-blocking reactive inter-service communication |
| **Observability** | Micrometer + Prometheus + Grafana | Metrics collection, storage, and visualization |
| **Build Tool** | Maven (Multi-module) | Dependency management and build lifecycle |
| **Containerization** | Docker + Google Jib | Containerizing services and pushing to Docker Hub |
| **Orchestration** | Docker Compose | Local full-stack environment setup |
| **Boilerplate Reduction** | Lombok | Annotation-driven code generation |
| **Testing** | Testcontainers + JUnit 5 | Integration testing with real Docker-containerized databases |

---

## Microservices Breakdown

### 1. Product Service

**Port:** `8082` (dynamic via Eureka, `server.port=0`)
**Database:** MongoDB (document-based, flexible schema for product catalog)

Manages the product catalog. Exposes REST endpoints to create and retrieve products.

**Key implementation details:**
- Uses **Spring Data MongoDB** with `MongoRepository` for zero-boilerplate CRUD
- Uses **Lombok** `@Builder`, `@Data`, `@RequiredArgsConstructor` for clean, concise model and service layers
- Registered with **Eureka** for dynamic discovery — instances run on random ports to support horizontal scaling
- Exposes **Prometheus actuator endpoint** (`/actuator/prometheus`) for metrics scraping
- **Integration tested with Testcontainers** — spins up a real MongoDB Docker container during `mvn test` for true integration coverage

```java
// ProductService.java — clean service layer with Lombok + Spring Data MongoDB
@Service
@RequiredArgsConstructor
@Slf4j
public class ProductService {
    private final ProductRepository productRepository;

    public void createProduct(ProductRequest productRequest) {
        Product product = Product.builder()
                .name(productRequest.getName())
                .description(productRequest.getDescription())
                .price(productRequest.getPrice())
                .build();
        productRepository.save(product);
        log.info("Product {} is saved", product.getId());
    }
}
```

---

### 2. Order Service

**Port:** `8081`
**Database:** MySQL (relational, strict transactional integrity for order data)

The most complex service — orchestrates the entire order placement workflow by communicating with the Inventory Service and publishing events to Kafka.

**Key implementation details:**
- Uses **Spring Data JPA + Hibernate** with `@Transactional` for ACID-compliant order persistence
- Performs **synchronous inter-service calls** to Inventory Service using **Spring WebFlux `WebClient`** (non-blocking, reactive HTTP client)
- Implements the **Circuit Breaker + TimeLimiter + Retry** pattern via **Resilience4J annotations**
- Publishes `OrderPlacedEvent` to Kafka topic `notificationTopic` via **`KafkaTemplate`** upon successful order placement
- Reads Inventory Service URL from config, supporting **Docker Compose environment variable injection**

```java
// OrderController.java — three Resilience4J patterns stacked on a single endpoint
@PostMapping
@ResponseStatus(HttpStatus.CREATED)
@CircuitBreaker(name = "inventory", fallbackMethod = "fallbackMethod")
@TimeLimiter(name = "inventory", fallbackMethod = "fallbackMethod")
@Retry(name = "inventory")
public CompletableFuture<String> placeOrder(@RequestBody OrderRequest orderRequest) {
    return CompletableFuture.supplyAsync(() -> orderService.placeOrder(orderRequest));
}

public CompletableFuture<String> fallbackMethod(OrderRequest orderRequest, RuntimeException ex) {
    return CompletableFuture.completedFuture("Oops! Something went wrong, please order after some time");
}
```

---

### 3. Inventory Service

**Port:** Dynamic (`server.port=0`) — multiple instances supported
**Database:** MySQL (tracks stock levels per SKU code)

Checks whether requested products are available in stock. Designed for **horizontal scalability** — multiple instances register with Eureka and receive load-balanced requests from the Order Service.

**Key implementation details:**
- `server.port=0` forces each instance onto a **random available port**, enabling multiple instances simultaneously
- Registered with **Eureka Discovery Server** under the name `inventory-service`
- Order Service discovers Inventory instances via **Eureka + WebClient load balancing** (`lb://inventory-service`)

---

### 4. Notification Service

**Port:** Dynamic
**Messaging:** Apache Kafka (consumer)

A dedicated, decoupled consumer service that listens for `OrderPlacedEvent` messages on the Kafka `notificationTopic` and handles downstream notifications (e.g., email, SMS).

**Key implementation details:**
- Uses **`@KafkaListener`** annotation to subscribe to the `notificationTopic`
- Completely **decoupled** from the Order Service — communicates only via Kafka, enabling zero-downtime deployments and independent scaling
- Configured with **JsonDeserializer** and type-mapping for safe event deserialization

```java
// NotificationServiceApplication.java — event-driven Kafka consumer
@KafkaListener(topics = "notificationTopic")
public void handleNotification(OrderPlacedEvent orderPlacedEvent) {
    // Send out email notification
    log.info("Received Notification for Order - {}", orderPlacedEvent.getOrderNumber());
}
```

---

## Infrastructure & Cross-Cutting Concerns

---

### Step 1: Service Discovery — Netflix Eureka Server

All microservices automatically register with the **Eureka Discovery Server** on startup. This eliminates hard-coded service URLs and enables **dynamic load balancing** across multiple instances.

**How it works:**
1. The `discovery-server` module runs a standalone Eureka Server (`@EnableEurekaServer`)
2. All other services include `spring-cloud-starter-netflix-eureka-client` and register using `eureka.client.serviceUrl.defaultZone`
3. The API Gateway uses `lb://service-name` URIs to resolve live instances via Eureka at request time

**Screenshot — Eureka Discovery Server Dashboard:**

> 📸 _[Screenshot: Eureka discovery server]_

**Screenshot — Inventory Service registered in Eureka:**

> 📸 _[Screenshot: inventory service mapped to eureka discovery server]_

**Screenshot — All service instances registered:**

> 📸 _[Screenshot: service instances running on eureka server]_

**Screenshot — Multiple Inventory instances on random ports:**

> 📸 _[Screenshot: extra inventory instances run on random ports]_

> 📸 _[Screenshot: with multiple instances of a services]_

---

### Step 2: API Gateway — Spring Cloud Gateway

The **API Gateway** acts as the single entry point for all client traffic. It integrates with Eureka for **client-side load balancing**, handles **JWT authentication**, and proxies routes to backend services — including the Eureka UI itself.

**Routes configured:**

| Route ID | Path | Target |
|---|---|---|
| `product-service` | `/api/product` | `lb://product.service` (Eureka load-balanced) |
| `order-service` | `/api/order` | `lb://order-service` (Eureka load-balanced) |
| `discovery-service` | `/eureka/web` | `http://localhost:8761/` (proxied with `SetPath` filter) |
| `discovery-server-static` | `/eureka/**` | `http://localhost:8761` (static assets proxy) |

**Key implementation details:**
- Built on **Spring WebFlux** (reactive, non-blocking) for high-throughput request handling
- Routes use `lb://` URI scheme — Spring Cloud automatically resolves service instances from Eureka
- Integrated with **Keycloak JWT validation** — all non-Eureka requests require a valid Bearer token
- Exposes Prometheus metrics via `/actuator/prometheus`

```properties
# application.properties — API Gateway route configuration
spring.cloud.gateway.routes[0].id=product-service
spring.cloud.gateway.routes[0].uri=lb://product.service
spring.cloud.gateway.routes[0].predicates[0]=Path=/api/product

spring.cloud.gateway.routes[2].id=discovery-service
spring.cloud.gateway.routes[2].uri=http://localhost:8761
spring.cloud.gateway.routes[2].predicates[0]=Path=/eureka/web
spring.cloud.gateway.routes[2].filters[0]=SetPath=/
```

**Screenshot — Eureka dashboard with API Gateway registered:**

> 📸 _[Screenshot: eureka discovery server with the API gateway]_

**Screenshot — Accessing product service through the API Gateway port (8090) instead of 8082:**

> 📸 _[Screenshot: accessed the API gateway port instead of the product service port]_

**Screenshot — API Gateway route mapped to product service:**

> 📸 _[Screenshot: API gateway route mapped to the product service]_

**Screenshot — API Gateway overview:**

> 📸 _[Screenshot: api gateway]_

**Screenshot — Accessing Eureka Server UI through the API Gateway:**

> 📸 _[Screenshot: Accessing the eureka server through the API gateway]_

**Screenshot — Eureka static resources served via API Gateway:**

> 📸 _[Screenshot: eureka server accessed through the API gateway with static resources]_

---

### Step 3: Security — Keycloak OAuth2 / OIDC

The API Gateway enforces **OAuth2 / OIDC security** using **Keycloak** as the Identity Provider. Every request to a protected route must carry a valid **JWT Bearer token**.

**How it works:**
1. Keycloak issues JWT tokens to authenticated clients (configured realm: `spring-boot-microservices-realm`)
2. The API Gateway validates tokens against Keycloak's JWKS endpoint (via `spring.security.oauth2.resourceserver.jwt.issuer-uri`)
3. A custom **`KeycloakRealmRoleConverter`** extracts both `realm_access.roles` and `resource_access.[client].roles` from the JWT and maps them to Spring Security `GrantedAuthority` objects
4. Only `/eureka/**` paths are publicly accessible; all other routes require authentication

```java
// SecurityConfig.java — Keycloak JWT role extraction
static class KeycloakRealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>> {
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        // Extracts roles from both realm_access and resource_access claims
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        // Maps to ROLE_<roleName> Spring Security authorities
    }
}
```

**Screenshot — Keycloak Client Configuration:**

> 📸 _[Screenshot: keycloak client]_

---

### Step 4: Resilience — Resilience4J Circuit Breaker, Retry & Time Limiter

The **Order Service** protects itself against failures in the **Inventory Service** using three stacked Resilience4J patterns applied via annotations on the `placeOrder` endpoint.

#### Circuit Breaker Pattern
- **CLOSED state:** Requests flow normally
- **OPEN state (triggered):** After a threshold of failures, the circuit opens and all requests immediately return the `fallbackMethod` response — fast failure without waiting
- **HALF-OPEN state:** After a configured wait duration, a limited number of test requests are allowed through to probe if the service has recovered

#### Retry Pattern
- Automatically retries failed requests up to a configured number of attempts before giving up
- Works in conjunction with the circuit breaker — retries happen while the circuit is CLOSED

#### Time Limiter Pattern
- Wraps the async `CompletableFuture` call with a timeout
- If the Inventory Service doesn't respond within the configured duration, a `TimeoutException` is thrown, triggering the fallback

**Screenshot — Resilience4J properties configured for Order Service:**

> 📸 _[Screenshot: resilience4j properties for the order service]_

**Screenshot — Fallback response when circuit is open:**

> 📸 _[Screenshot: fallback error message from the resilience4J]_

**Screenshot — Circuit Breaker in CLOSED/OPEN state:**

> 📸 _[Screenshot: circuit breaker status]_

**Screenshot — Circuit Breaker in HALF-OPEN state (probing recovery):**

> 📸 _[Screenshot: half open circuit breaker status]_

**Screenshot — Timeout exception from Time Limiter:**

> 📸 _[Screenshot: time out exception from the timeout handling from resilience4j for the retry events]_

**Screenshot — Retry events logged by Resilience4J:**

> 📸 _[Screenshot: retry events done by resilience 4j]_

---

### Step 5: Event-Driven Architecture — Apache Kafka

The system uses **Apache Kafka** for asynchronous, event-driven communication between the Order Service and Notification Service — fully decoupling the two services.

**Event Flow:**

```
Order Service                    Kafka Broker                   Notification Service
     │                               │                                │
     │  placeOrder() success         │                                │
     │──────────────────────────────►│  notificationTopic             │
     │  kafkaTemplate.send(...)      │───────────────────────────────►│
     │                               │                                │  @KafkaListener
     │                               │                         ┌──────▼──────┐
     │                               │                         │ handleNotif.│
     │                               │                         │ log order # │
     │                               │                         └─────────────┘
```

**Producer (Order Service):**
- `KafkaTemplate<String, OrderPlacedEvent>` with **JSON serializer**
- Sends `OrderPlacedEvent` (containing `orderNumber`) to `notificationTopic` after every successful order

**Consumer (Notification Service):**
- `@KafkaListener(topics = "notificationTopic")` with **JSON deserializer + type mapping**
- Listens to Confluent Kafka `7.5.0` (Docker) with Zookeeper

**Kafka Infrastructure (Docker Compose):**
- Confluent Platform Kafka `7.5.0` with separate internal (`29092`) and external (`9092`) listener configuration
- Health-checked Zookeeper with Kafka dependent on its healthy state

**Screenshot — Notification Service receiving Kafka events (Terminal):**

> 📸 _[Screenshot: Running notification service - event driven architecture from kafka]_

> 📸 _[Screenshot: notification service running terminal screenshot 2]_

> 📸 _[Screenshot: notification service running terminal screenshot]_

---

### Step 6: Observability — Prometheus & Grafana

Every microservice exposes a **Micrometer Prometheus endpoint** at `/actuator/prometheus`. Prometheus scrapes all services at 15-second intervals, and Grafana provides dashboarding on top.

**Scrape targets configured in `prometheus.yml`:**
- `discovery-server:8761`
- `product-service:8082`
- `inventory-service:8083`
- `order-service:8081`
- `notification-service:8084`
- `api-gateway:8090`

| Component | Port | Purpose |
|---|---|---|
| Prometheus | `9090` | Metrics scraping and time-series storage |
| Grafana | `3000` | Dashboard visualization and alerting |

---

### Step 7: Containerization — Docker & Jib

#### Google Jib (Containerless Docker Build)
All service images are built and pushed to **Docker Hub** using the **Google Jib Maven Plugin** — no Dockerfile required for application services. Jib builds optimized, layered container images directly from the Maven build without needing a Docker daemon running locally.

```xml
<!-- Root pom.xml — Jib plugin pushes images to Docker Hub -->
<plugin>
    <groupId>com.google.cloud.tools</groupId>
    <artifactId>jib-maven-plugin</artifactId>
    <version>3.5.2</version>
    <configuration>
        <from>
            <image>eclipse-temurin:17.0.4.1_1-jre</image>
        </from>
        <to>
            <image>registry.hub.docker.com/isuru071/${project.artifactId}</image>
        </to>
    </configuration>
</plugin>
```

**Published Docker Hub images:**
- `isuru071/discovery-server:latest`
- `isuru071/product-service:latest`
- `isuru071/inventory-service:latest`
- `isuru071/order-service:latest`
- `isuru071/notification-service:latest`
- `isuru071/api-gateway:latest`

**Screenshot — Service images on Docker Hub:**

> 📸 _[Screenshot: service images uploaded to docker hub]_

#### Docker Compose (Full Stack Local Environment)

A single `docker-compose.yml` brings up the **entire platform** with proper dependency ordering and health checks:

```
MySQL ──────────────────────────► Order Service
MongoDB ────────────────────────► Product Service
MySQL ──────────────────────────► Inventory Service
Zookeeper ──► Kafka ────────────► Order Service, Notification Service
Discovery Server ───────────────► All Application Services, API Gateway
All Services ───────────────────► Prometheus, Grafana
```

Every infrastructure service has a **health check** that dependent services wait for before starting, ensuring deterministic startup ordering.

**Screenshot — Docker Compose file:**

> 📸 _[Screenshot: Docker compose file]_

---

## Getting Started

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose
- (Optional) IntelliJ IDEA

### Running the Full Stack with Docker Compose

```bash
# Clone the repository
git clone <repo-url>
cd microservices-new

# Start all services (infrastructure + application)
docker compose up -d

# View logs
docker compose logs -f
```

**Service endpoints after startup:**

| Service | URL |
|---|---|
| API Gateway | http://localhost:8090 |
| Eureka Dashboard | http://localhost:8761 |
| Eureka via Gateway | http://localhost:8090/eureka/web |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Order Service | http://localhost:8081 |

### Running Services Individually (IntelliJ)

Each service is configured with `localhost` fallbacks in `application.properties`, so services can be run directly from IntelliJ against local databases:

```properties
# Example — Order Service falls back to localhost MySQL
spring.datasource.url=${SPRING_DATASOURCE_URL:jdbc:mysql://localhost:3306/order_db?...}
eureka.client.serviceUrl.defaultZone=${EUREKA_CLIENT_SERVICEURL_DEFAULTZONE:http://localhost:8761/eureka}
```

### Building & Pushing Docker Images (Jib)

```bash
# Build and push all images to Docker Hub
mvn compile jib:build -pl product-service,order-service,inventory-service,notification-service,discovery-server,api-gateway
```

### Running Tests

```bash
# Product Service integration tests (uses Testcontainers — requires Docker)
mvn test -pl product-service
```

---

## Project Structure

```
microservices-new/
├── api-gateway/                    # Spring Cloud Gateway + OAuth2 Resource Server
│   └── src/main/
│       ├── java/config/SecurityConfig.java
│       └── resources/application.properties
├── discovery-server/               # Netflix Eureka Server
├── product-service/                # Product CRUD — MongoDB
│   └── src/
│       ├── main/java/.../          # Controller, Service, Model, Repository, DTOs
│       └── test/java/.../          # Testcontainers integration tests
├── order-service/                  # Order orchestration — MySQL + Kafka + Resilience4J
│   └── src/main/java/.../
│       ├── controller/OrderController.java   # Circuit Breaker + Retry + TimeLimiter
│       ├── service/OrderService.java         # WebClient + KafkaTemplate
│       ├── event/OrderPlacedEvent.java
│       └── model/, repository/, dto/
├── inventory-service/              # Stock check — MySQL (multi-instance capable)
├── notification-service/           # Kafka consumer — event-driven notifications
├── infrastructure/
│   ├── mysql-init/                 # DB initialization scripts
│   ├── mongo-init/                 # MongoDB initialization scripts
│   └── prometheus/prometheus.yml   # Prometheus scrape configuration
├── docker-compose.yml              # Full platform orchestration
└── pom.xml                         # Multi-module Maven parent POM
```

---

## Key Engineering Highlights

| Pattern / Concept | Implementation |
|---|---|
| **Service Discovery** | Netflix Eureka — services register on startup, gateway resolves via `lb://` |
| **Client-side Load Balancing** | Spring Cloud LoadBalancer (built into `lb://` URI resolution) |
| **API Gateway** | Spring Cloud Gateway (WebFlux) with predicate-based routing |
| **Circuit Breaker** | Resilience4J `@CircuitBreaker` with CLOSED → OPEN → HALF-OPEN state machine |
| **Retry** | Resilience4J `@Retry` — automatic retry with configurable attempts |
| **Time Limiter** | Resilience4J `@TimeLimiter` — async timeout on `CompletableFuture` |
| **Event Streaming** | Apache Kafka — producer/consumer with JSON serialization |
| **Security** | Keycloak (OAuth2 / OIDC) + Spring Security JWT resource server |
| **Polyglot Persistence** | MongoDB (products), MySQL (orders, inventory) — right database per service |
| **Observability** | Micrometer → Prometheus → Grafana metrics pipeline |
| **Containerization** | Google Jib (Dockerless build) + Docker Compose orchestration |
| **Integration Testing** | Testcontainers — real MongoDB/MySQL containers in test lifecycle |
| **Reactive Programming** | WebFlux `WebClient` for non-blocking inter-service HTTP calls |

---

*Built with Spring Boot, Spring Cloud, and enterprise microservices best practices.*
