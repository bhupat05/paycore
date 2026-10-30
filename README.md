# PayCore

An event-driven payment processing engine and double-entry ledger built with Spring Boot, Apache Kafka, and PostgreSQL.

PayCore separates fast payment intake from asynchronous background settlement using the **Transactional Outbox pattern**. Payments return `202 Accepted` immediately, while background consumers handle atomic balance transfers, double-entry bookkeeping, and idempotency guarantees.

---

### Features

- **Asynchronous Settlement:** API intake writes a pending record and returns instantly; Kafka consumers process fund transfers in the background.
- **Double-Entry Ledger:** Every transfer writes balanced debit and credit entries atomically.
- **Deadlock-Free Transfers:** Concurrent bidirectional transfers use deterministic, ordered pessimistic locking (`SELECT ... FOR UPDATE`) to avoid deadlocks.
- **End-to-End Idempotency:** Redis caches submission requests (via `Idempotency-Key`), and database-level tracking prevents duplicate consumer execution.
- **Real-Time Dashboard:** A lightweight React dashboard to view accounts, balances, and payment status updates live.

---

### Tech Stack

- **Backend:** Java 21, Spring Boot 3.3, Spring Data JPA, Spring Kafka
- **Data & Streaming:** PostgreSQL 16, Apache Kafka (KRaft), Redis 7, Flyway
- **Frontend:** React 18, Vite
- **Testing:** Testcontainers, JUnit 5, k6

---

### Getting Started

#### 1. Start Services
Start PostgreSQL, Kafka, and Redis via Docker Compose:
```bash
docker compose up -d
```

#### 2. Run the Application
```bash
mvn spring-boot:run
```
The API starts on `http://localhost:8080`.

- **Swagger UI:** [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)
- **Kafka UI:** [http://localhost:8081](http://localhost:8081)
- **Actuator Health:** [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)

#### 3. Run the Dashboard (Optional)
```bash
cd frontend
npm install
npm run dev
```
Open [http://localhost:5173](http://localhost:5173).

---

### Quick Example

**Submit a transfer:**
```bash
curl -i -X POST http://localhost:8080/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "fromAccountId": "ALICE_ACCOUNT_ID",
    "toAccountId": "BOB_ACCOUNT_ID",
    "amount": 50.00,
    "currency": "USD"
  }'
```
Returns `202 Accepted` with status `PENDING`. Within milliseconds, the Kafka consumer settles the transaction and transitions the payment to `COMPLETED`.

---

### Testing & Verification

Run unit and integration tests (uses Testcontainers for Postgres and Kafka):
```bash
mvn clean verify
```

To run load tests with k6, check out [`loadtest/`](loadtest/README.md).

---

### Architecture & Design

For in-depth details on the request flow, concurrency control, and outbox mechanics, see [docs/architecture.md](docs/architecture.md).
