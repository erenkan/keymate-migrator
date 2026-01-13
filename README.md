# Keymate Migrator

**Keymate Migrator** is an open-source, high-throughput, restartable migration tool designed specifically to migrate user accounts and identities into [Keycloak](https://www.keycloak.org/) at scale.

Developed and battle-tested during real migrations of 20+ million user identities. Read the full story:
[How Keymate Migrated 20+ Million Identities to Keycloak](https://keymate.io/blog/how_keymate_migrated_20_million_identities).

Keymate Migrator simplifies and secures the process of large-scale [Keycloak migration](https://www.keycloak.org/) for enterprises, SaaS providers, and identity modernization projects. It ensures safe and controlled migrations with **bounded retries**, **operational visibility**, and **zero data loss**, making it the right choice for anyone needing to perform a bulk import of users into Keycloak with confidence.

---

## ✨ Key Features

- **Bounded concurrency**  
  Prevents retry storms and database saturation by enforcing strict parallelism limits.

- **Restartable & resumable**  
  Migration progress is persisted. The process can safely stop and resume without reprocessing completed data.

- **Durable requeue mechanism**  
  Failed records are written to a disk-backed segmented outbox before being retried.

- **Backoff-aware retry strategy**  
  Retries are delayed with controlled backoff to protect downstream systems.

- **Reactive, non-blocking design**  
  Built on Quarkus, Mutiny, and reactive PostgreSQL clients.

- **Operational visibility**  
  Clear insight into progress, failures, retry states, and system pressure points.

---

## 🧠 Design Principles

This migrator intentionally avoids common large-scale migration anti-patterns:

| Anti-pattern | This project |
|--------------|-------------|
| Unlimited retries | ❌ Explicitly prevented |
| Fire-and-forget writes | ❌ All failures persisted |
| Blind parallelism | ❌ Bounded concurrency |
| In-memory retry buffers | ❌ Disk-backed outbox |
| Manual recovery | ❌ Automatic resume |

The guiding philosophy:

> **Throughput is determined by the noisiest bottleneck, not by the number of threads.**

---

## 🏗 Architecture Overview

```mermaid
flowchart TD
A[Source Database] --> B[Work Queue (PostgreSQL)]

    B --> C[Processing Loop]

    C -->|Claim batch| D[Chunk by Concurrency]
    D -->|Route jobs| E[Job Router]

    E --> F[Keycloak Ingestion API]

    F -->|Success| G[Mark Completed]

    F -->|Failure| H[Failure Handler]
    H --> I[Compute Backoff]
    I --> J[Durable Outbox (Segmented JSONL)]
    J --> K[Requeue to Work Queue]
    K --> B
```
---

## 📦 Technology Stack

- Java 21+
- Quarkus
- Mutiny (Reactive programming)
- PostgreSQL (reactive client)
- Jackson

---

## 🚀 Getting Started

### Prerequisites

- Java 21+
- PostgreSQL

---

### Configuration

All configuration is done via `application.properties`.

Key settings to review carefully:

```properties
# Concurrency control
app.concurrency=64
app.claimers=32

# Reactive DB pool
quarkus.datasource.reactive.max-size=100

# HTTP / Netty tuning
quarkus.http.tcp-quick-ack=true
```

> ⚠️ Important
Increasing concurrency does not automatically increase throughput.
Always tune these values together with database limits and Keycloak capacity.

## ▶️ Running the Migrator

```bash
./mvnw quarkus:run
```

The migrator:

1. Claims work from the queue
2. Processes jobs with bounded parallelism
3. Marks success or persists failures
4. Requeues failed items with backoff
5. Continues safely until completion

## 🔄 Failure & Retry Model

- Failures are never lost
- Each failure is:
  - recorded with error context
  - assigned a next-retry timestamp
  - persisted before retrying 
- Retries are deliberately delayed, not immediate

This ensures:
- no uncontrolled retry storms
- no silent data loss
- predictable load on Keycloak and the database

## 🧪 Simulation Guide

This project includes a simulation environment to test the migrator's capabilities without needing a real Keycloak instance or production data.

### 1. Mock REST Server

The `mock-rest-server` module acts as a target system (like Keycloak) that simulates:
- Variable processing delays (`mock.max-delay-ms`)
- Random failures (`mock.error-rate`)
- Client vs. Server errors (`mock.client-error-share`)

To run the mock server:

```bash
cd mock-rest-server
./mvnw quarkus:run -Dquarkus.http.port=8081
```

### 2. Database Setup & Data Generation

The `migrator` module contains SQL scripts to set up the database schema and generate synthetic data.

1.  **Initialize Schema:**
    Run `src/main/resources/db/migration/V1__init.sql` to create the necessary tables (`orders`, `invoices`, `work_queue`, `processed_log`).

2.  **Generate Data:**
    Run `src/main/resources/db/migration/V1__load.sql` to populate the `orders` and `invoices` tables with millions of random records. This script is optimized for bulk loading.

3.  **Enqueue Work:**
    Run `src/main/resources/db/migration/V1__enqueue.sql` to fill the `work_queue` table with jobs derived from the generated data.

### 3. Running the Simulation

Once the database is ready and the mock server is running:

1.  Configure `migrator/src/main/resources/application.properties` to point to your local PostgreSQL instance.
2.  Ensure `quarkus.rest-client."order-api".url` and `quarkus.rest-client."invoice-api".url` point to your mock server (default is `http://localhost:8081`).
3.  Run the migrator:

```bash
cd migrator
./mvnw quarkus:run
```

You can observe the migration progress, retry mechanisms, and error handling in real-time.

## 🤝 Contributing

Contributions are welcome!

Please ensure:

- commits are signed (Signed-off-by)
- changes are well-documented
- concurrency and retry semantics are preserved

## 📄 License

This project is licensed under the **Apache License 2.0.**

## 📚 Background

This project is inspired by real production migrations involving tens of millions of records.
A detailed write-up describing the lessons learned during such a migration is available separately.

## ⚠️ Disclaimer

This tool is provided as-is.
Always test thoroughly in non-production environments before running large-scale migrations.
