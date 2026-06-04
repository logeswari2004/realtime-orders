# Real-Time Order Notification System

A backend service that pushes database changes to connected clients **instantly** — no polling, no delays.

---

## Requirements Checklist

Every item from the assignment specification is addressed below.

| # | Requirement | Status | How It Is Met |
|---|---|---|---|
| 1 | Table `orders` with fields `id`, `customer_name`, `product_name`, `status`, `updated_at` | ✅ | Defined in `sql/schema.sql`; mapped to `Order.java` JPA entity |
| 2 | Any INSERT / UPDATE / DELETE triggers a client update | ✅ | PostgreSQL trigger `order_change_trigger` fires `pg_notify()` on all three operations |
| 3 | System must **not** rely on frequent polling | ✅ | `PostgresNotifyListener` uses `LISTEN/NOTIFY` — the database pushes; no `SELECT` query is issued per tick |
| 4 | Working backend service that listens for DB changes and pushes to clients | ✅ | Spring Boot (`PostgresNotifyListener` + WebSocket STOMP broker) running on port 8080 |
| 5 | Simple client that shows updates in real time | ✅ | Two clients delivered: browser dashboard (`index.html`) and Python CLI (`client/cli_client.py`) |
| 6 | README explaining approach, how to run, and why the method was chosen | ✅ | This document — see sections 1, 3, 5, and 8 |
| 7 | Design thinking around scalability and efficiency | ✅ | See Section 7 — RabbitMQ upgrade path, Debezium/CDC, and Outbox pattern all discussed |

---

## Beyond the Requirements

The following features were added to demonstrate production-readiness and real-world design thinking. None of these were required by the assignment.

| Feature | Why It Was Added |
|---|---|
| **One-directional status flow** (`pending → shipped → delivered`) | Enforces business rules at both the API layer (`OrderController.isValidTransition()`) and the UI — invalid transitions are rejected even via direct API calls |
| **Auto-reconnect on WebSocket disconnect** | Production systems must recover from transient network failures without a manual page refresh |
| **Live statistics panel** | Demonstrates that the real-time layer can drive derived state (counts), not just raw card updates |
| **Search and filter** | Shows awareness of UX and that real-time data still needs to be navigable at scale |
| **Delete confirmation modal with fade-out animation** | Prevents accidental data loss; optimistic UI pattern |
| **Browser Notifications API** | Notifies users of order changes even when the tab is in the background |
| **Tab title badge** | `(3 pending) Order Dashboard` — ambient awareness without switching tabs |
| **Python CLI client** | Proves the WebSocket layer is protocol-agnostic — any STOMP client works, not just the browser |

---

## Table of Contents

1. [Architecture & Design Decisions](#1-architecture--design-decisions)
2. [Project Structure](#2-project-structure)
3. [How It Works – Step by Step](#3-how-it-works--step-by-step)
4. [Prerequisites](#4-prerequisites)
5. [Setup & Run](#5-setup--run)
6. [Using the System](#6-using-the-system)
7. [Scalability Discussion](#7-scalability-discussion)
8. [Trade-offs & Alternatives Considered](#8-trade-offs--alternatives-considered)

---

## 1. Architecture & Design Decisions

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                         │
│                                                             │
│   Browser (index.html)            Python CLI (cli_client.py)│
│   SockJS + STOMP                  stomp.py library          │
└──────────────────────┬──────────────────────────────────────┘
                       │  WebSocket / STOMP
                       │  /topic/orders
┌──────────────────────▼──────────────────────────────────────┐
│                    SPRING BOOT SERVER                        │
│                                                             │
│  ┌──────────────────┐    ┌─────────────────────────────┐   │
│  │  OrderController │    │  PostgresNotifyListener      │   │
│  │  REST API        │    │  (background thread)         │   │
│  │  /api/orders     │    │  LISTENS on pg channel       │   │
│  └────────┬─────────┘    └──────────────┬──────────────┘   │
│           │                             │                   │
│           │ JPA/SQL                     │ SimpMessagingTemplate│
│           │                             ▼                   │
│           │                  ┌────────────────────┐         │
│           │                  │  WebSocket Broker  │         │
│           │                  │  (in-memory STOMP) │         │
│           │                  └────────────────────┘         │
└───────────┼─────────────────────────────────────────────────┘
            │ JDBC
┌───────────▼─────────────────────────────────────────────────┐
│                      POSTGRESQL                             │
│                                                             │
│  ┌──────────────┐    INSERT/UPDATE/DELETE                   │
│  │  orders      │ ──────────────────────► TRIGGER           │
│  │  table       │                            │              │
│  └──────────────┘                            ▼              │
│                                       pg_notify()           │
│                                    'order_changes' channel   │
└─────────────────────────────────────────────────────────────┘
```

### Why PostgreSQL LISTEN/NOTIFY?

The core challenge is: **how does the server know when the database has changed, without constantly querying it?**

Three common approaches:

| Approach | How it works | Problem |
|---|---|---|
| **Client polling** | Client calls `/api/orders` every N seconds | Wastes bandwidth; stale data between polls |
| **Server polling** | Server queries DB every N seconds | Same problem, just shifted to the server |
| **DB push (chosen)** | DB notifies server the moment a row changes | Zero unnecessary queries; true real-time |

PostgreSQL's `LISTEN/NOTIFY` is a **built-in pub/sub system**. A trigger function calls `pg_notify('order_changes', payload)` after every INSERT/UPDATE/DELETE. A dedicated JDBC connection in the Spring Boot app has issued `LISTEN order_changes` and receives these notifications the moment they are emitted — no polling of the orders table is needed.

The `PGConnection.getNotifications()` call every 500 ms is **not a database query**. It simply checks a local buffer that the PostgreSQL JDBC driver maintains. This is orders of magnitude cheaper than `SELECT * FROM orders` on every tick.

### Why WebSocket + STOMP?

- **WebSocket** keeps a persistent TCP connection open, allowing the server to push messages to the browser at any time — impossible with plain HTTP.
- **STOMP** (Simple Text Oriented Messaging Protocol) adds topic-based routing on top of WebSocket, so multiple clients can subscribe to `/topic/orders` independently without the server managing them manually.
- **SockJS** provides automatic fallback for clients that can't use native WebSocket (e.g., some corporate proxies).

---

## 2. Project Structure

```
realtime-orders/
├── pom.xml                                  Maven build descriptor
│
├── sql/
│   └── schema.sql                           DB setup: table, trigger, seed data
│
├── src/main/
│   ├── java/com/orders/
│   │   ├── RealtimeOrdersApplication.java   Spring Boot entry point
│   │   ├── config/
│   │   │   └── WebSocketConfig.java         STOMP endpoint + broker config
│   │   ├── controller/
│   │   │   └── OrderController.java         REST API (GET/POST/PATCH/DELETE)
│   │   ├── listener/
│   │   │   └── PostgresNotifyListener.java  Core: LISTEN/NOTIFY → WebSocket bridge
│   │   ├── model/
│   │   │   ├── Order.java                   JPA entity
│   │   │   └── OrderEvent.java              DTO for trigger payloads
│   │   └── service/
│   │       └── OrderRepository.java         Spring Data JPA repository
│   └── resources/
│       ├── application.properties           DB + server config
│       └── static/
│           └── index.html                   Browser dashboard (served by Spring Boot)
│
└── client/
    └── cli_client.py                        Python terminal client
```

---

## 3. How It Works – Step by Step

1. **App starts** → `PostgresNotifyListener` opens a dedicated JDBC connection and executes `LISTEN order_changes`.

2. **Client opens browser** → Connects to `/ws` via SockJS, upgrades to WebSocket, subscribes to `/topic/orders` via STOMP. Browser also calls `GET /api/orders` to populate the initial table.

3. **Someone updates an order** (via browser UI, CLI, or any SQL client):
   ```sql
   UPDATE orders SET status = 'shipped' WHERE id = 1;
   ```

4. **PostgreSQL trigger fires** → calls `pg_notify('order_changes', '{"operation":"UPDATE","id":1,...}')`.

5. **Spring Boot listener receives notification** (within the next 500 ms poll tick) → deserializes the JSON payload into an `OrderEvent`.

6. **`SimpMessagingTemplate.convertAndSend("/topic/orders", event)`** → Spring's STOMP broker fans the message out to **all connected subscribers**.

7. **Every open browser tab receives the event** → the card for order #1 updates and flashes blue in real time, with no page refresh.

---

## 4. Prerequisites

| Tool | Version |
|---|---|
| Java | 21+ |
| Maven | 3.8+ |
| PostgreSQL | 13+ |
| Python | 3.8+ (for CLI client only) |

---

## 5. Setup & Run

### Step 1 – Create the database

```bash
psql -U postgres -c "CREATE DATABASE ordersdb;"
psql -U postgres -d ordersdb -f sql/schema.sql
```

This creates the `orders` table, the trigger function, and seeds three sample rows.

### Step 2 – Configure the database connection

Edit `src/main/resources/application.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/ordersdb
spring.datasource.username=your_pg_username
spring.datasource.password=your_pg_password
```

### Step 3 – Build and run

```bash
mvn clean package -DskipTests
java -jar target/realtime-orders-1.0.0.jar
```

Or with Maven directly:

```bash
mvn spring-boot:run
```

The server starts on **http://localhost:8080**.

### Step 4 – Open the dashboard

Open **http://localhost:8080** in your browser (or multiple tabs to see the fan-out).

### Step 5 (optional) – Python CLI client

```bash
pip install stomp.py requests
python client/cli_client.py
```

Type `help` at the prompt to see available commands.

---

## 6. Using the System

### Browser Dashboard

- **Add Order** – fill in the form at the top and click "Add Order".
- **Change Status** – use the dropdown on any card.
- **Delete** – click the ✕ button.
- Every action in one tab is reflected in **all open tabs within milliseconds**.

### CLI Client

```
orders> list                          # view all current orders
orders> add "Diana Prince" "Monitor"  # create a pending order
orders> status 4 shipped              # update order #4
orders> delete 4                      # delete order #4
```

Live events from other clients (browser, psql, etc.) will appear above the prompt as they arrive.

### Direct SQL (great for testing)

```sql
-- In psql or any SQL client:
INSERT INTO orders (customer_name, product_name, status)
VALUES ('Test User', 'Mechanical Keyboard', 'pending');

UPDATE orders SET status = 'delivered' WHERE id = 1;

DELETE FROM orders WHERE id = 2;
```

Every statement instantly pushes an event to all connected clients.

---

## 7. Scalability Discussion

### Current design (single instance)

The in-memory STOMP broker works perfectly for a single Spring Boot instance. Suitable for moderate load (hundreds of concurrent connections).

### Scaling to multiple instances

When running multiple Spring Boot instances behind a load balancer, the in-memory broker breaks: a notification received by instance A won't reach clients connected to instance B.

**Solution: Replace the in-memory broker with RabbitMQ or Redis Pub/Sub.**

```
Instance A (receives NOTIFY) → publishes to RabbitMQ topic exchange
                             ← Instance B subscribes → broadcasts to its clients
                             ← Instance C subscribes → broadcasts to its clients
```

Spring's STOMP support has first-class RabbitMQ integration via `enableStompBrokerRelay()`. Switching requires changing roughly 10 lines in `WebSocketConfig.java` — the rest of the code is unchanged.

### Database scaling

`LISTEN/NOTIFY` is a per-connection feature and does not replicate to read replicas. In a high-write environment, consider:

- **Debezium** (CDC): tails the PostgreSQL WAL log and publishes to Kafka. Scales to millions of events/second, but adds infrastructure complexity.
- **Outbox pattern**: application writes change events to a side table; a separate relay process reads and publishes them.

For the scope of this assignment (a corporate banking platform with hundreds of concurrent users), the current approach is perfectly adequate and avoids unnecessary infrastructure.

---

## 8. Trade-offs & Alternatives Considered

| Option | Pros | Cons | Decision |
|---|---|---|---|
| **PostgreSQL LISTEN/NOTIFY** ✓ | Zero extra infra; built into the DB; trigger fires atomically with the write | Single-instance; not replicated | **Chosen** – fits the scope, matches tech stack |
| Debezium + Kafka | Handles massive scale; no trigger overhead | Needs Kafka cluster; high operational complexity | Overkill for this problem |
| HTTP Long-Polling | Works without WebSocket | Wastes server threads; high latency | Rejected |
| Server-Sent Events (SSE) | Simpler than WebSocket; native browser support | Unidirectional only; less ecosystem support | WebSocket chosen for bidirectionality |
| Application-level publish | Server publishes after each REST call | Misses changes made directly via SQL | Rejected – trigger is source-of-truth |

The trigger-based approach is the most robust choice here: **any** change to the `orders` table — regardless of what wrote it (REST API, migration script, DBA, another service) — is propagated to clients. The application layer is not the sole arbiter of what constitutes a "change."
