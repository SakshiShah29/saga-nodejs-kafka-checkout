# Saga Checkout - Distributed Transaction Pattern with Kafka

A complete implementation of the **Saga Orchestration Pattern** for handling distributed transactions in a microservices architecture using Apache Kafka.

## 🎯 Project Overview

This project demonstrates how to handle a checkout process across multiple microservices while maintaining data consistency, even when individual services fail.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Topics: ORDER_SERVICE, INVENTORY_SERVICE,              │   │
│  │          PAYMENT_SERVICE, SERVICE_REPLY                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐   ┌─────────────────┐   ┌───────────────┐
│Order Service  │   │Inventory Service│   │Payment Service│
│   (MongoDB)   │   │    (MongoDB)    │   │   (MongoDB)   │
└───────────────┘   └─────────────────┘   └───────────────┘
        ▲                     ▲                     ▲
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                    ┌─────────────────┐
                    │  ORCHESTRATOR   │
                    │ (Saga Manager)  │
                    └─────────────────┘
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| Order Service | 3001 | Creates and manages orders |
| Inventory Service | 3002 | Manages stock reservation/release |
| Payment Service | 3003 | Processes payments and refunds |
| Orchestrator | 3004 | Coordinates the saga flow |

### Tech Stack

- **Runtime:** Node.js 18+ with TypeScript
- **Database:** MongoDB 6.0
- **Message Broker:** Apache Kafka (via KafkaJS)
- **Containerization:** Docker & Docker Compose

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- Docker & Docker Compose
- Git

### Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd saga-checkout

# Start infrastructure (MongoDB, Kafka, Zookeeper)
docker compose up -d

# Wait ~30 seconds for Kafka to be ready, then create topics
cd kafkaBroker && npm install && npm run create-topics
```

### Development URLs

| Service | URL |
|---------|-----|
| MongoDB | `localhost:27017` |
| Kafka | `localhost:9092` |
| Kafka UI | http://localhost:8080 |
| Mongo Express | http://localhost:8081 (admin/admin123) |

## 📊 Saga Flow

### Kafka Topics

| Topic | Purpose |
|-------|---------|
| `ORDER_SERVICE` | Commands to Order Service |
| `INVENTORY_SERVICE` | Commands to Inventory Service |
| `PAYMENT_SERVICE` | Commands to Payment Service |
| `SERVICE_REPLY` | Replies from all services to Orchestrator |

### Success Flow

```
Orchestrator                    Services
     │
     ├──► ORDER_SERVICE ──────► Order Service
     │                              │
     │◄── SERVICE_REPLY ◄───────────┘ (ORDER_CREATED)
     │
     ├──► INVENTORY_SERVICE ──► Inventory Service
     │                              │
     │◄── SERVICE_REPLY ◄───────────┘ (INVENTORY_RESERVED)
     │
     ├──► PAYMENT_SERVICE ────► Payment Service
     │                              │
     │◄── SERVICE_REPLY ◄───────────┘ (PAYMENT_COMPLETED)
     │
     └──► ORDER_SERVICE ──────► Order Service (COMPLETE)
```

### Failure + Compensation

```
If PAYMENT fails:
     │
     ├──► INVENTORY_SERVICE ──► Release Inventory (COMPENSATE)
     │
     └──► ORDER_SERVICE ──────► Cancel Order (COMPENSATE)
```

## 📁 Project Structure

```
saga-checkout/
├── services/
│   ├── order-service/       # Order management
│   ├── inventory-service/   # Stock management
│   ├── payment-service/     # Payment processing
│   ├── orchestrator-service/# Saga coordinator
│   └── shared/              # Shared types & Kafka utilities
├── kafkaBroker/             # Topic management
├── docker/                  # Docker configurations
├── scripts/                 # Utility scripts
└── docker-compose.yml