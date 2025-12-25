# Architecture Decisions – Travel Agency Lakehouse Platform

## Document Overview

**Document Version:** 1.0  
**Last Updated:** December 2025  
**Status:** Active  
**Reviewers:** Data Engineering Team

This document outlines the architectural decisions, design patterns, and data flow strategy for the Travel Agency Lakehouse Platform. It serves as the primary reference for understanding system components, data movement, and technical rationale.

---

## Executive Summary

The Travel Agency Lakehouse Platform implements a modern data architecture that unifies batch and streaming workloads using the medallion architecture pattern (Bronze-Silver-Gold). The platform is designed to support both operational analytics and strategic business intelligence while maintaining data quality, scalability, and cost-efficiency.

**Key Capabilities:**
- Unified batch and streaming data processing
- Multi-layer data quality framework with quarantine zone
- Real-time event processing and analytics
- Self-service BI and monitoring capabilities
- Cloud-native, scalable infrastructure

---

## 1. Architecture Overview

### 1.1 Design Principles

- **Separation of Concerns:** Clear delineation between ingestion, processing, storage, and serving layers
- **Schema Evolution:** Support for schema changes without breaking downstream consumers
- **Incremental Processing:** Delta Lake enables efficient incremental updates and time travel
- **Data Quality First:** Built-in validation with quarantine zone for anomalous records
- **Hybrid Processing:** Support for both batch and streaming workloads on unified infrastructure

### 1.2 Technology Stack Rationale

| Component | Technology | Justification |
|-----------|-----------|---------------|
| Storage Layer | Amazon S3 + Delta Lake | Cost-effective object storage with ACID transactions and time travel |
| Batch Ingestion | Airbyte | Open-source, extensive connector library, declarative configuration |
| Stream Processing | Apache Kafka + Spark Structured Streaming | Industry standard for event streaming with exactly-once semantics |
| Orchestration | Apache Airflow | Python-native, extensive operator library, robust scheduling |
| Serving Layer | Apache Hive + Cassandra | SQL analytics (Hive) and low-latency serving (Cassandra) |
| Visualization | Power BI + Grafana | Enterprise BI (Power BI) and operational monitoring (Grafana) |
| Compute Engine | Databricks + Apache Spark | Managed Spark with collaborative notebooks and cluster management |

---

## 2. Data Ingestion Architecture

### 2.1 Batch Ingestion Pipeline

**Source Systems:**
- PostgreSQL OLTP database (bookings, customers, flights)
- File-based sources (CSV files, metadata from PNG files)

**Ingestion Tool: Airbyte**

**Decision Rationale:**
- **Connector Availability:** Native PostgreSQL and S3 connectors eliminate custom development
- **Change Data Capture (CDC):** Supports incremental replication using PostgreSQL WAL
- **Schema Management:** Automatic schema detection and evolution handling
- **Monitoring:** Built-in data quality checks and sync failure notifications

**Data Flow:**
1. Airbyte establishes connection to PostgreSQL source
2. Initial full snapshot followed by incremental CDC-based syncs
3. Data written to S3 Bronze layer in Delta Lake format
4. Metadata tracked in Airbyte's internal catalog
5. Sync frequency: Configurable (default: hourly for transactional data)

**File Ingestion:**
- CSV files dropped to S3 landing zone
- Airflow triggers movement and processing of thw data in the landing zone
- PNG metadata extracted and stored as structured data

### 2.2 Streaming Ingestion Pipeline

**Source Systems:**
- External flight status APIs
- Internal event-driven microservices
- Real-time booking events

**Technology Stack: Apache Kafka**

**Decision Rationale:**
- **Durability:** Event retention allows replay and recovery
- **Scalability:** Horizontal scaling through topic partitioning
- **Decoupling:** Producers and consumers operate independently
- **Integration:** Native Spark Structured Streaming integration

**Topic Design:**
- `flight.events.raw` – Raw flight status updates
- `booking.events.raw` – Real-time booking transactions
- `api.events.raw` – External API responses

**Data Flow:**
1. Producers publish events to Kafka topics (Avro serialization)
2. Spark Structured Streaming consumes from topics
3. Stream processing applies transformations and validations
4. Results written to Cassandra for low-latency serving
5. Checkpoint mechanism ensures exactly-once processing

**Schema Management:**
- Avro schemas stored in `/ingestion/streaming/schemas/`
- Schema evolution follows backward compatibility rules

---

## 3. Lakehouse Storage Architecture (Medallion Pattern)

### 3.1 Bronze Layer (Raw Data Zone)

**Purpose:** Persist raw, unprocessed data exactly as received from source systems

**Characteristics:**
- Schema-on-read approach
- No data transformations or cleansing
- Complete data lineage preservation
- Immutable append-only storage

**Storage Format:** Delta Lake
- ACID transaction support
- Time travel for auditing
- Schema enforcement at write time

**Data Retention:** 90 days (configurable based on compliance requirements)

**Directory Structure:**
```
s3://lakehouse-bronze/
├── bookings/
│   ├── year=2025/
│   │   └── month=12/
├── flights/
└── customers/
```

### 3.2 Silver Layer (Cleaned and Conformed Zone)

**Purpose:** Provide validated, cleaned, and business-ready datasets

**Transformation Logic:**
- **Data Quality Checks:**
  - Null value handling
  - Referential integrity validation
  - Data type conformance
  - Business rule validation (e.g., booking_date <= departure_date)

- **Deduplication:** Primary key-based deduplication using `MERGE` operations
- **Standardization:** Consistent naming conventions, date formats, currency codes
- **Enrichment:** Join operations to add reference data

**Processing Tool:** Apache Spark (Databricks)

**Data Flow:**
1. Spark reads from Bronze Delta tables
2. Validation framework applies configurable rules
3. Valid records written to Silver layer
4. Invalid records routed to Quarantine zone with failure metadata
5. Incremental processing using Delta Lake's change data feed

**Storage Format:** Delta Lake with optimized partitioning

### 3.3 Gold Layer (Analytics-Ready Zone)

**Purpose:** Serve aggregated, business-specific analytical datasets

**Use Cases:**
- Daily revenue metrics by route and customer segment
- Booking conversion funnel analytics
- Flight occupancy trends
- Customer lifetime value calculations

**Characteristics:**
- Highly denormalized for query performance
- Pre-computed aggregations
- SCD Type 2 for historical tracking where needed
- Optimized for BI tool consumption

**Processing Frequency:**
- Daily batch jobs (Airflow-orchestrated)
- Incremental refresh for frequently updated metrics

### 3.4 Quarantine Zone (Data Quality Management)

**Purpose:** Isolate and track records that fail validation

**Metadata Captured:**
- Original record content
- Validation failure reason
- Timestamp and processing batch ID
- Source system identifier

**Workflow:**
1. Failed records written to quarantine with error metadata
2. Data quality dashboard tracks failure trends
3. Manual review and remediation process
4. Corrected records re-ingested through Bronze layer

---

## 4. Processing Layer

### 4.1 Batch Processing

**Technology:** Apache Spark on Databricks

**Job Types:**
- **Bronze → Silver:** Data cleansing and validation (`bronze_to_silver.py`)
- **Silver → Gold:** Aggregations and analytics (`silver_to_gold.py`)
- **Backfill Jobs:** Historical data reprocessing

**Optimization Strategies:**
- **Partition Pruning:** Date-based partitioning for efficient reads
- **Z-Ordering:** Co-locate related data for faster queries
- **Adaptive Query Execution:** Dynamic optimization based on runtime statistics
- **Data Skipping:** Delta Lake file statistics minimize data scanned

**Resource Management:**
- Auto-scaling clusters based on workload
- Spot instance usage for cost optimization
- Job prioritization through Airflow task dependencies

### 4.2 Stream Processing

**Technology:** Spark Structured Streaming

**Processing Patterns:**
- **Stateless Transformations:** Filtering, projection, validation
- **Stateful Aggregations:** Windowed metrics (tumbling, sliding windows)
- **Stream-Stream Joins:** Enrichment with reference streams
- **Stream-Batch Joins:** Joining with Delta Lake dimension tables

**Fault Tolerance:**
- Checkpoint location on S3 for state recovery
- Write-ahead logs for exactly-once semantics
- Kafka offset management

**Key Pipelines:**
- `kafka_to_cassandra.py` – Real-time event persistence
- `aggregations.py` – Streaming metrics computation

---

## 5. Orchestration Layer

**Technology:** Apache Airflow

**Design Philosophy:**
- **DAG Structure:** Modular, reusable task definitions
- **Dependency Management:** Clear task dependencies with proper sensor usage
- **Error Handling:** Automatic retry logic with exponential backoff
- **Monitoring:** Slack/email notifications on failure

**Primary DAGs:**

1. **`batch_pipeline_dag.py`**
   - Schedules: Daily at 2:00 AM UTC
   - Tasks: Bronze → Silver → Gold transformations
   - SLA: 4 hours

2. **`daily_cumulative_events_dag.py`**
   - Aggregates streaming events into daily summaries
   - Bridges real-time and batch analytics

**Deployment:**
- Docker-based Airflow deployment
- Version-controlled DAG definitions in Git
- CI/CD pipeline for DAG validation and deployment

---

## 6. Serving Layer

### 6.1 Analytical Serving (Hive + Power BI)

**Apache Hive:**
- External tables on Delta Lake Gold layer
- SQL interface for data analysts
- Predefined views for common queries

**Power BI:**
- DirectQuery mode for real-time dashboards
- Scheduled refresh for aggregated reports
- Row-level security based on user roles

**Key Dashboards:**
- Executive Revenue Dashboard
- Operational Booking Metrics
- Flight Performance Analytics

### 6.2 Operational Serving (Cassandra)

**Use Cases:**
- Real-time flight status lookup
- Customer profile retrieval
- Event history queries

**Schema Design:**
- Denormalized tables optimized for query patterns
- Time-series data with TTL for automatic cleanup
- Replication factor: 3 for high availability

**Data Flow:**
- Spark Structured Streaming writes to Cassandra
- Application APIs query Cassandra directly
- Sub-10ms query latency for key lookups

---

## 7. Monitoring and Observability

### 7.1 Data Quality Monitoring

- Quarantine zone record counts
- Schema evolution tracking
- Data freshness metrics (data arrival time vs. processing time)

### 7.2 Pipeline Monitoring

**Grafana Dashboards:**
- Airflow DAG run success rates
- Spark job execution times
- Kafka consumer lag
- Cassandra read/write latency

**Alerting:**
- PagerDuty integration for critical failures
- Slack notifications for DAG failures
- Automated incident creation in ticketing system

### 7.3 Cost Monitoring

- S3 storage costs by layer
- Databricks compute costs by job
- Kafka cluster utilization
- Reserved vs. on-demand instance analysis

---

### 8. Environment Strategy

- **Local:** Docker Compose for development
- **Staging:** Databricks Community Edition + AWS Free Tier
- **Production:** Full Databricks + AWS production resources

---

### 9. Future Enhancements

- Real-time data quality scoring
- ML model integration for predictive analytics
- Multi-region replication for disaster recovery
- Data mesh architecture for domain ownership

---

## 10. References and Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Lakehouse Architecture](https://databricks.com/glossary/data-lakehouse)
- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Kafka Design Patterns](https://kafka.apache.org/documentation/)

---

