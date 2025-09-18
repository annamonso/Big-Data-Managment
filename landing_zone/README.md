# README for Landing Zone Data Lakehouse

## Overview

This repository implements the **Landing Zone Data Lakehouse** architecture, serving as the initial repository for raw ingested data. We adopted **Delta Lake**, an open-source storage layer built on **Apache Spark**, to combine the flexibility of data lakes with the governance and reliability of data warehouses. This approach enables:

- **ACID transactions**
- **Schema enforcement and evolution**
- **Time travel**

Delta Lake ensures data integrity and consistency, even in the face of concurrent writes or failures, and supports large-scale analytics at lower storage costs. Unlike traditional data storage systems like HDFS or cloud object storage, Delta Lake provides native support for transactions, schema validation, and efficient metadata handling.

---

## Data Organization Protocol

To avoid the common issue of unmanaged data accumulation (known as a **data swamp**), our data lakehouse follows a strict **data organization protocol**. This protocol ensures:

- Proper **naming conventions**
- Directory **partitioning strategies**
- **Version control** and **metadata tracking**

The data lakehouse is divided into two main directories: **batch** and **streaming**.

---

## Directory Structure

### Batch Directory

The **batch** directory contains data ingested in batch mode. A new subdirectory is created for each new data source, following the naming convention:

```
    <data_source_name+timestamp>
```


Each of these subdirectories contains:

1. **`_delta_log/` folder**: Contains Delta Lake transaction log files (`<version>.json`) that record ingestion actions (e.g., adding files, schema changes). The `<version>.json.crc` checksum files ensure data integrity. These logs enable features like versioning and time travel, providing valuable metadata for traceability.

2. **`.parquet` files**: Store ingested data in an optimized columnar format. The `.parquet.crc` checksum files verify the integrity of the data, ensuring it hasn't been corrupted during storage or transfer.

### Streaming Directory

The **streaming** directory contains data ingested in real-time. Each data source creates a separate **JSON file** that records both the data and metadata (e.g., timestamp, author). For this use case:

- Data and metadata are stored in **JSON format** rather than **Parquet** to minimize processing time for real-time ingestion.
- The goal is to process data in a **hot path** immediately after it is received, without the need for full traceability or recoverability.
- Data is processed, insights are generated, and the data is removed, allowing the cycle to repeat.

The architecture is designed to ensure **low-latency** processing, which is critical for real-time data use cases.

---

## Architecture Design

For a visual representation of the system architecture, including the **Batch** and **Streaming** directories and how they work together, refer to the **Architecture Design Diagram**.