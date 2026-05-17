# Wikimedia Real-Time Analytics Pipeline

## Project Overview

This project is an end-to-end real-time big data pipeline for analyzing Wikimedia recent-change events.

The pipeline collects live Wikimedia-style events, sends them into Kafka, processes them using Spark Structured Streaming, stores the processed results in Hive, and displays the final insights in a custom dashboard.

The main goal of the project is to demonstrate a complete big data workflow:

1. Real-time data collection
2. Kafka streaming
3. Spark Structured Streaming processing
4. Hive persistent storage
5. Dashboard visualization

The dashboard shows useful insights such as latest events, top Wikimedia servers by change count, bot versus human activity, and one-minute window summaries.

---

## Architecture

The project architecture follows this flow:

```text
Wikimedia Recent Change Events
        ↓
Python Producer
        ↓
Apache Kafka Topic
        ↓
Spark Structured Streaming
        ↓
HDFS Static Reference Dataset Join
        ↓
Hive Tables
        ↓
Custom Dashboard
```
