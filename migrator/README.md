# Keymate Migrator Module

This module contains the core logic for the high-throughput, restartable migration tool.

> **Note:** For the complete documentation, including architecture overview, configuration, and simulation guide, please refer to the [Root README](../README.md).

## Module Structure

- `src/main/java`: Core application logic (Quarkus + Mutiny).
- `src/main/resources/db/migration`: SQL scripts for schema initialization and synthetic data generation.
- `src/main/resources/application.properties`: Configuration file for database connections, concurrency limits, and retry policies.

## Quick Start

To run this module individually (assuming database and target systems are ready):

```bash
./mvnw quarkus:run
```
