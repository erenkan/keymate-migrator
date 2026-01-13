# Mock REST Server Module

This module provides a simulation target for the Keymate Migrator. It mimics the behavior of a real ingestion API (like Keycloak) by introducing artificial delays and random failures.

> **Note:** For the complete documentation and how to use this module in a full simulation, please refer to the [Root README](../README.md).

## Features

- **Configurable Delays:** Simulates network or processing latency.
- **Random Failures:** Injects HTTP 500 and 400 errors based on a configurable probability.
- **Metrics:** Provides insight into the simulated load.

## Quick Start

To run this module on port 8081:

```bash
./mvnw quarkus:run -Dquarkus.http.port=8081
```
