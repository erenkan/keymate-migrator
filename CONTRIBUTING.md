# Contributing to Keymate Migrator

First off, thank you for considering contributing to Keymate Migrator! It's people like you that make open source software such a great place to learn, inspire, and create.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally.
3.  Ensure you have **Java 21+** and **Docker** installed (for PostgreSQL integration tests).

## Development Workflow

1.  Create a new branch for your feature or bugfix:
    ```bash
    git checkout -b feature/my-awesome-feature
    ```
2.  Make your changes. Please ensure your code follows the existing style (standard Java/Quarkus conventions).
3.  Run tests to ensure no regressions:
    ```bash
    ./mvnw test
    ```

## Commit Guidelines

We require all commits to be signed off (DCO - Developer Certificate of Origin).
This certifies that you wrote the code or have the right to contribute it.

```bash
git commit -s -m "feat: add new retry strategy"
```

## Pull Request Process

1.  Push your branch to your fork on GitHub.
2.  Open a Pull Request against the `main` branch of this repository.
3.  Describe your changes clearly. If it fixes an issue, please link to it.
4.  Wait for code review. We may ask for changes to ensure consistency and quality.

## Code Style

- We use standard Java naming conventions.
- Prefer reactive patterns (Mutiny) over imperative blocking code where possible.
- Ensure concurrency limits are respected in any new logic.
