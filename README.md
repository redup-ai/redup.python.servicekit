# redup-servicekit

![Python Test status](https://github.com/redup-ai/redup.python.servicekit/actions/workflows/python-test.yml/badge.svg?branch=master)


Python toolkit for building production-ready microservices with:

- centralized YAML configuration and environment overrides,
- structured JSON logging,
- Prometheus metrics and monitoring helpers,
- gRPC health checks and utilities (async client, server-side decorators).

## Features

- **Configuration**: `ConfigSingleton` for loading and merging YAML configs, with environment variable overrides.
- **Logging**: JSON console logging via `python-json-logger`.
- **Monitoring & Metrics**: Prometheus integration with counters, gauges and histograms for tasks and service stats.
- **gRPC Utilities**: Async gRPC client, health checks and decorators for collecting per-method metrics.

## Installation

```bash
pip install redup-servicekit
```

## License

Licensed under the MIT License. See the `LICENSE` file for details.
