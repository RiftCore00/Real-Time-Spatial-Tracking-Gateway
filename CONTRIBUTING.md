# Contributing to Real-Time Spatial Tracking Gateway

Thank you for your interest in contributing! This document outlines the development workflow, project structure, and guidelines.

## Project Structure

```
├── src/
│   ├── index.js         # Entry point, signal handling
│   ├── server.js        # WebSocket server, message routing
│   ├── room-manager.js  # Room-based subscription management
│   ├── validator.js     # Zod-based message validation
│   ├── auth.js          # JWT connection authentication
│   └── logger.js        # Structured JSON logging
├── tests/
│   ├── room-manager.test.js
│   ├── validator.test.js
│   └── cleanup.test.js
├── .github/workflows/   # CI pipeline
└── README.md
```

## Development Setup

```bash
git clone https://github.com/RiftCore00/Real-Time-Spatial-Tracking-Gateway.git
cd Real-Time-Spatial-Tracking-Gateway
npm install
cp .env.example .env
npm test
```

## Coding Standards

### JavaScript Style

- Run `npm run lint` before committing
- Use ES module syntax (`import`/`export`)
- Follow the existing code style (2-space indent, trailing semicolons)

### Testing

Every new feature must include tests. Tests use **Vitest**. Key test areas:

- **Validation**: Malformed, missing, and out-of-range payloads
- **Room lifecycle**: Join, leave, broadcast membership boundaries
- **Cleanup**: Disconnect teardown, memory leak prevention

Run tests with:

```bash
npm test          # Run once
npm run test:watch  # Watch mode
npm run test:coverage  # With coverage report
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes with clear, descriptive commit messages.
3. Ensure tests pass (`npm test`) and lint is clean (`npm run lint`).
4. Open a pull request linking any related issues.
5. Ensure CI passes across all Node.js versions (18, 20, 22).

## Protocol

See [README.md](README.md#api--websocket-protocol) for the WebSocket message protocol.
