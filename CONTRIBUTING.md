# Contributing to Platformatic Kafka

Welcome to the Platformatic Kafka development guide! This document will help you set up your development environment and contribute to the project.

## Project Overview

Platformatic is an open-source, modern, high-performance, pure TypeScript/JavaScript type safe client for Apache Kafka.

## Prerequisites

- **Node.js**: Version 20.19.4, 22.18.0 or 24.6.0 or higher
- **pnpm**: Package manager (follow the [pnpm installation guide](https://pnpm.io/installation))
- **Docker** (optional): Only required for running tests locally.

## Getting Started

### 1. Clone and Setup

```bash
git clone https://github.com/platformatic/kafka.git
cd kafka
```

### 2. Install Dependencies

```bash
pnpm install
```

### 4. Setup Docker Environment (Optional)

This step is only required if you plan to run tests locally.

```bash
docker compose up -d
```

## Development Workflow

### Essential Commands

```bash
# Development workflow
pnpm test              # Run all tests
pnpm run build         # Build the package
pnpm run lint          # Lint the package
```

### Testing

Tests require Docker for Kafka cluster setup.

```
docker compose up -d
```

You can eventually provide a `KAFKA_VERSION` environment variable to select a specific version of Kafka.
Note that the images use Confluent Kafka so you need to add 4 to your desider version. For instance, to run on Kafka 3.9.0:

```
bash
KAFKA_VERSION=7.9.0 docker compose up -d
```

Then you run the test normally:

```bash
pnpm test
```

## Pull Request Process

### Before Submitting

1. Ensure all tests pass: `pnpm test`
2. Run linting: `pnpm run lint`
3. Build all packages: `pnpm run build`

### Creating a Pull Request

1. If your PR fixes a GitHub issue, add this at the top of the PR description:

```
Fixes #<issue-number>.
```

2. Ensure all checks pass and get approval
3. Merge using "squash and merge" option

## Developer Certificate of Origin

All contributions must include a Developer Certificate of Origin (DCO) sign-off. Use the `-s` flag when committing:

```bash
git commit -s -m "Your commit message"
```

This automatically adds your `Signed-off-by` line to the commit message. If working with AI assistance, ensure both contributors are signed off.

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

If you want to know how to retroactively add the DCO to your commits,
check out [this guide](https://github.com/src-d/guide/blob/master/developer-community/fix-DCO.md).

## Getting Help

- Check the [documentation](https://docs.platformatic.dev/)
- Join our [Discord community](https://discord.gg/platformatic) for real-time help and discussions
- Open an issue on GitHub

Thank you for contributing to Platformatic Kafka! 🚀
