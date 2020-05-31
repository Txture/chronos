# Exodus ChronoDB Backend

This is the primary storage backend for [ChronoDB](../org.chronos.chronodb.api/readme.md). It offers file-based persistence and is implemented on top of [Jetbrains Exodus](https://github.com/JetBrains/xodus), a key-value store.

## Core Features

- Primary storage backend of choice for ChronoDB
- ACID file-based persistence
- Dynamic loading & unloading: Work with datasets which are much larger than your RAM

## Configuration

Exodus offers a wide range of configuration options. In order to keep things consistent, we defined **prefixed configuration keys**. Those will be picked up by ChronoDB in the configuration phase, and then be forwarded to the original Exodus configuration keys. For more datails, please see the `ExodusChronoDBConfiguration` in the source code; it lists all available configuration options.
