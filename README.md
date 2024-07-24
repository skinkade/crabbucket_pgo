# crabbucket_pgo

[![Package Version](https://img.shields.io/hexpm/v/crabbucket_pgo)](https://hex.pm/packages/crabbucket_pgo)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/crabbucket_pgo/)

```sh
gleam add crabbucket_pgo@1
```

## Usage

[The Wisp API example](./examples/wisp_api_limit_example/src/wisp_api_limit_example.gleam)
showcases how to use this library with Wisp middleware to apply rate limiting to an API.

Further documentation can be found at <https://hexdocs.pm/crabbucket_pgo>.

## Development

Unit tests run against a Postgres instance, spun up via `podman-compose`

```sh
make test
```
