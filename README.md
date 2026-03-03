# petas

A high-performance embedded key-value storage engine written in Go.

## Features

- **Fast**: Hot read ~450ns, cold read ~2μs
- **Efficient**: 1.15x memory overhead, supports values up to 50MB
- **Semantic Types**: String, Hash, ZSet with Redis-like API
- **Compression**: Auto compression (s2/zstd) for optimal performance
- **TTL Support**: Key-level expiration
- **Batch Operations**: Atomic writes
- **Production Ready**: 69% test coverage, race-detector clean

## Installation

```bash
go get github.com/naipad/petas
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/naipad/petas"
)

func main() {
    db, err := petas.Open("./data", nil)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    db.SetString("", "key", []byte("value"), 0)
    val, ok, _ := db.GetString("", "key")
    fmt.Printf("Value: %s, Found: %v\n", val, ok)
}
```

## License

MIT
