# kiro-rs-cache-simulator

纯内存 prompt-cache 模拟代理，用于给已经运行的 `kiro-rs` 补充缓存命中字段。

## 配置

只支持命令行参数和环境变量。

```bash
UPSTREAM=http://127.0.0.1:8080 cargo run --manifest-path kiro-rs-cache-simulator/Cargo.toml
cargo run --manifest-path kiro-rs-cache-simulator/Cargo.toml -- --upstream http://127.0.0.1:8080
```

| 命令行 | 环境变量 | 默认值 |
| --- | --- | --- |
| `--upstream` | `UPSTREAM` | 必填 |
| `--host` | `HOST` | `0.0.0.0` |
| `--port` | `PORT` | `8990` |

## 构建

```bash
cargo build --manifest-path kiro-rs-cache-simulator/Cargo.toml --release
```

## Docker

```bash
docker run --rm -p 8990:8990 \
  -e UPSTREAM=http://host.docker.internal:8080 \
  ghcr.io/ai-trash/kiro-rs-cache-simulator:latest
```

## License

AGPL-3.0-or-later. See `LICENSE`.
