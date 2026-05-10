# kiro-rs-cache-simulator

纯内存 prompt-cache 模拟代理，用于给已经运行的 `kiro-rs` 套一层缓存命中字段填充逻辑。

## 行为

- 监听 `0.0.0.0:8990`（可配置）。
- 通过 `sourceUrl` 转发所有请求到真实 `kiro-rs`。
- 仅对 `POST /v1/messages` 与 `POST /cc/v1/messages` 计算 PR #57 风格的缓存断点。
- 不依赖 Redis；缓存保存在进程内存中，重启即清空。
- 默认日志只输出错误，以及每次透传完成后的缓存计算摘要。
- 在 JSON/SSE 响应的 `usage` 中补充：
  - `cache_creation_input_tokens`
  - `cache_read_input_tokens`

## 配置

创建 `cache-simulator.json`：

```json
{
  "sourceUrl": "http://127.0.0.1:8080",
  "host": "0.0.0.0",
  "port": 8990
}
```

也可以用命令行覆盖：

```bash
cargo run --manifest-path kiro-rs-cache-simulator/Cargo.toml -- --source-url http://127.0.0.1:8080
```

## 构建

```bash
cargo build --manifest-path kiro-rs-cache-simulator/Cargo.toml --release
```

## Docker

```bash
docker run --rm -p 8990:8990 ghcr.io/ai-trash/kiro-rs-cache-simulator:latest \
  --source-url http://host.docker.internal:8080
```

Docker Compose 示例：

```yaml
services:
  kiro-rs-cache-simulator:
    image: ghcr.io/ai-trash/kiro-rs-cache-simulator:latest
    ports:
      - "8990:8990"
    command:
      - --source-url
      - http://host.docker.internal:8080
```

如果 `kiro-rs` 也在同一个 Compose 网络里运行，把 `sourceUrl` 改成对应服务名即可，例如 `http://kiro-rs:8080`。

## 使用

1. 先启动真实 `kiro-rs`，例如暴露在 `http://127.0.0.1:8080`。
2. 启动模拟器并配置 `sourceUrl` 指向真实服务。
3. 客户端改为请求 `http://127.0.0.1:8990/v1/messages` 或 `/cc/v1/messages`。

缓存键按 `cache:{apiKey}:{hash}` 隔离，`apiKey` 来自 `x-api-key` 或 `Authorization: Bearer ...`。

## License

AGPL-3.0-or-later. See `LICENSE`.
