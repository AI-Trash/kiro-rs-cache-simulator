# claude-cache-simulator

纯内存 Claude prompt-cache 模拟代理，可在转发请求时模拟 cache 计数字段，并按需剔除 Claude Code 请求中的动态 `cch`。

请求会代理到 `UPSTREAM`；默认会先从 Claude messages JSON 的 `system` 文本里移除 `cch=...` 段，再转发和计算 cache hash。关闭 `STRIP_CCH` 后请求体不会被改写。

## 配置

| 命令行 | 环境变量 | 默认值 |
| --- | --- | --- |
| `--upstream` | `UPSTREAM` | 必填 |
| `--host` | `HOST` | `0.0.0.0` |
| `--port` | `PORT` | `8990` |
| `--simulate-cache` / `--no-simulate-cache` | `SIMULATE_CACHE` | `true` |
| `--strip-cch` / `--no-strip-cch` | `STRIP_CCH` | `true` |

布尔环境变量支持 `1/true/yes/on` 和 `0/false/no/off`。

- `SIMULATE_CACHE=true`：根据 Claude `cache_control` 断点模拟并注入 `cache_creation_input_tokens`、`cache_read_input_tokens` 和调整后的 `input_tokens`。
- `STRIP_CCH=true`：从 Claude Code 注入的 `x-anthropic-billing-header` 文本中剔除动态 `cch=...` 段，避免每次请求生成不同 cache hash。

## 构建

```bash
cargo build --release
```

## Docker

```bash
docker run --rm -p 8990:8990 \
  -e UPSTREAM=http://host.docker.internal:8080 \
  ghcr.io/ai-trash/claude-cache-simulator:latest
```

```yaml
services:
  claude-cache-simulator:
    image: ghcr.io/ai-trash/claude-cache-simulator:latest
    ports:
      - "8990:8990"
    environment:
      UPSTREAM: http://host.docker.internal:8080
      SIMULATE_CACHE: "true"
      STRIP_CCH: "true"
```

## License

AGPL-3.0-or-later. See `LICENSE`.
