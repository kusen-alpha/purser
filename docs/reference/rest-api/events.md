# 事件

事件总线把内核状态变化发布为可回放的事件流，支持 SSE 实时订阅与按游标历史回放。

## SSE 实时流

```bash
curl -N "http://127.0.0.1:8000/pools/proxies/events?after_seq=0" \
  -H "Authorization: Bearer $PURSER_READONLY_KEY"
```

- `after_seq` 只推送序号大于该值的事件；断线后带上次 `seq` 重连即可续传。
- 服务端每 0.5 秒检查一次历史环形缓冲，其他池的事件只推进游标、不发送。
- `text/event-stream`，每条消息为 `data: {json}\n\n`。

## 历史回放

```bash
curl "http://127.0.0.1:8000/pools/proxies/events/history?after_seq=0&limit=200&type=lease_acquired,lease_released&rid=r1"
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `after_seq` | `0` | 只返回更新的记录 |
| `type` | 全部 | 逗号分隔的事件类型 |
| `rid` | 全部 | 限定资源 |
| `limit` | `200` | `1..1000` |

```json
{
  "events": [
    {
      "seq": 42,
      "type": "lease_acquired",
      "pool": "proxies",
      "rid": "r2",
      "data": {"token": "l_...", "consumer": "crawler-a", "slots": 1},
      "ts": 1731000000.0,
      "trace_id": "trace-1"
    }
  ],
  "cursor": 42
}
```

SSE 与历史回放都走同一套鉴权：配置密钥后，两者都需要 `Authorization: Bearer <key>`。若浏览器 `EventSource` 无法设置请求头，可在受信环境中使用只读密钥代理转发，或仅在完全开放的部署中直接连接；本服务不通过查询参数传递密钥。

## 常见事件类型

| 类别 | 事件 |
| --- | --- |
| 租约 | `lease_acquired`、`lease_released`、`lease_lost`、`lease_expired`、`lease_revoked`、`lease_reconciled` |
| 导入 | `import_finished` |
| 状态 | `state_changed`、`quarantined`、`quarantine_expired`、`banned`、`resource_restored` |
| 池 | `pool_created`、`pool_deleted` |
| 验活任务 | `validate_job_started`、`validate_job_finished` |

事件总线是有界环形缓冲（默认 10 000 条）：慢订阅者会被丢弃并告警，历史回放保留最近窗口。
