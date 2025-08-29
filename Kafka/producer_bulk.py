import argparse, json, time, random, string, sys
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

def rand_str(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def build_record(schema: str, payload_size: int):
    """
    schema: 'json' or 'csv'（ClickHouse Kafka Engine 推荐 JSONEachRow）
    payload_size: 附加随机负载大小（字节级别近似）
    """
    base = {
        "event_time": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
        "user_id": random.randint(1, 10_000_000),
        "event_type": random.choice(["click", "view", "buy", "cart"]),
        "page": random.choice(["home", "search", "detail", "cart", "checkout"]),
        "country": random.choice(["SG", "CN", "US", "IN", "DE"]),
        "amount": round(random.uniform(0, 9999), 2),
        "payload": rand_str(payload_size) if payload_size > 0 else ""
    }
    if schema == "json":
        return json.dumps(base).encode("utf-8")
    elif schema == "csv":
        # 与 ClickHouse CSV 解析列序一致即可
        row = [
            base["event_time"], str(base["user_id"]), base["event_type"],
            base["page"], base["country"], str(base["amount"]), base["payload"]
        ]
        return (",".join(row) + "\n").encode("utf-8")
    else:
        raise ValueError("unsupported schema")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="ch_events")
    ap.add_argument("--records", type=int, default=100000, help="发送总条数")
    ap.add_argument("--rps", type=int, default=0, help="限速，0 表示不限速")
    ap.add_argument("--batch", type=int, default=500, help="应用侧flush批大小（客户端缓冲自动生效）")
    ap.add_argument("--payload-size", type=int, default=64, help="每条额外负载大小")
    ap.add_argument("--schema", choices=["json","csv"], default="json")
    ap.add_argument("--compression", choices=["gzip","snappy","lz4","zstd","none"], default="zstd")
    ap.add_argument("--linger-ms", type=int, default=20, help="聚合延迟（毫秒），提升批量")
    ap.add_argument("--acks", default="1", choices=["0","1","all"])
    ap.add_argument("--idempotent", action="store_true", help="开启幂等生产（与 acks=all 配合）")
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        "compression.type": "none" if args.compression=="none" else args.compression,
        "linger.ms": args.linger_ms,
        "batch.num.messages": 100000,      # 尽量大批
        "queue.buffering.max.kbytes": 1024*1024,  # 1GB 缓冲
        "acks": args.acks
    }
    if args.idempotent:
        conf.update({
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2147483647,
            "request.timeout.ms": 60000,
            "delivery.timeout.ms": 120000
        })

    p = Producer(conf)

    sent = 0
    t0 = time.time()
    next_tick = t0
    rps = args.rps
    batch = args.batch

    def dr(err, msg):
        if err:
            # 打印错误即可；生产场景可做重试/告警
            print(f"DELIVERY_ERROR: {err}", file=sys.stderr)

    while sent < args.records:
        value = build_record(args.schema, args.payload_size)
        # 可选 key：相同 key 会进同一分区，保持有序
        key = None
        p.produce(args.topic, value=value, key=key, callback=dr)
        sent += 1

        # 应用层flush节流：减少 flush 次数（内部仍会聚合）
        if sent % batch == 0:
            p.poll(0)   # 触发回调
        # RPS 限速
        if rps > 0:
            # 简单的时间片控制
            if time.time() >= next_tick + 1:
                next_tick = time.time()
            # 控制每秒发送数量
            if (sent % rps) == 0:
                # 等到下一秒
                sleep_left = next_tick + 1 - time.time()
                if sleep_left > 0:
                    time.sleep(sleep_left)
                next_tick = time.time()

    # 清空缓冲区
    p.flush()
    t1 = time.time()
    dur = t1 - t0
    print(f"Done. Sent={sent}, time={dur:.2f}s, avg={sent/max(dur,1e-6):.1f} rec/s")

if __name__ == "__main__":
    main()
