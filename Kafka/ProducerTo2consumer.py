from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import threading
import time
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "partition-demo"
GROUP_ID = "groupB"

# ---------- Producer ----------
def run_producer():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"[Producer] Delivery failed: {err}")
        else:
            print(f"[Producer] Delivered to partition {msg.partition()} @ offset {msg.offset()}")

    i = 0
    while i < 30:  # 发 20 条消息
        key = f"user_{random.randint(1, 3)}"
        value = f"message_{i}"
        producer.produce(TOPIC, key=key.encode("utf-8"), value=value.encode("utf-8"), callback=delivery_report)
        producer.flush()
        time.sleep(0.5)
        i += 1


# ---------- Consumer ----------
def run_consumer(name):
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            print(f"[{name}] Got message: {msg.value().decode()} from partition {msg.partition()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    # 启动两个消费者线程
    t1 = threading.Thread(target=run_consumer, args=("Consumer-1",))
    t2 = threading.Thread(target=run_consumer, args=("Consumer-2",))
    t1.start()
    t2.start()

    # 启动生产者（主线程跑）
    run_producer()

    t1.join()
    t2.join()
