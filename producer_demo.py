import json
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_INVENTORY = 'order-events'

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode('utf-8')
    )

    print("--- Gửi tin nhắn thử nghiệm ---")

    # 1. Tin nhắn hợp lệ 1
    msg1 = {"productId": "PROD-101", "quantity": 2}
    producer.send(TOPIC_INVENTORY, msg1)
    print(f"Sent: {msg1}")
    time.sleep(1)

    # 2. Tin nhắn LỖI định dạng JSON (Thiếu dấu ngoặc nhọn đóng '}')
    invalid_json = b'{"productId": "PROD-102", "quantity": 5'
    producer.send(TOPIC_INVENTORY, invalid_json)
    print(f"Sent (Corrupted JSON): {invalid_json.decode('utf-8')}")
    time.sleep(1)

    # 3. Tin nhắn hợp lệ 2 (Đảm bảo consumer không bị kẹt và vẫn đọc tiếp được)
    msg3 = {"productId": "PROD-103", "quantity": 1}
    producer.send(TOPIC_INVENTORY, msg3)
    print(f"Sent: {msg3}")

    producer.flush()
    producer.close()
    print("--- Đã gửi xong tất cả tin nhắn ---")

if __name__ == '__main__':
    run_producer()
