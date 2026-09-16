import json
import logging
import time
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("InventoryConsumer")

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_INVENTORY = 'order-events'
TOPIC_DLQ = 'order-events-dlq'
CONSUMER_GROUP = 'inventory-group'
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1

def create_consumer():
    return KafkaConsumer(
        TOPIC_INVENTORY,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False, # Tắt auto-commit để kiểm soát commit offset thủ công
        value_deserializer=lambda m: m # Nhận raw bytes để xử lý lỗi JSON linh hoạt
    )

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v
    )

def process_order_event(raw_bytes):
    try:
        decoded_str = raw_bytes.decode('utf-8')
        data = json.loads(decoded_str)
    except Exception as e:
        raise ValueError(f"Lỗi định dạng JSON payload: {e}")

    if not isinstance(data, dict) or 'productId' not in data or 'quantity' not in data:
        raise ValueError(f"Thiếu trường dữ liệu bắt buộc (productId, quantity): {data}")

    product_id = data['productId']
    quantity = data['quantity']

    # Giả lập trừ kho hàng thành công
    logger.info(f"[SUCCESS] Đã trừ kho thành công: Sản phẩm={product_id}, Số lượng={quantity}")

def send_to_dlq(producer, record, error_message):
    dlq_payload = {
        "original_topic": record.topic,
        "original_partition": record.partition,
        "original_offset": record.offset,
        "raw_payload": record.value.decode('utf-8', errors='replace'),
        "error_message": str(error_message),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.send(TOPIC_DLQ, value=dlq_payload)
    producer.flush()
    logger.warning(f"[DLQ] Đã chuyển tin nhắn lỗi tại Offset {record.offset} sang topic '{TOPIC_DLQ}'")

def main():
    consumer = create_consumer()
    producer = create_producer()
    logger.info(f"Inventory Consumer đã khởi động. Đang lắng nghe trên topic '{TOPIC_INVENTORY}'...")

    try:
        for record in consumer:
            success = False
            last_exception = None

            # Thử lại (Retry) tối đa MAX_RETRIES lần
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    logger.info(f"Đang xử lý message tại Offset {record.offset} (Lần thử {attempt}/{MAX_RETRIES})...")
                    process_order_event(record.value)
                    success = True
                    break
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Lần thử {attempt}/{MAX_RETRIES} thất bại cho Offset {record.offset}: {e}")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY_SECONDS)

            # Nếu thử lại thất bại -> Chuyển vào Dead Letter Queue (DLQ)
            if not success:
                logger.error(f"[FAILED] Xử lý thất bại sau {MAX_RETRIES} lần thử cho Offset {record.offset}.")
                send_to_dlq(producer, record, last_exception)

            # Đảm bảo luôn commit offset để consumer không bị kẹt
            consumer.commit()
            logger.info(f"Đã commit Offset {record.offset} thành công.")

    except KeyboardInterrupt:
        logger.info("Đã dừng Consumer.")
    finally:
        consumer.close()
        producer.close()

if __name__ == '__main__':
    main()
