# BÀI TẬP 4: XỬ LÝ LỖI CHO KAFKA CONSUMER VỚI RETRY VÀ DEAD LETTER QUEUE (DLQ)

## PHẦN 1: PHÂN TÍCH VẤN ĐỀ

### 1. Cơ chế đọc Offset của Kafka
- **Offset** là một số nguyên tăng dần xác định vị trí duy nhất của từng tin nhắn (message) trong một Partition của Kafka Topic.
- Trong Kafka Consumer, có 2 khái niệm offset quan trọng:
  - **Current Offset**: Vị trí mà Consumer vừa đọc được từ partition.
  - **Committed Offset**: Vị trí mà Consumer thông báo cho Kafka Broker biết là đã xử lý hoàn tất.

### 2. Lý do Consumer bị kẹt khi gặp Exception
- Khi Consumer gặp tin nhắn bị lỗi định dạng (Poison Pill Message, ví dụ JSON bị thiếu dấu ngoặc `}`):
  1. Consumer đọc tin nhắn tại offset $N$.
  2. Trong quá trình deserialization hoặc xử lý nghiệp vụ, ứng dụng throw ra `Exception`.
  3. do gặp lỗi, dòng mã thực hiện `commit()` offset $N$ không bao giờ được gọi (hoặc bị hủy bỏ).
  4. Ở chu kỳ poll tiếp theo (hoặc sau khi restart), Consumer lại đọc từ **Committed Offset** gần nhất (vẫn là offset $N$).
  5. Kết quả: Consumer lặp đi lặp lại việc xử lý tin nhắn lỗi tại offset $N$ vô tận, dẫn đến hệ thống bị kẹt và không thể đọc tiếp các tin nhắn hợp lệ tại offset $N+1, N+2,...$

### 3. Giải pháp thiết kế
- **Thử lại (Retry)**: Khi xảy ra exception, thử lại tối đa **3 lần** (có khoảng chờ giữa các lần retry) để loại trừ các lỗi tạm thời (như gián đoạn kết nối mạng, DB timeout).
- **Dead Letter Queue (DLQ)**: Nếu sau 3 lần retry vẫn thất bại (do lỗi định dạng vĩnh viễn), đẩy tin nhắn lỗi sang một Topic riêng biệt gọi là `order-events-dlq` kèm thông tin lỗi để quản trị viên kiểm tra và xử lý sau.
- **Commit Offset**: Sau khi đã đẩy thành công tin nhắn lỗi vào DLQ, tiến hành **commit offset** $N$ để Consumer chuyển sang xử lý các tin nhắn tiếp theo bình thường.

---

## PHẦN 2: HƯỚNG DẪN CÀI ĐẶT VÀ CHẠY MÃ NGUỒN

### Cấu trúc thư mục
- `requirements.txt`: Khai báo thư viện phụ thuộc (`kafka-python`).
- `inventory_consumer.py`: Mã nguồn Kafka Consumer có xử lý Retry, DLQ và Commit offset.
- `producer_demo.py`: Mã nguồn Producer để gửi các tin nhắn thử nghiệm (tin hợp lệ và tin lỗi JSON).

### Các bước vận hành

1. **Cài đặt thư viện phụ thuộc**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Khởi chạy Kafka Consumer**:
   ```bash
   python inventory_consumer.py
   ```

3. **Mở terminal mới và chạy Producer để gửi tin nhắn kiểm thử**:
   ```bash
   python producer_demo.py
   ```

4. **Kết quả quan sát**:
   - Tin nhắn 1 (hợp lệ) -> Xử lý thành công.
   - Tin nhắn 2 (lỗi JSON) -> Thử lại 3 lần -> Đẩy vào `order-events-dlq` -> Commit offset.
   - Tin nhắn 3 (hợp lệ) -> Tiếp tục được xử lý thành công mà không bị kẹt.