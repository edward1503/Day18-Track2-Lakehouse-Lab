# Reflection — Day 18 Lab

## Anti-pattern team dễ vướng nhất: "Small-File Problem" (slide §5)

Trong thực tế, team chúng tôi dễ vướng nhất vào **Small-File Problem** — anti-pattern phổ biến nhất khi xây dựng Lakehouse.

Lý do: pipeline ingestion của team thường thiết kế theo kiểu micro-batch hoặc streaming, mỗi batch ghi một file nhỏ riêng biệt vào Delta Lake. Sau vài ngày vận hành, số lượng file tăng lên hàng nghìn, khiến:

1. **Query chậm** — engine phải mở và scan quá nhiều file nhỏ, overhead metadata vượt quá thời gian đọc data thực sự.
2. **Object store cost tăng** — MinIO/S3 tính phí theo số lượng GET requests, không phải kích thước file.
3. **Transaction log phình to** — mỗi file nhỏ tạo thêm entry trong `_delta_log`, khiến planning phase của query optimizer mất nhiều thời gian hơn.

NB2 đã chứng minh điều này: 200 small files tạo query time 5.39s, nhưng sau OPTIMIZE + Z-ORDER chỉ còn 0.36s (14.8× improvement). Bài học: cần lên lịch `OPTIMIZE` định kỳ (ví dụ: nightly job) và sử dụng Auto Compaction nếu platform hỗ trợ.
