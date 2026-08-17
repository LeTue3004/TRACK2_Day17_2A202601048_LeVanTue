# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Văn Tuệ  
**Lớp:** AICB-P2T2  
**Ngày:** 17/08/2026

## 0 · Kết quả `make verify`

```
run 1/3: 42.4s
run 2/3: 42.9s
run 3/3: 43.0s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG
gold_training_set     ✓ ok              12,480      12,480
gold_feature_daily    ✓ ok               9,100       9,100
gold_doc_chunks       ✓ ok              31,200      31,200
quarantine_tickets    ✓ ok                 312         312

CHECKSUM từng lượt
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
  số file parquet                           ✗ 5,000 → 5,000
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT: 4/4 tiêu chí đạt
```

`make verify` cũng in trạng thái không đạt cho dashboard/Parquet của bài mở
rộng A (5.000.000 rows scanned, 5.000 file); bài này không thuộc phạm vi ba
nhiệm vụ bắt buộc và chưa thực hiện tối ưu.

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng số hàng mỗi khi retry/Clear Task dù nguồn Silver chỉ có một trạng thái cuối cho mỗi ticket. |
| **Nguyên nhân** | Model incremental không có `unique_key` và `incremental_strategy`, nên dbt append bằng `INSERT`. Một retry ghi thêm cùng ticket thay vì thay thế. CDC có `op='u'`, vì vậy một ticket còn có thể xuất hiện ở nhiều partition `_ingested_at`; xoá-ghi lại theo ngày cũng không đủ. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, đặt `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1` để giảm rủi ro Clear Task/backfill đồng thời. |
| **Bằng chứng** | Sau sửa: 12.480 hàng, không ticket trùng; checksum `8dd7c98653` giống ở cả 3 lượt. |

`catchup` và `max_active_runs` chỉ giảm khả năng kích hoạt retry/backfill, không
phải root cause; tính idempotent phải được đảm bảo tại phép ghi của model.

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; phần thiếu tập trung ở ngày cũ. |
| **P99 độ trễ đo được** | **2.725833 ngày** (P50 0.128090; P95 1.813693; max 2.944688 ngày; 5.0509% event đến muộn hơn 1 ngày). |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 2.725833 ngày, đủ bao phủ ít nhất 99% event trễ. |
| **Nguyên nhân** | Điều kiện incremental chỉ lấy `event_date > max(event_date)` trong target. Event xảy ra ngày cũ nhưng tới Bronze sau khi target đã đi qua ngày đó sẽ không còn thoả điều kiện ở bất kỳ lượt sau nào. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, tính lại từ `max(event_date) - interval 3 day`, và dùng `merge` với khoá ghép `['event_date', 'customer_id']` để kết quả tính lại thay thế kết quả cũ, không cộng dồn. |
| **Bằng chứng** | Sau sửa: 9.100 hàng; checksum `3db448685c` giống cả 3 lượt. |

P99 thay vì max giới hạn chi phí tái tính lại hằng ngày: dùng max sẽ rộng hơn
để bao phủ cả ngoại lệ hiếm, nhưng luôn quét/tổng hợp thêm dữ liệu. Với dữ liệu
trễ hơn P99, cần một quy trình backfill/đối soát riêng.

## 3 · Kiểu dữ liệu `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `try_cast` làm nhãn chuỗi hợp lệ thành `NULL`, đồng thời để các số ngoài miền như `0`, `5`, `-1` đi qua; model phân loại nhận priority sai hoặc thiếu. |
| **Nguyên nhân** | Pipeline chỉ ép kiểu, không thực hiện semantic validation. Nó nhầm schema evolution (`urgent/high/medium/low`) với dữ liệu hỏng và không kiểm tra domain contract 1..4. |
| **Ba nhóm giá trị và xử lý** | `1..4`: giữ nguyên. `urgent/high/medium/low`: map lần lượt về `1/2/3/4`. `P1`, `unknown`, số ngoài miền, chuỗi rỗng/NULL: trả `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | `normalize_priority` dùng `CASE` để map/validate; `silver_tickets` lọc bản ghi lỗi **trước** khi xếp hạng CDC; `quarantine_tickets` dùng chính macro để lấy đúng các bản ghi lỗi. Bật contract và thêm `not_null`, `accepted_values [1,2,3,4]` trong `schema.yml`. |
| **Bằng chứng** | `quarantine_tickets = 312`; priority Silver sạch, thuộc 1..4; Silver vẫn giữ 12.480 ticket; `dbt test` 11/11 pass. |

Nên giữ Bronze thô để còn điều tra, replay và đối chiếu nguồn; việc chuẩn hoá,
quarantine thuộc Silver. Không nên dừng toàn bộ DAG vì vài trăm row lỗi sẽ
chặn hơn 130.000 event và 31.200 chunk hợp lệ. Quarantine tạo hàng đợi có thể
hành động cho người trực, trong khi pipeline vẫn phục vụ dữ liệu hợp lệ.

## 4 · Bài mở rộng

Không làm. Dataset được sinh chỉ để verifier có thể chạy hết, chưa áp dụng
partitioning/compaction dashboard hoặc sửa delivery semantics consumer.

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận hệ thống chưa quen, cần kiểm tra trước |
|---|---|
| 1 | Grain, natural key và SQL materialization thực tế của incremental model. |
| 2 | Phân bố event-time so với ingest-time trước khi chọn watermark/lookback. |
| 3 | Phân biệt thay đổi biểu diễn hợp lệ với vi phạm semantic contract; bảo toàn raw data và tách lỗi. |
