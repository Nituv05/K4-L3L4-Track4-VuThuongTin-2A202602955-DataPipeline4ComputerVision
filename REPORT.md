# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Vũ Thường Tín |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602955 |
| GitHub | Nituv05 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-03904 |

MSSV chứa chữ nên tôi giữ nguyên `2A202602955` cho `LAB_STUDENT_ID` và việc chia dataset; riêng công thức chọn trip dùng phần số `202602955 % 26 = 9`, tương ứng `trip-03904` trong danh sách đã sắp xếp của `cli list-trips` (chỉ số bắt đầu từ 0).

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Tôi thấy nguồn A dùng `ts_utc` dạng ISO UTC (ví dụ `2025-10-07T08:00:00.014Z`), còn B dùng `timestamp_ms` dạng epoch mili giây (`1759933801509`). A có `speed_mps` theo m/s, B có `spd_kmh` theo km/h. Tên cột hướng và GPS cũng khác: `heading_deg`, `lat`, `lon` ở A tương ứng `hdg`, `latitude`, `longitude` ở B. Bàn đạp A dùng `gas`, `brake` theo tỷ lệ, còn B dùng `throttle_pct`, `brake_pct` theo phần trăm; ví dụ gas A là `0.028`, throttle B là `2.0`. Theo manifest nguồn, đồng hồ A không lệch, còn B lệch +1500 ms so với video.

Giữ mỗi nguồn trong một bảng bronze giúp lưu đúng schema nhận được để kiểm tra và xử lý schema drift ở bước sau. Các giá trị CSV được giữ dạng chuỗi, kể cả ô trống, nên dữ liệu sai kiểu hoặc thiếu không bị mất khi ép kiểu sớm; silver mới chuẩn hoá và cách ly dòng lỗi. Quy tắc dạng chuỗi áp dụng cho cột dữ liệu CSV; cột lineage `_row_no` là số nguyên và `_ingested_at` là timestamp. Video gốc được lưu riêng trong object storage, còn bảng metadata giữ URI và SHA256 để nhận diện lô đã nạp.

Lần chạy của tôi có **3021 dòng A, 3234 dòng B, 26 trip và 2 video**, đạt **5/5 checks passed** ở module 1. Nạp lại cả A, B và trips đều báo `already present`; lịch sử mỗi bảng telemetry chỉ có một commit `version=0`, lần lượt thêm 3021 và 3234 dòng. Điều này cho thấy nhận diện theo batch SHA256 giúp chạy lại không tăng số dòng. Đầu ra thực tế được lưu tại [step1_bronze.txt](report/checkpoints/step1_bronze.txt) và [step1_bronze_details.txt](report/checkpoints/step1_bronze_details.txt).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

(trả lời ở đây)

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
