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

Nguồn A ép `ts_utc` thành timestamp có múi giờ và giữ tốc độ m/s; nguồn B đổi tên cột, chia `spd_kmh` cho 3.6 và chia phần trăm bàn đạp cho 100. Timestamp B được tính bằng `to_timestamp((timestamp_ms - offset_ms) / 1000.0)`, với `offset_ms=1500` lấy từ metadata bronze; vì vậy hai nguồn cùng dùng UTC và cùng đồng hồ với video. Ô tốc độ trống được đổi thành NULL bằng `nullif` và `try_cast` trước khi phân loại lỗi.

Trong lần chạy của tôi, B bắt đầu ở **2025-10-08 14:30:00.009000+00:00**, chỉ muộn 9 ms so với video bắt đầu lúc 14:30:00, thay vì lệch khoảng 1.5 giây. Silver có **2968 dòng A và 3181 dòng B**; tốc độ tối đa sau đổi sang km/h và làm tròn lần lượt là **71 và 100**. Module 2 đạt **7/7 checks passed**, gồm kiểm tra không trùng `(drive_id, ts)`, tốc độ trong 0–70 m/s và hiệu chỉnh đồng hồ B.

| Drive | `duplicate` | `speed_missing` | `speed_out_of_range` | Tổng quarantine |
|---|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Đối chiếu bảo toàn số dòng: **2968 + 53 = 3021** ở A và **3181 + 53 = 3234** ở B; tổng cộng 6149 dòng sạch và 106 dòng quarantine. Bảng `telemetry_rejects` giữ lý do lỗi cùng `_batch_id`, `_row_no`, thời gian và tốc độ để truy vết, xem lại hoặc xử lý sau; xoá dòng lỗi sẽ mất bằng chứng và che khuất chất lượng nguồn. Số liệu được lưu trong [step2_silver_telemetry.txt](report/checkpoints/step2_silver_telemetry.txt).

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` trả về khoảng trống giữa các mẫu liên tiếp lớn hơn 1000 ms và được cấu hình `severity: warn`, vì mất tín hiệu cảm biến là thiếu hụt cần báo để theo dõi; bước ghép khung hình sau đó còn kiểm tra độ lệch thời gian. Truy vấn của tôi thấy mỗi drive có một khoảng vượt ngưỡng: lớn nhất **3073 ms ở A** và **3124 ms ở B**. `in_range` kiểm tra tốc độ 0–70 m/s với mức mặc định `error`, vì tốc độ vô lý không được đi vào dữ liệu huấn luyện; task dbt test thất bại sẽ chặn các task downstream trong DAG.

Theo [break_gate.json](report/evidence/break_gate.json), khi nới luật làm sạch lên **1000 m/s**, test **`in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng**, với kết quả **PASS=5 WARN=1 ERROR=1 SKIP=0**. Ngưỡng kiểm thử vẫn là 70 m/s nên bắt được dữ liệu sai lọt qua bước làm sạch: cổng chất lượng là một kiểm tra riêng, không chỉ lặp lại kết luận của phép biến đổi. Sau khôi phục ngưỡng làm sạch 70 m/s, kết quả trở lại **PASS=6 WARN=1 ERROR=0 SKIP=0**, và kiểm tra module 2 vẫn đạt **7/7**. Đầu ra thí nghiệm và kiểm tra sau khôi phục nằm trong [step2_break_gate.txt](report/checkpoints/step2_break_gate.txt) và [step2_restored_checks.txt](report/checkpoints/step2_restored_checks.txt).

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Tôi chạy thí nghiệm với **`trip-03904`**: [idempotency.json](report/evidence/idempotency.json) ghi **1246 dòng trước và 1246 dòng sau hai lần chạy lại**, cùng dấu vân tay **`cb6118dbb0b8`**, `unchanged=true`. Trip này có **22 khung hình**; ghi bằng `append` hai lần vào bảng thử tạo **44 dòng**, tức nhân đôi. Dấu vân tay kiểm tra ID khung hình, trip, độ sáng, độ nét và dHash; nó không tính `_processed_at`, vốn thay đổi theo lần xử lý.

Mã thực tế dùng `mode="overwrite"` với predicate `trip_id = 'trip-03904'` để thay thế đúng tập dòng của trip; bảng Delta được phân vùng vật lý theo **`drive_id`**, còn trip là đơn vị chạy lại. Delta ghi file mới rồi commit một phiên bản bảng trong giao dịch: trước commit, người đọc vẫn thấy snapshot đã commit; lỗi trước commit không làm mất tập dòng cũ. Nếu xoá rồi ghi bằng hai thao tác riêng, lỗi sau xoá có thể để bảng thiếu dữ liệu. Cơ chế snapshot và commit được mô tả trong [tài liệu Delta Lake](https://docs.delta.io/concurrency-control/). Đây là bảo đảm cho bảng Delta; ảnh JPG được ghi vào các URI cố định trước đó và không cùng một giao dịch với bảng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 | 8 |
| 2 | 149 | 52 |
| 6 | 363 | 244 |

Pipeline coi khung **đen** là độ sáng trung bình <12, còn khung **tối** là từ 12 đến dưới 40; đen bị loại khỏi gold, tối chỉ được gắn cờ và vẫn có thể giữ nếu đạt các luật khác. Lần chạy của tôi có **6 khung đen ở A, 4 ở B**, cùng **4 khung tối ở A và 61 ở B**. Áp dụng các điều kiện giữ của gold thì **4/4 khung tối A và 58/61 khung tối B** đủ điều kiện; cờ tối không bảo đảm mọi khung đều được giữ. Ảnh mẫu `drive_b-00030` dưới cầu vẫn thấy đường và cảnh phía trước dù phần lớn ảnh tối. Tôi đồng ý giữ những cảnh này để dataset phản ánh cả bóng râm và thiếu sáng trong thực tế.

Theo [threshold.json](report/evidence/threshold.json), ngưỡng 0, 2 và 6 cho số khung trùng ở bảng trên; mỗi khung được so với khung trước trong cùng trip, không so giữa hai trip. Tôi chọn **2 bit** làm mức cân bằng: ngưỡng 0 dễ giữ lại khung gần giống do nhiễu nhỏ, còn ngưỡng 6 coi **607/1246 khung** là trùng, có nguy cơ mất biến thiên hữu ích. Ở tốc độ lấy mẫu 2 fps, xe chạy chậm làm cảnh giữa hai khung thay đổi ít nên dễ bị đánh dấu trùng; xe chạy nhanh thường làm cảnh thay đổi nhiều hơn. Vì vậy khi đổi tốc độ xe hoặc fps, cần xem lại ngưỡng và ảnh thực tế; thí nghiệm này chưa chứng minh một ngưỡng tối ưu cho mọi chuyến.

Bước 3 xử lý **26 trip, 1246 khung hình** (602 A, 644 B), đạt **6/6 kiểm tra** và **9/9 unit test**. A có 9 khung mờ, B có 0; 8/8 khung trong đoạn cố ý làm mờ và 11/12 khung trong đoạn camera đứng hình được phát hiện. ASOF join chọn telemetry gần nhất tại hoặc trước event time của khung hình: độ trễ trung bình làm tròn là **66 ms ở A, 73 ms ở B**, lớn nhất **2884 và 3112 ms**; mỗi drive có 7 khung không đạt dung sai 150 ms. Số cờ có thể chồng lấp, nên không cộng chúng để tính tổng khung bị loại. Bằng chứng bổ sung ở [step3_silver_frames.txt](report/checkpoints/step3_silver_frames.txt), [step3_unit_tests.txt](report/checkpoints/step3_unit_tests.txt) và [step3_quality_details.txt](report/checkpoints/step3_quality_details.txt).

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
