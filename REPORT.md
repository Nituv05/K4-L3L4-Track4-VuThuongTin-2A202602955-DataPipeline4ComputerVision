# REPORT — Lab Day 9

> Báo cáo dùng số liệu từ các lần chạy pipeline và thí nghiệm trên máy của tôi, với `LAB_STUDENT_ID=2A202602955`. Các đầu ra và ảnh giao diện thật được lưu trong `report/` để đối chiếu.

| | |
|---|---|
| Họ và tên | Vũ Thường Tín |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602955 |
| GitHub | Nituv05 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-03904 |

MSSV chứa chữ nên tôi giữ nguyên `2A202602955` cho `LAB_STUDENT_ID` và việc chia dataset; riêng công thức chọn trip dùng phần số `202602955 % 26 = 9`, tương ứng `trip-03904` trong danh sách đã sắp xếp của `cli list-trips` (chỉ số bắt đầu từ 0).

## Kết quả cuối

- `dataset_version` của bạn: `be21992bca74` (khớp [run_summary.json](report/evidence/run_summary.json) của lần chạy cuối từ lake trống).
- Số khung hình train / val / test của bạn: **695 / 165 / 160** (tổng 1020).
- [report/verify.txt](report/verify.txt): **27/27 checks passed**; [lần chạy cuối](report/checkpoints/step5_final_run.txt) đạt cả năm bước, `verify_all_passed=true`.
- DAG từ lake trống và DAG sau phục hồi đều **37/37 task success**; đủ **6 mart** trong warehouse, gallery có **1020 dòng**.
- Ảnh thật: [Airflow Graph](report/screenshots/airflow_graph.png), [MinIO lake](report/screenshots/minio_lake.png), [Metabase dashboard](report/screenshots/metabase_dashboard.png), [log task đỏ](report/screenshots/airflow_crash_log.png), [hộp thoại clear](report/screenshots/airflow_clear_task.png), [Graph sau phục hồi](report/screenshots/airflow_recovered_graph.png), [câu SQL riêng](report/screenshots/metabase_custom_question.png).

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

Theo [leakage.json](report/evidence/leakage.json), **1020 khung hợp lệ thuộc 26 trip** được đưa vào thí nghiệm: chia theo khung hình làm **26/26 trip nằm ở nhiều split**, và **26/26 trip có khung ở cả train lẫn test**. Gold chia theo trip có **0 trip bị chia cắt**. Mỗi drive có 9 trip train, 2 val và 2 test; tổng cộng **18/4/4 trip**, tương ứng **695/165/160 khung**. Tỷ lệ 70/15/15 được áp dụng lên số trip và làm tròn theo từng drive, nên tỷ lệ khung không nhất thiết đúng 70/15/15.

Các khung liền nhau trong cùng chuyến chia sẻ cảnh, ánh sáng và quỹ đạo; nếu chia chúng sang train và test, mô hình có thể được chấm trên cảnh gần giống cảnh đã học thay vì chuyến mới. Điểm test vì thế có thể cao giả tạo so với khả năng tổng quát hoá. Thí nghiệm này đo sự giao nhau của trip, chưa huấn luyện mô hình nên không đưa ra mức tăng điểm cụ thể. Chia theo trip tránh kiểu rò rỉ này nhưng chưa bảo đảm các trip khác nhau không đi cùng tuyến đường.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Trong mã của lab, version là **12 ký tự đầu của SHA256** từ các dòng `frame_id|split` được sắp xếp theo `frame_id`. Cùng tập khung và cách chia split sẽ tạo cùng version dù chạy lúc khác; ngày giờ hoặc số đếm sẽ đổi chỉ vì chạy lại và không nhận diện được nội dung tương đương. Lần tạo đầu và lần `export-dataset` kế tiếp của tôi đều cho **`be21992bca74`**, **1020 khung**, split **695/165/160**. Tôi cũng tính lại băm từ bảng Delta gold và xác nhận trùng version.

MSSV được nối với `trip_id` khi tính MD5 để xếp thứ tự trip trong mỗi drive. Khi thử mã **`2A202602956`** bằng biến môi trường cho riêng lệnh chạy, version đổi thành **`a419c86bfddd`**, split thành **700/167/153**; tổng khung và số trip mỗi split vẫn giữ nguyên. Chạy lại bằng mã chính thức **`2A202602955`** khôi phục **`be21992bca74`** và **695/165/160**, đạt **7/7 kiểm tra**. Hai mã trong thí nghiệm cho version khác nhau vì thay đổi việc gán split; không phải chỉ băm trực tiếp MSSV. Bằng chứng ở [step4_export_repeat.txt](report/checkpoints/step4_export_repeat.txt), [step4_student_id_variant.txt](report/checkpoints/step4_student_id_variant.txt) và [step4_student_id_restored.txt](report/checkpoints/step4_student_id_restored.txt).

Giới hạn: version này băm **ID khung và split**, không băm toàn bộ ảnh, nhãn hay metadata. Thay nội dung ảnh dưới cùng ID mà không đổi split có thể giữ nguyên version; vì vậy phải đọc thêm SHA256 và lineage để truy nguồn. `created_at` và phiên bản commit Delta cũng có thể đổi trong khi dataset version giữ nguyên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest cuối của **`be21992bca74`** được lưu nguyên bản ở [step5_final_manifest.json](report/checkpoints/step5_final_manifest.json); snapshot checkpoint 4 vẫn được giữ tại [step4_manifest.json](report/checkpoints/step4_manifest.json). Nó ghi **1020 khung**, gồm **436 của drive A và 584 của B**, và các phiên bản bảng upstream sau:

| Bảng upstream | Delta version của lần chạy cuối |
|---|---:|
| `bronze.raw_telemetry_a`, `bronze.raw_telemetry_b`, `bronze.trips_raw` | 0 |
| `bronze.video_files` | 1 |
| `silver.frames` | 25 |
| `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` | 0 |

SHA256 video A là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Các luật trong manifest loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok` và chia theo trip, phân tầng theo drive với thứ tự MD5. `silver.frames` cuối ở version 25 sau 26 lần ghi trip (version bắt đầu từ 0); checkpoint 4 từng ở version 27 do hai lần chạy lại trong thí nghiệm idempotency. Số commit tăng không đồng nghĩa số khung tăng, và dựng lại lake không thay version dataset khi tập khung và split vẫn giống nhau.

Để tái tạo sau 3 tháng, cần giữ dữ liệu gốc khớp SHA256 trong `data/MANIFEST.json`, manifest dataset, mã và cấu hình pipeline, các dependency cùng MSSV **`2A202602955`**. Nếu giữ lịch sử Delta thì dùng phiên bản upstream để đọc đúng snapshot; nếu dựng lại từ lake trống, số phiên bản commit có thể khác nhưng tập `frame_id|split` và version dataset phải khớp. `manifest_sha256` **`458331e2c4e99601dcc2f26828dfb059bd2f3de389efd72707c8cc91ed35fae1`** kiểm tra file manifest Parquet của lần xuất này, không phải SHA256 của mọi ảnh. Lần chạy cuối đã xác nhận version và checksum manifest Parquet khớp checkpoint 4, trong khi lineage `silver.frames` phản ánh lịch sử ghi mới.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Tôi trigger `day09_reset`, rồi chạy DAG `day09_video_pipeline` từ lake trống: lần `cp5_baseline` có **37/37 task success**, verify **27/27**, version **`be21992bca74`**. Tiếp đó bật `LAB_FAIL_ONCE=trip-03904`, reset và trigger `cp5_fail_once`. Kết quả là **1 task failed** (`silver_frames`, map index **9**), **7 task upstream_failed** và **29 task success**. Log ghi rõ **`simulated crash while extracting trip-03904; run it again`**; bằng chứng ở [airflow_crash_log.png](report/screenshots/airflow_crash_log.png) và [step5_airflow_failed.json](report/checkpoints/step5_airflow_failed.json).

Trong giao diện, tôi chọn **Clear task** cho đúng map index 9 của lần chạy này. Hộp thoại mặc định bật **Downstream**, nên danh sách affected có **8 task**: task đỏ và 7 task bị chặn; không chọn các trip thành công, không chọn lần chạy trước/sau hoặc upstream. Đây là điểm cần làm rõ so với cách diễn đạt “clear task đỏ” của hướng dẫn: 7 task `upstream_failed` cũng cần trở về trạng thái có thể được lên lịch. Tham số thao tác và ảnh hộp thoại nằm ở [step5_clear_action.json](report/checkpoints/step5_clear_action.json) và [airflow_clear_task.png](report/screenshots/airflow_clear_task.png).

Sau clear, chỉ **1 task trích khung được chạy lại**, từ attempt **1 lên 2**; 7 task downstream chạy lần đầu trong lần DAG này vì trước đó chưa được thực thi. Cả **29 task đã success giữ nguyên thời điểm bắt đầu**, chứng minh chúng không chạy lại. DAG kết thúc **37/37 success**, verify **27/27**, bảng Silver có **1246 dòng và 1246 frame ID duy nhất**, trip lỗi có **22 khung**, Gold có **1020 khung**; version vẫn **`be21992bca74`**, giống baseline. Bằng chứng ở [airflow_recovered_graph.png](report/screenshots/airflow_recovered_graph.png), [step5_airflow_recovered.json](report/checkpoints/step5_airflow_recovered.json), [step5_recovered_dataset.json](report/checkpoints/step5_recovered_dataset.json) và [step5_recovered_verify.txt](report/checkpoints/step5_recovered_verify.txt).

Tôi đã xoá `LAB_FAIL_ONCE` và tạo lại container để tắt mô phỏng lỗi. Hook của lab thực tế báo lỗi **trước bước FFmpeg**, nên bài tập này chứng minh việc phục hồi và phạm vi chạy lại, chưa thử lỗi giữa lúc commit Delta hoặc giữa lúc upload ảnh. Thí nghiệm idempotency ở câu 4 bổ sung bằng chứng về việc thay thế dữ liệu đã tồn tại.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Tôi dự báo ba nút nghẽn sau từ cấu hình lab; đây chưa phải kết quả load test 500 xe. Chỉ hai drive hiện đã tạo **1246 khung**, còn dữ liệu thực tế mỗi xe dài bao lâu và có bao nhiêu camera sẽ quyết định tải cuối cùng.

1. **Trích video bằng FFmpeg:** mỗi trip tải lại video của drive, giải mã và upload từng JPG; `silver_frames` hiện giới hạn một task đồng thời nên backlog sẽ tăng. Tôi chuyển phần xử lý ảnh sang worker CPU phân tán theo trip, dùng cache video theo drive và xử lý theo batch; có thể dùng [Ray Data](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html) để mở rộng tiền xử lý ảnh/video. Giữ ID ổn định và overwrite theo trip để retry an toàn.
2. **DuckDB + dbt trên một máy:** các phép nối, sort và cửa sổ cùng dùng tài nguyên của một máy và file DuckDB chung; tăng số worker không tự làm bước SQL mở rộng theo nhiều máy. DuckDB vẫn có khả năng spill ra đĩa, nên không thể kết luận cứ vượt RAM là hỏng, nhưng giới hạn CPU/I/O và cách ghi chung sẽ thành nút nghẽn. Tôi chuyển biến đổi lớn sang Spark + Delta trên cụm, xử lý tăng dần theo partition thay vì dựng lại toàn bộ; điều này nối với lựa chọn công cụ ELT/SQL khi quy mô tăng. Tham khảo [mô hình một node của DuckDB](https://www.duckdb.org/faq).
3. **Airflow LocalExecutor trong cùng container:** scheduler, webserver và task dùng chung máy; lab chỉ cho parallelism 2 và RAM container 2500 MB, nên vừa hạn chế throughput vừa tạo điểm lỗi tập trung. Tôi dùng KubernetesExecutor hoặc CeleryExecutor với worker tách biệt, giới hạn tài nguyên và điều chỉnh concurrency theo CPU, storage và nhu cầu từng task. Airflow tiếp tục điều phối và kiểm tra chất lượng; việc thay executor không tự mở rộng file DuckDB hay object storage. [Tài liệu Airflow về executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html) phân biệt triển khai local với thực thi trên nhiều máy.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm câu hỏi **“Trip nào có tỷ lệ khung bị loại cao nhất?”** vào [dashboard Metabase cục bộ](http://localhost:3000/dashboard/2), [card riêng](http://localhost:3000/question/46). Dashboard có sáu câu hỏi dựng sẵn và câu SQL này; tài khoản lab là `lab@example.com`, mật khẩu mặc định `Day9-lab-pass!`.

```sql
select drive_id, trip_id, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 2) as dropped_pct,
       avg_speed_kmh, split
from marts.mart_trip_summary
order by dropped_pct desc, frames desc, trip_id;
```

Kết quả cao nhất là **drive A, `trip-04622`**: **60 khung, giữ 26, loại 34, tỷ lệ 56.67%**, tốc độ trung bình **67.1 km/h**, split **val**. Trip của bài tập lỗi `trip-03904` đứng thứ hai với **11/22 khung bị loại, 50%**. Tỷ lệ này giúp ưu tiên kiểm tra chuyến mất nhiều dữ liệu trước khi huấn luyện; dùng `frames - frames_kept` để đếm khung loại đúng một lần, tránh cộng các cờ có thể chồng lấp. Truy vấn bổ sung xác nhận `trip-04622` có **34 near-duplicate, 0 đen, 0 mờ, 0 mất đồng bộ**: số liệu cho thấy luật khung trùng là nguyên nhân trực tiếp, chưa đủ để kết luận camera bị hỏng. SQL và 26 dòng kết quả được lưu ở [step5_custom_query.sql](report/checkpoints/step5_custom_query.sql), [step5_metabase_query.json](report/checkpoints/step5_metabase_query.json); ảnh ở [metabase_custom_question.png](report/screenshots/metabase_custom_question.png).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Qua các checkpoint đã chạy, tôi chưa thấy vấn đề vận hành khác ngoài những tình huống đã ghi trong bằng chứng. Kết quả đáng chú ý là chia theo frame làm cả 26 trip có mặt ở train và test, dù các dòng đều hợp lệ theo luật chất lượng. Khung tối vẫn có thể chứa cảnh đường hữu ích: B có 61 khung tối và 58 khung đủ các điều kiện giữ của gold. Tôi chưa có thí nghiệm huấn luyện mô hình để đo mức ảnh hưởng của ngưỡng dHash và leakage lên điểm test. Một giới hạn chưa được kiểm tra là GPS nhảy vị trí hoặc hướng nằm ngoài khoảng hợp lệ: schema hiện chủ yếu kiểm tra tốc độ, thiếu mẫu và trùng; version dataset cũng không tự phát hiện ảnh bị đổi nội dung dưới cùng frame ID.
