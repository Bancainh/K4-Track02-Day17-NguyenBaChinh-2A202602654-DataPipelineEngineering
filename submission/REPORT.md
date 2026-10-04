# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** NguyenBaChinh / 2A202602654 

**Repo:** https://github.com/Bancainh/K4-Track02-Day17-NguyenBaChinh-2A202602654-DataPipelineEngineering

**Commit bài nộp:** Chưa tạo commit; các thay đổi hiện ở working tree, cần commit/push trước khi nộp.

**AI đã dùng:** Codex hỗ trợ đọc đề, tìm/sửa lỗi, chạy kiểm tra và soạn báo cáo; học viên cần review diff và hiểu cách sửa.

**Nguồn tham khảo:** README, SUBMISSION, RUBRIC, RULES, CHECKPOINTS và code trong repo; không dùng lời giải bên ngoài.

**Môi trường chạy:** Windows PowerShell, Python 3.11.9, DuckDB 1.5.6, dbt-core 1.12.5, dbt-duckdb 1.11.0. Bằng chứng sinh ngày 04/10/2026 (UTC+7).

## 1. Ba lỗi

Baseline: verify **8/18**, pytest **9 failed, 25 passed**.

| | Silver | Late data | CDC delete |
|---|---|---|---|
| Triệu chứng | 24 hàng/12 ticket; T-91 có ba trạng thái; chunks trùng. | u05 ngày 08-12 chỉ có 2 events/0 down; checksum khác full recompute. | T-97 còn ở Silver, snapshot mới nhất và 2 chunks. |
| Nguyên nhân | INSERT nối batch; dedup nội batch chưa chống replay giữa batch. | Lookback 0 bỏ event đến trễ 3 ngày. | Chỉ đọc khoá từ `after`; delete có `after=null` bị lọc mất. |
| Sửa | `silver.py`: PRIMARY KEY, MERGE theo ticket_id; chỉ UPDATE khi LSN nguồn lớn hơn. | `config.py`: LOOKBACK_DAYS=3; dùng logic overwrite cửa sổ event time có sẵn. | `staging.py`: coalesce khoá từ after/before/key; vẫn bỏ Kafka tombstone. PII lấy từ after nên thành NULL khi delete. |
| Slide | Silver có khoá; idempotency; newest LSN wins. | Event time; đo lateness; lookback. | CDC log-based; tombstone; xoá phải lan xuống Gold. |

## 2. Các con số

- 43 records Bronze: P50=0, P95=2.90, P99=3, max=3 ngày → lookback=ceil(P99)=3.
- Gold checksum: `39e115c510ecdf526800eac227158a4f`; C0=C1=C2=C3, **PASS**.
- Sau sửa: **18/18 ALL PASS**, **34 tests passed**, dbt **PASS=19**, đối chiếu **PARITY**.

## 3. Lựa chọn kỹ thuật

- MERGE giữ một trạng thái/ticket; LSN guard ngăn batch cũ ghi đè. Feature là aggregate nên ghi đè partition tránh cộng lặp.
- Tombstone giữ khoá và LSN chống hồi sinh khi replay; đánh đổi là phải lưu hàng đánh dấu xoá.
- Snapshot đọc Bronze as-of, priority lúc tạo tránh rò rỉ tương lai; bản cũ bất biến để tái lập training.
- DuckDB chạy local trên seed nhỏ; dbt thể hiện merge/microbatch, contract và parity. Spark tăng chi phí vận hành không cần thiết ở quy mô này.
- Lookback 3 bao phủ P99 đã đo; dữ liệu trễ hơn cần backfill và đo lại định kỳ.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot cũ vẫn chứa T-97: cần quy trình erasure riêng, purge/redact Bronze, transcripts, snapshots, caches và bản sao; thu hồi dataset/model bị ảnh hưởng, dựng version sạch, lưu audit không chứa PII. Chấp nhận mất khả năng tái lập bản đã xoá.
2. Đặt chốt regex + NER tiếng Việt tại Silver, kiểm tra lại trước Gold/training; vùng nghi PII vào quarantine. Đo precision/recall trên mẫu gán nhãn và tỷ lệ lọt PII, ưu tiên recall; kiểm tra cả tên và dữ liệu không dấu.


## 5. Output thực tế

PowerShell, chạy từ gốc repo. Chỉ bỏ mã màu ANSI và khoảng trắng cuối dòng; nội dung output giữ nguyên.

```powershell
.\.venv\Scripts\python.exe -m scripts.verify
```

```text
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```powershell
.\.venv\Scripts\python.exe -m pytest
```

```text
..................................                                       [100%]
34 passed in 3.15s
```

```powershell
.\.venv\Scripts\python.exe -m scripts.rerun_check
```

```text
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```powershell
.\.venv\Scripts\python.exe main.py --lateness
```

```text
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```powershell
$env:DO_NOT_TRACK = '1'
Push-Location dbt_project
try {
    ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
} finally {
    Pop-Location
}
```

```text
05:19:37  Running with dbt=1.12.5
05:19:37  Registered adapter: duckdb=1.11.0
05:19:38  Unable to do partial parsing because saved manifest not found. Starting full parse.
05:19:42  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:19:42
05:19:42  Concurrency: 1 threads (target='dev')
05:19:42
05:19:46  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:19:46  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.10s]
05:19:46  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:19:46  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
05:19:46  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:19:47  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.14s]
05:19:47  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:19:47  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.21s]
05:19:47  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:19:47  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.14s]
05:19:47  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:19:47  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.06s]
05:19:47  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:19:47  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
05:19:47  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:19:47  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
05:19:47  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:19:47  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
05:19:47  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:19:47  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
05:19:47  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:19:47  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:19:47  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:19:47  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
05:19:47  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:19:47  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.08s]
05:19:47  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:19:47  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.04s]
05:19:47  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:19:47  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
05:19:47  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:19:47  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:19:47  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
05:19:47  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.11s]
05:19:48  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.06s]
05:19:48  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
05:19:48  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.05s]
05:19:48  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
05:19:48  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:19:48  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.05s]
05:19:48  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.47s]
05:19:48  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:19:48  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
05:19:48  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:19:48  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:19:48  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:19:48  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
05:19:48
05:19:48  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 5.58 seconds (5.58s).
05:19:48
05:19:48  Completed successfully
05:19:48
05:19:48  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```powershell
# Same dbt command, second build on existing dbt.duckdb
```

```text
05:21:07  Running with dbt=1.12.5
05:21:08  Registered adapter: duckdb=1.11.0
05:21:09  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:21:09
05:21:09  Concurrency: 1 threads (target='dev')
05:21:09
05:21:09  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:21:09  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.10s]
05:21:09  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:21:09  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
05:21:09  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:21:09  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.17s]
05:21:09  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:21:09  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.17s]
05:21:09  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:21:10  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.21s]
05:21:10  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:21:10  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
05:21:10  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:21:10  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
05:21:10  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:21:10  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
05:21:10  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:21:10  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
05:21:10  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:21:10  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
05:21:10  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:21:10  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:21:10  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:21:10  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
05:21:10  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:21:10  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
05:21:10  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:21:10  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
05:21:10  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:21:10  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:21:10  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:21:10  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.06s]
05:21:10  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
05:21:10  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
05:21:10  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
05:21:10  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
05:21:10  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
05:21:10  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:21:10  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
05:21:10  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.35s]
05:21:10  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:21:10  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
05:21:10  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:21:10  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:21:10  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:21:10  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
05:21:11
05:21:11  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.72 seconds (1.72s).
05:21:11
05:21:11  Completed successfully
05:21:11
05:21:11  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```powershell
.\.venv\Scripts\python.exe -m scripts.parity
```

```text
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

## 6. Bonus B1: LLM cache

`pipeline/llm_label.py`: cache key = SHA-256(input) + actual model + prompt version; cache both valid and invalid responses. Strict JSON validation accepts only bug/billing/other; invalid responses enter a keyed quarantine. Estimate tokens/cost for cache misses before calling. Rebuild current Gold labels from live tickets, with model/prompt/input_hash metadata. Changing input/model/prompt creates new cache entries; retrying cached failures requires an explicit version/input change. FakeLLM and prices are simulations; no paid API is called.

```powershell
.\.venv\Scripts\python.exe -m scripts.bonus_llm
```

```text
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

Additional in-memory checks passed: cached invalid responses, changed input/model, deleted ticket removal, and strict JSON rejection. Bonus B2 was not attempted.
