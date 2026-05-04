# PLAN — Spark/Docker Path · Day 18 Lakehouse Lab

> Mục tiêu: hoàn thành 4 notebook bằng PySpark + MinIO + Delta Lake chạy trong Docker.
> Stack: `quay.io/jupyter/pyspark-notebook:spark-3.5.0` + `minio/minio:latest`.

---

## Kiến trúc stack

```
┌─────────────────────────────────────────────────────────┐
│  Host machine (port mapping)                            │
│   :8888 → Jupyter Lab    (token: lakehouse)             │
│   :9000 → MinIO S3 API                                  │
│   :9001 → MinIO Console  (minioadmin / minioadmin)      │
│   :4040 → Spark UI        (khi job đang chạy)           │
└────────────────┬────────────────────────────────────────┘
                 │ Docker network
     ┌───────────┼───────────────┐
     ▼           ▼               ▼
 [minio]    [minio-init]     [spark]
  MinIO       tạo 4 bucket   PySpark 3.5 + Jupyter
  /data       lakehouse       /workspace = repo root
              bronze          delta-spark 3.2.0
              silver          hadoop-aws 3.3.4
              gold
```

**Buckets → Delta table paths:**

| Bucket | Notebook dùng |
|---|---|
| `s3a://lakehouse/` | NB1 (users_delta), NB2 (events_smallfiles), NB3 (customers_tt), smoke |
| `s3a://bronze/llm_calls_raw` | NB4 Bronze |
| `s3a://silver/llm_calls` | NB4 Silver |
| `s3a://gold/llm_daily_metrics` | NB4 Gold |

---

## Yêu cầu trước khi bắt đầu

- [ ] Docker Desktop ≥ 4.x đang chạy
- [ ] RAM khả dụng ≥ 8 GB (Spark driver + executor + MinIO)
- [ ] Port 8888, 9000, 9001, 4040 chưa bị chiếm
- [ ] Đường truyền internet để pull image lần đầu (~2 GB)
- [ ] Git clone repo về local (đã có nếu đang đọc file này)

---

## Phase 0 — Kiểm tra môi trường

```bash
# Xác nhận Docker chạy và đủ RAM
docker info | grep "Total Memory"
docker ps

# Xác nhận port rảnh
# Windows PowerShell:
netstat -ano | findstr ":8888 :9000 :9001 :4040"
# Nếu có kết quả → port đang bị chiếm → phải giải phóng trước
```

**Nếu port 8888 bị chiếm:** sửa dòng `"8888:8888"` trong
[docker/docker-compose.yml](docker/docker-compose.yml) thành `"8889:8888"`, rồi
truy cập `http://localhost:8889`.

---

## Phase 1 — Khởi động stack

### Bước 1.1 — Pull và start (lần đầu ~3-5 phút)

```bash
make spark-up
```

Lệnh này chạy `docker compose -f docker/docker-compose.yml up -d`.
Thứ tự khởi động:
1. `minio` start → healthcheck pass (curl `/minio/health/live`)
2. `minio-init` chạy `mc mb` tạo 4 bucket → exit 0
3. `spark` start → pip install `delta-spark`, `jupytext`, `faker` → Jupyter up

### Bước 1.2 — Theo dõi log startup

```bash
docker compose -f docker/docker-compose.yml logs -f spark
```

Đợi cho đến khi thấy dòng:
```
>> Starting Jupyter (token: lakehouse)…
```

Thoát theo dõi: `Ctrl+C`

### Bước 1.3 — Kiểm tra containers

```bash
docker ps
# Kỳ vọng: 2 containers running (lakehouse-minio, lakehouse-spark)
# lakehouse-minio-init đã exited(0) — bình thường
```

---

## Phase 2 — Smoke test

### Bước 2.1 — Chạy smoke test

```bash
make spark-smoke
```

Lệnh này thực thi `scripts/verify.py` bên trong container spark.
Test gồm 4 bước:
1. Boot Spark session với Delta extensions + S3A config
2. Write Delta table tới `s3a://lakehouse/_smoke`
3. Read back và kiểm tra 10 rows
4. Append + kiểm tra time travel (v0 vẫn = 10 rows)

**Kết quả thành công:**
```
All checks passed — lab is ready. Open http://localhost:8888
```

**Lần đầu chạy:** Spark phải tải Maven JARs (~200 MB) từ Maven Central.
Nếu smoke test FAIL lần đầu — chờ 30 giây rồi chạy lại một lần.

### Bước 2.2 — Mở Jupyter

Trình duyệt → [http://localhost:8888](http://localhost:8888)

Token: `lakehouse`

Điều hướng tới thư mục `notebooks-spark/` — đây là 4 notebook PySpark.

---

## Phase 3 — Generate dữ liệu Bronze (cho NB4)

```bash
make spark-data
```

Tạo **1 triệu rows** LLM call logs → `s3a://bronze/llm_calls_raw` (Delta format).
Chờ ~2-3 phút. Log sẽ in:
```
Wrote 1,000,000 rows to s3a://bronze/llm_calls_raw
```

> Chạy lệnh này **trước khi mở NB4**. NB1-NB3 không cần.

---

## Phase 4 — Thực hiện 4 Notebooks

### NB1 — Delta Lake Basics
**File:** [notebooks-spark/01_delta_basics.py](notebooks-spark/01_delta_basics.py)

**Mục tiêu:** Write/read Delta, schema enforcement, schema evolution.

**Thực hiện từng cell:**

| Cell | Hành động | Kỳ vọng |
|---|---|---|
| Import + get_spark | Khởi động Spark session | Không có lỗi; lần đầu ~60s |
| Write Delta table | Ghi 3 rows → `s3a://lakehouse/users_delta` | Không lỗi |
| Read + DESCRIBE HISTORY | Đọc lại và xem transaction log | Show 3 rows; history version=0 |
| Schema enforcement | Ghi row với `age="thirty"` (sai type) | In `BLOCKED by schema enforcement` |
| Schema evolution | Ghi với `mergeSchema=true` + cột `tier` | Show 4 rows; có cột `tier` |

**Verify deliverable:**
```bash
# Vào MinIO Console http://localhost:9001
# Bucket: lakehouse → users_delta/_delta_log/
# Phải thấy: 00000000000000000000.json
```

**Screenshot cần chụp:** Output của cell cuối cùng (bảng có cột `tier`).

---

### NB2 — OPTIMIZE + Z-ORDER
**File:** [notebooks-spark/02_optimize_zorder.py](notebooks-spark/02_optimize_zorder.py)

**Mục tiêu:** Tạo small-file problem → OPTIMIZE → benchmark speedup ≥ 3×.

**Cảnh báo:** Cell "Manufacture small-file problem" chạy **200 vòng append** —
có thể mất 5-10 phút trên Spark Docker. Đây là bình thường.

**Thực hiện từng cell:**

| Cell | Hành động | Kỳ vọng |
|---|---|---|
| Reset path | DROP TABLE + overwrite | Idempotent |
| Manufacture 200 batches | 200 × 500 rows append | Chờ ~5-10 phút; total 100K rows |
| Benchmark BEFORE | Query `user_id=4242 AND kind='purchase'` | In thời gian trước optimize |
| OPTIMIZE + ZORDER | `OPTIMIZE ZORDER BY (user_id)` | Merge ~200 files → ít files |
| Benchmark AFTER | Cùng query | In thời gian sau optimize |
| DESCRIBE DETAIL | Xem `numFiles` | `numFiles` giảm rõ rệt |

**Pass condition:** `Speedup: X.X×  (target ≥ 3×)` — nếu speedup < 3× thì
check `files-pruned ratio` (DESCRIBE DETAIL — trước/sau). Một trong hai ≥ target là pass.

**Screenshot cần chụp:** Dòng in `BEFORE / AFTER / Speedup`.

---

### NB3 — Time Travel + MERGE Upsert
**File:** [notebooks-spark/03_time_travel.py](notebooks-spark/03_time_travel.py)

**Mục tiêu:** Build version history, MERGE 100K rows, RESTORE, xem ≥ 5 versions.

**Thực hiện từng cell:**

| Cell | Hành động | Kỳ vọng |
|---|---|---|
| Build version history | Write v0(100K) → v1(schema add) → v2(MERGE) → v3(bad data) | 4 phiên bản được tạo |
| DESCRIBE HISTORY (lần 1) | Xem 4 versions (v0–v3) | 4 rows |
| Time travel queries | `versionAsOf=0` → 100K rows; `versionAsOf=1` → có cột `tier` | Kết quả đúng |
| RESTORE to v2 | `restoreToVersion(2)` | In thời gian; target < 30s |
| Verify restore | Count `score < 0` | Kết quả = 0 |
| DESCRIBE HISTORY (lần 2) | Xem sau khi RESTORE | ≥ 5 versions (v0,v1,v2,v3, RESTORE) |

**Pass condition:** Lần `DESCRIBE HISTORY` **sau** RESTORE phải in ≥ 5 versions.
`Total versions: X  (target ≥ 5)`

**Screenshot cần chụp:** Output cell cuối — list `v0 WRITE … v4 RESTORE` + `Total versions`.

---

### NB4 — Medallion Pipeline (Bronze → Silver → Gold)
**File:** [notebooks-spark/04_medallion.py](notebooks-spark/04_medallion.py)

**Pre-req:** `make spark-data` đã chạy xong (Phase 3 ở trên).

**Thực hiện từng cell:**

| Cell | Hành động | Kỳ vọng |
|---|---|---|
| Import + get_spark | Khởi động | OK |
| Bronze verify | `spark.read.format("delta").load(BRONZE).count()` | 1,000,000 |
| Silver transform | Parse JSON, dedup by `request_id`, partition by `date` | |
| Silver verify | In `Silver rows` vs `Bronze rows` | `Silver < Bronze` (dedup hoạt động) |
| Gold aggregate | Group by `(date, model)`, tính p50/p95/cost_usd/error_rate | |
| Gold OPTIMIZE | `ZORDER BY (model)` | OK |
| Gold verify | Show top 20 rows | ≥ 7 ngày × 3 models có dữ liệu |

**Pass condition:**
- `Silver rows < Bronze rows` (dedup observable)
- Gold table có `date` × `model` grid với ≥ 7 distinct dates, 3 models
- Cột `cost_usd`, `p50_latency_ms`, `p95_latency_ms`, `error_rate` có giá trị non-null, non-zero

**Screenshot cần chụp:** Cell Silver verify (Bronze vs Silver count) + cell Gold show().

---

## Phase 5 — Verify toàn bộ trong MinIO Console

Mở [http://localhost:9001](http://localhost:9001) → đăng nhập `minioadmin/minioadmin`

Kiểm tra bucket structure:

```
lakehouse/
  users_delta/          ← NB1
    _delta_log/
      00000000000000000000.json   ← phải tồn tại
  events_smallfiles/    ← NB2
  customers_tt/         ← NB3
  _smoke/               ← smoke test

bronze/
  llm_calls_raw/        ← NB4 Bronze
    _delta_log/

silver/
  llm_calls/            ← NB4 Silver
    _delta_log/

gold/
  llm_daily_metrics/    ← NB4 Gold
    _delta_log/
```

---

## Phase 6 — Dọn dẹp

### Dừng stack (giữ data):
```bash
make spark-down
# Tương đương: docker compose -f docker/docker-compose.yml down
# MinIO volume vẫn còn → start lại bằng make spark-up
```

### Xóa hoàn toàn (wipe data + images cached):
```bash
make spark-clean
# Tương đương: docker compose -f docker/docker-compose.yml down -v
# XÓA MinIO volume + ivy-cache + ipython-cache
```

---

## Troubleshooting

| Triệu chứng | Nguyên nhân | Fix |
|---|---|---|
| `make spark-up` treo không xong | Docker chưa khởi động | Mở Docker Desktop → chờ "Engine running" |
| Smoke test FAIL lần đầu | Maven Central đang tải JARs (~200 MB) | Chờ 60s → chạy lại `make spark-smoke` |
| Jupyter timeout / không mở được | Spark container chưa ready | `docker compose logs spark` → chờ dòng "Starting Jupyter" |
| NB4 lỗi `Path does not exist` cho BRONZE | Quên generate data | `make spark-data` → chờ xong → chạy lại NB4 |
| NB3 RESTORE > 30s | MinIO I/O chậm (disk) | Chấp nhận nếu < 60s; ghi chú trong screenshot |
| NB2 Speedup < 3× | Spark executor RAM thấp | Kiểm tra Docker Desktop → Resources → tăng RAM lên 8 GB |
| Port 8888 conflict | App khác đang dùng | Sửa `docker-compose.yml` line 43: `"8889:8888"` |
| `minio-init` exited(1) | MinIO chưa healthy kịp | `docker compose restart minio-init` |
| Spark OOM (OutOfMemoryError) | RAM < 4 GB cho Spark | Đóng apps khác; tăng Docker memory limit |

### Debug commands hữu ích:

```bash
# Xem log realtime của tất cả services
docker compose -f docker/docker-compose.yml logs -f

# Chỉ xem log spark
docker compose -f docker/docker-compose.yml logs -f spark

# Exec vào spark container
docker exec -it lakehouse-spark bash

# Kiểm tra MinIO buckets từ bên trong container
docker exec lakehouse-minio mc ls local/
```

---

## Checklist deliverable cuối

- [ ] **NB1** — Screenshot bảng có cột `tier`; MinIO console thấy `_delta_log/*.json`
- [ ] **NB2** — Screenshot dòng `Speedup: X.X×` ≥ 3× hoặc `numFiles` giảm ≥ 10×
- [ ] **NB3** — Screenshot `Total versions: X (target ≥ 5)` sau RESTORE
- [ ] **NB4** — Screenshot Silver < Bronze count + Gold table với ≥ 7 dates × 3 models
- [ ] 4 `.ipynb` đã run (có output) trong `notebooks-spark/`
- [ ] `submission/REFLECTION.md` (≤ 200 words)

---

## Submission

```bash
# Fork repo → push notebooks đã run + REFLECTION
git add notebooks-spark/*.ipynb submission/REFLECTION.md
git commit -m "[NXX] Lab18 — <Họ Tên>"
git push origin main
# Tạo PR vào upstream với title: [NXX] Lab18 — <Họ Tên>
```
