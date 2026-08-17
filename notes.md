# Nháp Lab 17 — sổ điều phối

**Trần Nguyễn Thế Nhật** · AICB-P2T2 · Ngày làm: ………

> Quy tắc xuyên suốt: **đo trước, sửa sau**. Số "trước khi sửa" chỉ lấy được một lần duy nhất.
> Chi tiết điều tra của từng sự cố nằm trong phiếu riêng — file này chỉ giữ baseline chung
> và trạng thái tổng thể.

---

## Hồ sơ sự cố

| Phiếu | Nhiệm vụ | Bảng bị ảnh hưởng | Trạng thái |
|---|---|---|---|
| [#1041](PHIEU_SU_CO_1041.md) | 1 · Idempotency | `gold_training_set` | ☑ điều tra ☑ sửa ☑ **kiểm chứng — ĐẠT** |
| [#1043](PHIEU_SU_CO_1043.md) | 2 · Dữ liệu về muộn | `gold_feature_daily` | ☑ điều tra ☑ sửa ☑ **kiểm chứng — ĐẠT** |
| [#1047](PHIEU_SU_CO_1047.md) | 3 · Schema evolution | `silver_tickets`, `quarantine_tickets` | ☑ điều tra ☑ sửa ☑ **kiểm chứng — ĐẠT** |
| [#1052](PHIEU_SU_CO_1052.md) | Thưởng A · Small-file | `queries/dashboard.sql` | ☑ điều tra ☑ sửa ☑ **kiểm chứng — ĐẠT** |
| [crash](PHIEU_SU_CO_CONSUMER_CRASH.md) | Thưởng B · Delivery semantics | `ingest/consumer.py` | ☑ điều tra ☑ sửa ☑ **kiểm chứng — ĐẠT** |

Tài liệu khác: [HUONG_DAN_LAB17.html](HUONG_DAN_LAB17.html) (runbook) · `REPORT_TEMPLATE.md` → `REPORT.md` (bài nộp)

---

## 0 · Baseline — trạng thái khi tiếp nhận

```bash
make verify 2>&1 | tee verify_before.txt
```

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 14.9s
  run 2/3 … 15.5s
  run 3/3 … 15.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     7c461563f4    d11657ff21    2b76a4f850   ✗
  gold_feature_daily    3269dbe574    3269dbe574    3269dbe574   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✗ True / None

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✗  1 · gold_training_set idempotent & đúng số hàng
  ✗  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✗  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  1/4 tiêu chí đạt
```

**Đọc bảng này:** hai cột đầu đo hai đại lượng khác nhau. `ỔN ĐỊNH` = chạy lại có cho cùng
kết quả không · `SỐ HÀNG` = kết quả đó có đúng không. `gold_feature_daily` **ổn định nhưng
vẫn sai** — đó là hai vấn đề tách biệt.

---

## Công cụ

Hàm truy vấn nhanh — dán vào terminal một lần, dùng cả buổi:

```bash
q() { .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }
```

| Lệnh | Tác dụng |
|---|---|
| `make pipeline` | chạy đường ống một lượt |
| `make verify` | xoá kho, chạy 3 lượt, in bảng chấm — **công cụ phản hồi chính** |
| `make quick` | 1 lượt, nhanh, **không** kiểm tra tính ổn định |
| `make dbt-test` | chạy dbt test |
| `make clean` | xoá kho + target dbt |

---

## Verify tổng thể — sau khi xong cả ba nhiệm vụ

```bash
make clean && make pipeline
make verify 2>&1 | tee verify_after.txt
make dbt-test
```

> ⚠️ Trên máy mới phải chạy `make seed-extra` rồi `make compact` **trước** `make verify`, nếu không
> verify dừng bằng `IOException` ở bước kiểm tra dashboard. Xem mục "Cách tái lập kết quả" đầu
> `REPORT.md`.

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

| Kiểm tra | Kết quả |
|---|---|
| Tổng kết | **4 / 4** tiêu chí đạt |
| Checksum 3 lượt giống hệt ở cả 4 bảng | ☑ |
| `dbt test` pass toàn bộ | **11 / 11** |
| Lượt 4, 5 không đổi số hàng | ☑ |
| *(thưởng A)* `make explain` | ☑ 536,3× · hash không đổi |
| *(thưởng B)* `make crash-test` | ☑ ĐẠT — 20.000 / 20.000 |

---

## Bảng tổng kết cuối báo cáo

Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên:

| Nhiệm vụ | Phép kiểm tra rẻ nhất lẽ ra nên chạy từ đầu |
|---|---|
| 1 | Chạy pipeline hai lần trên cùng một khoảng thời gian rồi so số hàng — bảng idempotent thì con số phải bất động. Sau đó rà mọi model `incremental` xem đã khai `unique_key` chưa, và khoá đó có khớp **grain** ghi ở đầu file không. |
| 2 | Xem mốc lọc incremental lấy từ **event-time** hay **ingestion-time**. Nếu là event-time mà không có lookback, đo ngay phân bố `_ingested_at − event_time` và đối chiếu P99 với độ rộng cửa sổ. Kèm một phép đối soát `left join` từ nguồn xuống đích để đếm bản ghi bị rơi. |
| 3 | Kiểm tra `contract` có bật không và cột quan trọng đã có test **miền giá trị** chưa — contract chỉ ràng buộc kiểu, `priority = 99` vẫn lọt. Kèm theo: đếm NULL theo từng cột **và theo thời gian**. |

Điểm chung của cả ba sự cố:

> Không sự cố nào **sinh ra lỗi**: pipeline chạy xong, không job đỏ, `dbt test` ban đầu pass 9/9 ở
> cả ba trường hợp. Thứ nguy hiểm trong data pipeline không phải job đỏ — job đỏ có người xử lý ngay
> trong vài phút — mà là **job xanh đang âm thầm nhân bản, bỏ sót hoặc vứt bỏ dữ liệu**. Hệ quả cho
> cách kiểm thử: phải nhắm vào **bất biến** (chạy lại cho cùng kết quả · số hàng đối soát được với
> nguồn · miền giá trị đúng contract), không chỉ nhắm vào việc pipeline có chạy xong hay không. Và
> một bộ test chỉ bảo vệ được đúng những bất biến nó được viết ra để bảo vệ — sự im lặng của nó
> không phải bằng chứng dữ liệu sạch.

---

## Checklist nộp bài

- ☑ `make verify` từ trạng thái sạch → **4/4 tiêu chí đạt**
- ☑ Không còn `TODO` nào chưa xử lý trong các file đã sửa
- ☑ `REPORT.md` viết xong, phần "Nguyên nhân" nói về **cơ chế** chứ không liệt kê thao tác
- ☑ Giá trị **P99 = 2,73 ngày** có trong báo cáo
- ☑ Output `make verify` ba lượt đã dán vào báo cáo *(mục 0)*
- ☑ Không đụng file cấm sửa — `expected/`, `seed/generate.py`, `tools/{verify,explain,common}.py`
- ☑ `.venv/`, `warehouse.duckdb`, `data/`, `.omc/` đều nằm trong `.gitignore`
- ☑ Đã push lên remote
- ☑ Ghi rõ trình tự tái lập (`seed-extra` → `compact` → `verify`) ở đầu `REPORT.md`
