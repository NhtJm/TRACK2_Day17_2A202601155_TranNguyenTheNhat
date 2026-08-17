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

```
(dán output vào đây)

```

| Kiểm tra | Kết quả |
|---|---|
| Tổng kết | ……… / 4 tiêu chí đạt |
| Checksum 3 lượt giống hệt ở cả 4 bảng | ☐ |
| `dbt test` pass toàn bộ | ……… / ……… |
| Lượt 4, 5 không đổi số hàng | ☐ |

---

## Bảng tổng kết cuối báo cáo

Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên:

| Nhiệm vụ | Phép kiểm tra rẻ nhất lẽ ra nên chạy từ đầu |
|---|---|
| 1 | |
| 2 | |
| 3 | |

Điểm chung của cả ba sự cố:

>

---

## Checklist nộp bài

- ☐ `make verify` từ trạng thái sạch → 4/4 tiêu chí đạt
- ☐ Không còn `TODO` nào chưa xử lý trong các file đã sửa
- ☐ `REPORT.md` viết xong, phần "Nguyên nhân" nói về **cơ chế** chứ không liệt kê thao tác
- ☐ Giá trị **P99** có trong báo cáo (bắt buộc, nhiệm vụ 2)
- ☐ Output `make verify` ba lượt đã dán vào báo cáo
- ☐ `make clean` trước khi nén / commit
- ☐ Đã push lên remote
