# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Nguyễn Thế Nhật  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## Cách tái lập kết quả trên máy mới

```bash
make setup        # venv + thư viện + sinh 14 ngày seed
make seed-extra   # ~30s — sinh data/gold_events/ cho bài mở rộng A
make compact      # bố trí lại thành data/gold_events_v2/
make verify       # 4/4 tiêu chí đạt
make crash-test   # bài mở rộng B: ĐẠT
```

> ⚠️ **Phải chạy `make seed-extra` và `make compact` trước `make verify`.**
>
> `expected/dashboard_baseline.json` được commit sẵn trong repo gốc, nên `tools/verify.py:231`
> **luôn** gọi `dashboard_check()`; hàm này lại không bắt lỗi khi dataset Parquet chưa tồn tại
> (`tools/verify.py:125-132`). Trong khi đó `data/` nằm trong `.gitignore` — theo đúng thiết kế của
> lab, vì `RUBRIC.md` trừ 3 điểm nếu nộp kèm thư mục này.
>
> Hệ quả: trên một bản clone mới, `make verify` sẽ dừng bằng `IOException: No files found` thay vì
> in bảng chấm. Điều này đúng với **cả repo gốc** (khi đó thiếu `data/gold_events/`) lẫn bản đã sửa
> (thiếu `data/gold_events_v2/`) — tôi đã kiểm chứng cả hai trường hợp. Đây là đặc tính sẵn có của
> lab, không phải hệ quả của các thay đổi trong bài này; chỉ khác ở chỗ bản đã sửa cần thêm một
> lệnh `make compact`, vì dataset tối ưu là **sản phẩm** của bài mở rộng A chứ không phải dữ liệu
> nguồn.
>
> Tôi không commit `data/gold_events_v2/` (3,8 MB) để tránh mục trừ điểm nói trên, và không sửa
> `tools/verify.py` vì đó là file nằm trong danh sách cấm sửa của `RUBRIC.md`.

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy — <strong>4/4 tiêu chí đạt</strong></summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 15.2s
  run 2/3 … 15.5s
  run 3/3 … 15.5s

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

</details>

Tổng kết: **4 / 4 tiêu chí đạt** — ba nhiệm vụ bắt buộc và **cả hai bài mở rộng** đã hoàn thành, kiểm chứng đầy đủ.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Phiếu #1041: sau khi bấm Clear Task trên Airflow cho chạy lại, `gold_training_set` phình lên; chạy lại lần nữa lại tăng thêm. Không có lỗi đỏ, `dbt test` vẫn pass 9/9. Đo trên kho sạch: lượt 1 cho 13.790 hàng, lượt 2 cho 26.270 hàng — tăng đúng **12.480 hàng mỗi lượt**, trong khi số ticket phân biệt đứng yên ở 12.480. Baseline `make verify` (3 lượt) cho 38.750 hàng với checksum khác nhau ở cả ba lượt. |
| **Nguyên nhân** | Model `gold_training_set` khai `materialized='incremental'` nhưng **không khai `unique_key`**, nên dbt không có khoá để so khớp và sinh ra câu `INSERT INTO` thay vì `MERGE` — mọi lượt chạy lại trên cùng một partition là ghi **thêm** dòng chứ không ghi **đè**. Nguồn CDC lại chứa 1.310 bản ghi `op='u'`, khiến một ticket được tạo ngày D1 rồi sửa ngày D2 lọt qua mệnh đề `WHERE` theo `run_date` ở **hai partition khác nhau**; do đó ngay cả cách khắc phục quen thuộc "xoá partition ngày rồi ghi lại" cũng không dọn được bản sao nằm ở partition cũ. Hệ quả: mỗi lượt chạy cộng thêm đúng 12.480 hàng, cộng 1.310 hàng ma mang trạng thái đã lỗi thời — theo đúng công thức `12.480 × N + 1.310` — mà không sinh bất kỳ lỗi nào để cảnh báo. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`; giữ nguyên mệnh đề `WHERE` theo `run_date` vì đó là tối ưu backfill có chủ đích, không phải lỗi. · `dags/ai_training_pipeline.py`: `catchup=False` và `max_active_runs=1` — hai tham số này chỉ **giảm tần suất kích hoạt**, không phải root cause. |
| **Bằng chứng** | trước: **38.750** hàng / 12.480 ticket phân biệt, checksum 3 lượt `7c461563f4` · `d11657ff21` · `2b76a4f850` (khác nhau) · sau: **12.480** hàng, checksum 3 lượt giống hệt nhau (`8622572a97` ngay sau khi sửa nhiệm vụ 1; chuyển thành `8dd7c98653` sau khi nhiệm vụ 3 chữa cột `priority` — checksum tính trên cột này nên nội dung đổi là đúng như mong đợi, tính ổn định vẫn nguyên); chạy thêm lượt 4 và 5 vẫn đúng 12.480. Số ticket bị rải trên nhiều partition giảm từ **1.310 xuống 0**. SQL dbt sinh ra chuyển từ `INSERT INTO` sang `MERGE INTO ... ON ticket_id ... WHEN MATCHED THEN UPDATE`. |

### Diễn giải cơ chế — lần theo một ticket cụ thể

Bằng chứng thuyết phục nhất không nằm ở con số tổng, mà ở việc lần theo một ticket đơn lẻ.
Ticket `T000009` có hai bản ghi CDC: tạo ngày 08-11 (`op='c'`) và sửa ngày 08-12 (`op='u'`).
Trong `gold_training_set` trước khi sửa, nó có mặt ở **cả hai** partition:

```
┌────────────┬─────────┐
│    ngay    │ ban_sao │        ← sau đúng 2 lượt chạy
├────────────┼─────────┤
│ 2026-08-11 │       1 │        ← bản ghi ma, sinh ở lượt 1, không bao giờ bị dọn
│ 2026-08-12 │       2 │        ← một bản mỗi lượt chạy
└────────────┴─────────┘
```

Bản ghi ở 08-11 sinh ra vì tại lượt chạy đầu tiên, khi pipeline xử lý ngày vận hành 08-11, Bronze
mới nạp đến hôm đó nên `silver_tickets` còn thấy ticket ở trạng thái cũ với `_ingested_at = 08-11`.
Sang ngày 08-12, bản ghi `u` tới, Silver chuyển ticket sang `_ingested_at = 08-12` và mệnh đề `WHERE`
của ngày 08-12 khớp lần nữa. Từ lượt chạy thứ hai trở đi, Bronze đã đầy đủ ngay từ đầu nên Silver
không còn trả ticket đó về 08-11 — hàng cũ nằm lại đó vĩnh viễn, không bao giờ được ghi đè cũng
không bao giờ bị xoá.

Điều này giải thích hằng số 1.310 trong công thức: đúng 1.310 ticket bị rải trên hai partition,
khớp chính xác số bản ghi `op='u'` trong nguồn. Ba con số đo độc lập nhau — số hàng thừa, số ticket
đa partition, số bản ghi update — trùng khít.

Đáng lo hơn cả việc thừa hàng là **nội dung** của hàng ma: nó mang trạng thái `open` trong khi
ticket thực tế đã `resolved`. Tập huấn luyện vì thế không chỉ bị nhân bản mà còn chứa nhãn mâu
thuẫn, và mô hình phân loại học từ một hỗn hợp giữa trạng thái cũ và mới.

Sau khi sửa, `T000009` chỉ còn **một** hàng duy nhất ở partition 08-12 với trạng thái `resolved`,
và trên toàn bảng không còn ticket nào nằm ở nhiều partition.

### Vì sao `merge` chứ không phải `delete+insert` theo partition ngày?

Đây là điểm dễ chọn sai nhất của nhiệm vụ. `delete+insert` theo ngày *có vẻ* hợp lý — nó là cách
làm quen thuộc với bảng phân vùng theo thời gian, và nó xử lý đúng phần lớn ticket. Nhưng nó thất
bại đúng ở nhóm 1.310 ticket bị update, vì **đơn vị của phép xoá (một ngày) không trùng với đơn vị
của grain (một ticket)**. Xoá partition 08-12 rồi ghi lại chỉ dọn được bản sao trong ngày đó; bản
ghi ma ở 08-11 không có gì để ghi đè lên nó, vì khi backfill ngày 08-11 thì Silver không còn trả về
ticket này nữa.

`merge` theo `ticket_id` khớp bản ghi bất kể nó nằm ở partition nào, nên xử lý được cả hai nhóm.
Nguyên tắc rút ra: **chiến lược ghi phải khớp với grain của bảng, không khớp với cách dữ liệu tình
cờ được phân vùng.**

### Vì sao giữ nguyên mệnh đề `WHERE` theo `run_date`?

Xoá mệnh đề này đi thì `make verify` cũng sẽ xanh — nhưng đó là chữa triệu chứng bằng cách vứt bỏ
tính incremental của model, biến nó thành bảng quét toàn bộ lịch sử mỗi lượt chạy. Mệnh đề `WHERE`
là tối ưu hiệu năng có chủ đích, không phải khiếm khuyết logic. Yêu cầu thực sự là *vừa giữ
incremental vừa đạt idempotent*, và `merge` + `unique_key` đạt được cả hai.

### Vì sao hai tham số DAG không phải nguyên nhân?

`catchup=True` khiến Airflow tự xếp hàng chạy bù toàn bộ lịch sử kể từ `start_date` mỗi khi DAG
được bật lại, và thiếu `max_active_runs` cho phép nhiều run ghi đồng thời vào cùng một bảng — cả
hai đều làm sự cố xảy ra thường xuyên hơn và ở quy mô lớn hơn. Nhưng chúng chỉ là **điều kiện kích
hoạt**, không phải cơ chế. Bằng chứng: chạy tay `make pipeline` hai lần, không có Airflow nào tham
gia, dữ liệu vẫn nhân đôi. Ngược lại, sửa DAG mà không sửa `config()` thì `make verify` vẫn đỏ.

Phân biệt này quan trọng trong vận hành: nếu chỉ đặt `catchup=False`, đội trực sẽ tin rằng sự cố đã
được xử lý, trong khi thực chất chỉ giảm được xác suất gặp phải — và nó sẽ quay lại vào lần retry
tiếp theo.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1043: `gold_feature_daily` thiếu ~5% so với đối chiếu thủ công (8.645 / 9.100 — thiếu **455** cặp), và chỉ thiếu ở những ngày đã chạy xong từ lâu. Đối soát xác nhận: các cặp thiếu trải từ 08-03 đến 08-13, ba ngày mới nhất không thiếu cặp nào. Nghịch lý là bảng vẫn báo `ỔN ĐỊNH ✓` — checksum ba lượt giống hệt (`3269dbe574`), tức nó tái tạo cùng một sai lệch một cách hoàn toàn tất định. |
| **P99 độ trễ đo được** | **2,73 ngày** *(p50 = 0,13 · p95 = 1,81 · max = 2,94 · tỷ lệ trễ > 1 ngày = **5,05%**, trùng khớp con số "thiếu khoảng 5%" trong phiếu)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 = 2,73. Phân bố độ trễ kết thúc dứt khoát ở 3 ngày (84,09% tới đúng ngày · 10,94% trễ 1 ngày · 2,97% trễ 2 ngày · 2,00% trễ 3 ngày · không có bản ghi nào trễ hơn), nên cửa sổ 3 ngày phủ trọn phần đuôi. |
| **Nguyên nhân** | Điều kiện lọc incremental so `event_date` với `max(event_date)` của **chính bảng đích** — một đại lượng chỉ tăng đơn điệu — nên nó ngầm giả định dữ liệu luôn tới kho theo đúng thứ tự thời gian sự kiện xảy ra. Thực đo cho thấy **15,91%** bản ghi vi phạm giả định đó. Hệ quả: một cặp `(event_date, customer_id)` mà **toàn bộ** sự kiện đều tới muộn sẽ không tồn tại trong bảng đích ở ngày vận hành của nó, và khi dữ liệu tới thì `max(event_date)` đã vượt qua — mốc so sánh chỉ tiến về phía trước nên cặp đó **không bao giờ** được xử lý ở bất kỳ lượt chạy nào sau đó. Đúng 455 cặp rơi vào tình trạng này. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`, **hai thay đổi bắt buộc đi cùng nhau**: (1) đổi điều kiện thành `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`; (2) thêm `unique_key = ['event_date','customer_id']` + `incremental_strategy = 'merge'`, vì cửa sổ rộng khiến cùng một cặp được tính lại ở nhiều lượt chạy. |
| **Bằng chứng** | trước: **8.645** hàng, 455 cặp thiếu · sau: **9.100** hàng, **0** cặp thiếu, checksum 3 lượt `f8d3f591f0` giống hệt; lượt 4 và 5 vẫn 9.100 với số hàng bằng đúng số cặp phân biệt. Cặp `(08-12, C0004)` được thu hồi với đúng 11 sự kiện. Nhiệm vụ 1 không bị ảnh hưởng (12.480 · `8622572a97` ×3 không đổi). |

### Đặc trưng hoá chính xác tập dữ liệu bị mất

Đối soát `left join` từ Silver xuống Gold cho thấy khác biệt giữa cặp *mất* và cặp *còn* nằm gọn
trong một cột: nhóm mất có `min(ingested_date)` = `event_date` **+ 1**, nhóm còn có
`min(ingested_date)` = **chính** `event_date`. Từ đó rút ra phát biểu:

> Một cặp `(event_date, customer_id)` sống sót **khi và chỉ khi** có ít nhất một sự kiện của nó tới
> kho ngay trong chính ngày sự kiện xảy ra.

Kiểm chứng định lượng cho kết quả tuyệt đối: số cặp mà toàn bộ sự kiện đều tới muộn là **455**, số
cặp thực tế thiếu là **455**, và số cặp trùng khớp giữa hai tập là **455** — không phải hai tập khác
nhau tình cờ cùng kích thước. Nhóm còn lại đúng bằng **8.645**, tức toàn bộ nội dung hiện có của bảng.

Ca cụ thể: khách hàng `C0004` có 11 sự kiện ngày 08-12, toàn bộ tới kho ngày 08-14 và 08-15. Tại
ngày vận hành 08-14, `max(event_date)` trong bảng đích đã là 08-13, nên `08-12 > 08-13` sai và dữ
liệu bị bỏ qua; ngày 08-15 khoảng cách còn nới rộng thêm. Cánh cửa đóng vĩnh viễn.

### Vì sao đổi `>` thành `>=` không đủ — đo bằng thực nghiệm

| Chỉ số | `>` (gốc) | `>=` |
|---|---|---|
| Tổng hàng | 8.645 | **16.704** |
| Cặp phân biệt | 8.645 | 8.709 |
| Còn thiếu | 455 | **391** |
| Hàng trùng lặp | 0 | **7.995** |

`>=` chỉ kéo thêm đúng một ngày biên, thu hồi được **64 / 455** cặp (14%), đồng thời đẻ ra gần 8.000
hàng trùng vì ngày biên bị tính lại mà không có khoá để ghi đè. Nó vừa không giải quyết vấn đề cũ
vừa tạo ra một vấn đề mới thuộc đúng loại của nhiệm vụ 1. Kết luận: vấn đề không nằm ở **biên** của
điều kiện mà ở **bản chất của mốc so sánh** — `max(event_date)` tăng đơn điệu, nên mọi tinh chỉnh
toán tử quanh nó đều không cứu được dữ liệu đã nằm lại phía sau.

### Vì sao lookback và khoá composite không thay thế được cho nhau

Thực nghiệm nới lookback 3 ngày nhưng giữ nguyên `config()` gốc:

| | Tổng hàng | Cặp phân biệt |
|---|---|---|
| Lượt chạy 1 | **38.152** | 9.100 |
| Lượt chạy 2 | **74.552** | 9.100 |

Cột "cặp phân biệt" chứng minh **cửa sổ 3 ngày là đúng** — nó thu hồi đủ cả 9.100 cặp ngay lượt đầu.
Nhưng thiếu khoá thì mỗi cặp bị ghi lại nhiều lần: 14 ngày vận hành trong một lượt `make pipeline`
đã có cửa sổ chồng lấn nhau, nên bảng phình lên 38.152 hàng ngay lượt 1 rồi gấp đôi ở lượt 2.

**Lookback sửa tính đúng; khoá composite sửa tính ổn định.** Hai thay đổi giải quyết hai vấn đề khác
nhau, và `make verify` in tách riêng hai cột chính là để phân biệt được điều này.

### Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một quan sát **đơn lẻ**, không có tính thống kê — nó bị quyết định bởi đúng một bản ghi
> cực đoan. Chỉ cần một sự kiện lạc trễ 30 ngày là cửa sổ phải mở 30 ngày, và cái giá đó phải trả ở
> **mọi lượt chạy về sau, vĩnh viễn**, để phục vụ một bản ghi duy nhất. `max` cũng không ổn định:
> nó có thể nhảy vọt bất cứ lúc nào một sự cố mạng đơn lẻ xảy ra, khiến chi phí hệ thống bị điều
> khiển bởi nhiễu.
>
> Chi phí đo được của lookback ở lab này: mỗi ngày lùi thêm là thêm 650 cặp phải tính lại trong từng
> ngày vận hành. Trước khi sửa: 650 cặp mỗi ngày. Lookback 3 ngày: **2.600** cặp — gấp 4. Bỏ hẳn
> incremental mà quét toàn bộ: 9.100 cặp — gấp 14. Vậy lookback 3 ngày tốn gấp 4 lần hiện tại nhưng
> vẫn rẻ hơn 3,5 lần so với quét toàn bộ, và nó mua lại 455 cặp dữ liệu vốn đang mất vĩnh viễn.
>
> P99 chấp nhận bỏ sót 1% cực đoan để đổi lấy chi phí **ổn định và dự đoán được**. Ở bộ dữ liệu này
> P99 (2,73) và max (2,94) gần nhau nên cả hai đều quy về 3 ngày — kết quả trùng nhau, nhưng
> **nguyên tắc chọn thì khác nhau**, và đó mới là thứ mang sang được hệ thống khác.
>
> Đánh đổi có hai phía và cần nói rõ cả hai: cửa sổ **quá hẹp** thì dữ liệu mất **im lặng** — đúng
> như sự cố này, không cảnh báo nào bắt được. Cửa sổ **quá rộng** thì chỉ tốn thêm tài nguyên, mà
> tài nguyên là thứ đo được, có ngân sách, tối ưu dần được. Khi buộc phải sai một phía, sai về phía
> rộng an toàn hơn nhiều — nhưng "an toàn hơn" không có nghĩa là "không cần đo".

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1047: backend đổi kiểu cột `priority` từ số sang chuỗi hôm 08-10, có thông báo Slack. Pipeline không dừng, `dbt test` vẫn pass 9/9, nhưng model phân loại dự đoán kém hẳn từ hôm đó. Đo được: **6.606 / 12.480** ticket có `priority` sai — 6.488 hàng NULL cộng 118 hàng mang giá trị `-1`, `0`, `5` ngoài miền contract. `quarantine_tickets` rỗng hoàn toàn (0 / 312). |
| **Nguyên nhân** | Từ 2026-08-10 backend đổi cách biểu diễn `priority` sang nhãn chữ — một thay đổi **schema evolution** giữ nguyên ý nghĩa. Macro chuẩn hoá lại dùng `try_cast(priority_raw as integer)`, tức kiểm tra **kiểu dữ liệu** thay vì **miền giá trị nghiệp vụ**, nên sai theo **hai hướng ngược nhau cùng lúc**: nó biến toàn bộ 7.142 nhãn chữ hợp lệ thành `NULL`, đồng thời chấp nhận `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên. Sự cố diễn ra im lặng vì `contract` đang tắt và cột `priority` không có test nào ràng buộc miền giá trị — không có gì để fail, nên `dbt test` báo 9/9 pass trong khi hơn một nửa dữ liệu đã hỏng. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1 — `'1' '2' '3' '4'` (6.846 bản ghi):** đúng contract ban đầu → **giữ nguyên**. **Nhóm 2 — `'urgent' 'high' 'medium' 'low'` (7.142 bản ghi):** schema evolution, ý nghĩa không đổi chỉ đổi cách biểu diễn → **map** về 1/2/3/4 theo tài liệu API. **Nhóm 3 — `'0'`(49) `''`(43) `'P1'`(39) `'unknown'`(39) `'P2'`(38) `'5'`(37) `NULL`(35) `'-1'`(32) = **312** bản ghi:** hỏng thật → trả `NULL` làm tín hiệu rồi **quarantine**. Tổng ba nhóm = 14.300 = đúng số bản ghi CDC. |
| **Cách khắc phục** | Bốn file. **①** `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE lower(trim(...))` liệt kê tường minh cả ba nhóm — macro này được **cả hai** model dùng nên chúng không thể lệch định nghĩa nhau. **②** `dbt/models/silver/silver_tickets.sql`: tách CTE `normalized → valid_records → ranked → latest`, lọc **trước** `row_number()`. **③** `dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null`. **④** `dbt/models/silver/schema.yml`: `contract.enforced: true` + test `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng, đúng grain 1 hàng / 1 bản ghi CDC, chia theo lý do: 118 số ngoài miền · 116 chuỗi lạ · 43 rỗng · 35 NULL. `silver_tickets` giữ đủ **12.480** ticket. `priority` phân bố 1:3.134 · 2:3.029 · 3:3.115 · 4:3.202 — không còn NULL, không còn `-1/0/5`. `dbt test` **11/11 pass** (tăng từ 9). `contract` đọc từ manifest: `{'enforced': True}`. Lượt 4 và 5 giữ nguyên 12.480 / 312 / 12.480. |

### Vì sao thứ tự lọc và xếp hạng quyết định số hàng — đo bằng thực nghiệm

| | Lọc **sau** `row_number()` | Lọc **trước** `row_number()` |
|---|---|---|
| `silver_tickets` | **12.168** ✗ *(mất 312 ticket)* | **12.480** ✓ |
| `gold_training_set` | 12.480 — **vẫn "đúng"** | 12.480 ✓ |
| Ticket mồ côi trong Gold | **312** | **0** |

Dòng cuối là phát hiện đáng chú ý nhất: cách sai **không làm sai số hàng của Gold**, nên nhìn vào
`gold_training_set` thì mọi thứ vẫn bình thường. Lý do là `merge` chỉ `UPDATE`/`INSERT`, không
`DELETE` — 312 ticket đó đã được ghi vào Gold ở những ngày chúng còn hợp lệ, và khi biến mất khỏi
Silver thì hàng cũ vẫn nằm lại, trở thành **ticket mồ côi** mang `priority` lỗi thời. Đây là kiểu
lỗi tự che giấu ở đúng tầng mà người ta hay nhìn nhất; `make verify` bắt được nó chỉ nhờ có một dòng
kiểm tra riêng cho `silver_tickets`.

Minh chứng cụ thể cho cách làm đúng — ticket `T000714` có hai bản ghi CDC: `cdc_seq=0` với
`priority='1'` (hợp lệ, 08-08) và `cdc_seq=2` với `priority='-1'` (hỏng, 08-10, là bản **mới nhất**).
Sau khi sửa: Silver **giữ lại** ticket với `priority=1`, `updated_at=08-08` — lùi về trạng thái tin
cậy được; Quarantine giữ **đúng bản ghi hỏng** `cdc_seq=2` kèm lý do. Loại *bản ghi*, không loại
*ticket*.

### Nhận diện schema evolution: đứt gãy sắc nét, không phải nhiễu

| ngày | dạng số | dạng chữ |
|---|---|---|
| 08-03 → 08-09 | 100% | 0 |
| **08-10** → 08-16 | **0** | **100%** |

Trước 08-10 là 100% dạng số, từ 08-10 trở đi 100% dạng chữ, không có giai đoạn chuyển tiếp — dấu
vân tay của một thay đổi **có chủ đích** ở phía nguồn. Ngược lại, 312 bản ghi nhóm 3 rải đều suốt
14 ngày, đúng đặc trưng của lỗi ngẫu nhiên. Phân biệt được hai dạng phân bố này chính là thứ quyết
định cách xử lý: schema evolution cần **ánh xạ**, dữ liệu hỏng cần **cách ly**. Nhầm lẫn hai thứ sẽ
đẩy quarantine từ 312 lên **7.454** hàng và vứt bỏ hơn một nửa dữ liệu tốt.

### Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver.** Bronze là bản sao trung thực của nguồn, append-only — nhiệm vụ của nó là ghi
> lại những gì nguồn đã gửi, kể cả cái sai. Nếu Bronze từ chối bản ghi lỗi thì mất ba thứ cùng lúc:
> **bằng chứng** để điều tra, **khả năng xử lý lại** khi quy tắc chuẩn hoá thay đổi (nếu tháng sau
> backend công bố `P1 → 1` thì dữ liệu đã bị vứt không lấy lại được), và **khả năng đối soát** với
> nguồn. Cụ thể ở sự cố này: chính vì Bronze giữ nguyên `priority_raw` mà tôi mới dựng được bảng
> phân ba nhóm và xác định được mốc 08-10 — nếu Bronze đã lọc, cuộc điều tra không thể diễn ra.
> Silver mới là ranh giới giữa "dữ liệu thô" và "dữ liệu dùng được", nơi duy nhất mà việc từ chối
> một bản ghi có ý nghĩa.
>
> **Không dừng pipeline**, vì cân nhắc quy mô: 312 bản ghi hỏng chiếm **2,18%** tổng bản ghi CDC.
> Dừng cả DAG vì chúng nghĩa là chặn luôn 12.480 ticket sạch, hơn 130.000 event và 31.200 chunk
> hoàn toàn bình thường — biến một sự cố **chất lượng cục bộ** thành một sự cố **ngừng dịch vụ toàn
> hệ thống**, và ba hệ AI ở hạ nguồn mất dữ liệu hoàn toàn.
>
> Quarantine là mô hình **dead-letter queue**: phần sạch chảy tiếp, phần hỏng được giữ lại kèm lý do
> trong một hàng đợi đo đếm và cảnh báo được. Nó không né tránh vấn đề mà **chuyển vấn đề từ trạng
> thái ẩn sang trạng thái hiện** — trước khi sửa, 312 bản ghi này vẫn tồn tại nhưng nằm lẫn trong
> Silver dưới dạng NULL và `-1`, không ai đếm được; sau khi sửa chúng nằm trong một bảng riêng có
> số đếm và có lý do từ chối phân loại thành bốn nhóm.
>
> Điểm sâu hơn: trạng thái tệ nhất không phải pipeline dừng — pipeline dừng thì có người xử lý ngay
> trong vài phút. Tệ nhất là pipeline chạy êm trong khi âm thầm vứt đi một nửa dữ liệu tốt, đúng
> như những gì đã xảy ra suốt từ 08-10 đến khi phiếu này được mở.

---

## 4 · *(mở rộng)* Hai bài trong EXTRA.md — đã làm cả hai

### Bài A — Query dashboard chậm *(phiếu #1052)*

| | |
|---|---|
| **Triệu chứng** | Dashboard CSKH mất 38 giây để load, ba tháng trước chỉ 2 giây, không ai sửa dòng code nào. Đo được: `rows scanned` = **5.000.000** để trả về **3.500** hàng — tỷ lệ lãng phí 1.428:1; 5.000 file trên 130.683 hàng thật. |
| **Nguyên nhân** | Hai lỗi cộng hưởng. **(a) Storage layout:** `data/gold_events/` là 5.000 file Parquet tí hon (~26 hàng/file) không partition; đường dẫn `part-000NN.parquet` không mang thông tin của cột nào trong bộ lọc, nên engine buộc phải mở **toàn bộ** file rồi mới biết file nào có ích. DuckDB làm tròn chi phí đọc lên theo từng file (~1.000 hàng/file bất kể file chỉ có 26 hàng), nên 97,4% công quét là chi phí **mở file**, không phải đọc dữ liệu. **(b) Predicate không sargable:** `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call, nên engine không so được kết quả với tên thư mục partition lẫn min/max statistics của row group — mọi cơ chế lược bỏ đều tắt. Hiện tượng "không ai sửa code" là chính xác: thứ thay đổi theo thời gian là **số lượng file**. |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... TO 'data/gold_events_v2' (partition_by (event_date), order by event_date, customer_name, row_group_size 2000)` kèm `assert` không mất hàng. `queries/dashboard.sql`: trỏ dataset mới với `hive_partitioning = 1`, viết lại điều kiện thành `event_date = DATE '2026-08-09'` (cột đứng một mình một vế). Ngữ nghĩa không đổi vì `event_date` sinh từ chính `event_time::date`. |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324** (giảm **536,3×**, yêu cầu ≥ 10×) · `files` **5.000 → 14** · `result hash` `4379e4c5d9f3` **không đổi** ✓ · thời gian 1.372 ms → **6,8 ms** · dung lượng 20 MB → 3,8 MB. Con số 9.324 khớp số hàng thật của một ngày (130.683 ÷ 14 ≈ 9.334) — engine chỉ đọc đúng partition cần thiết. |

**Ba quyết định layout — chọn bằng số đo, không bằng cảm tính.** Tôi dựng thử ba phương án và đo:

| Phương án | Số file | rows scanned |
|---|---|---|
| partition theo `customer_name` | **650** | **49.000** |
| partition theo `event_date`, không sắp xếp, row group mặc định | 14 | **9.324** |
| partition theo `event_date` + sắp xếp + `row_group_size 2000` | 14 | **9.324** |

`event_date` có 14 giá trị phân biệt (~9.334 hàng/thư mục) nên file vẫn lành mạnh; `customer_name`
có 650 giá trị, mỗi thư mục chỉ ~201 hàng — **tái lập đúng small-file problem đang cần chữa**, và
quét gấp 5,3 lần. Nguyên tắc: cột partition tốt phải **vừa nằm trong bộ lọc, vừa có lực lượng thấp**.

**Ghi nhận trung thực về hai quyết định còn lại.** `ORDER BY` **không** làm đổi `rows scanned`
(9.324 ở cả hai phương án), nhưng giảm dung lượng **16%** (4.611 KB → 3.884 KB) nhờ giá trị giống
nhau nằm liền kề giúp nén tốt hơn. Với `ROW_GROUP_SIZE`, tôi đo năm giá trị (mặc định / 5000 / 2000
/ 500 / 100) và tất cả đều cho đúng **9.324** — metric `OPERATOR_ROWS_SCANNED` của DuckDB đếm số
hàng trong các **file được mở**, không trừ phần row group bị bỏ qua. Vậy toàn bộ 536× đến từ
**partition pruning ở mức file**, không một chút nào từ row-group pruning. Vẫn giữ `2000` vì đó là
layout đúng cho truy vấn hẹp hơn trên dataset lớn hơn, nhưng sẽ là không trung thực nếu ghi rằng nó
đóng góp vào con số 536×.

### Bài B — Consumer gặp sự cố giữa batch *(phiếu crash-test)*

| | |
|---|---|
| **Triệu chứng** | `make crash-test` giết consumer ở lô 7 rồi khởi động lại: chỉ còn **19.500 / 20.000** hàng — **mất 500**, đúng bằng kích thước một lô. `offset đã commit: 3.500` = 7 lô × 500. |
| **Nguyên nhân** | Thứ tự thao tác trong `consume()` là `commit()` → `write_batch()`, tức **at-most-once**: khi tiến trình bị `kill -9` giữa hai bước, offset đã dịch qua lô hiện tại nhưng dữ liệu chưa kịp ghi, nên lần khởi động lại đọc tiếp từ lô sau và 500 message của lô đang dở **mất vĩnh viễn**. Mất mát diễn ra hoàn toàn im lặng vì consumer khởi động lại thành công và không sinh log lỗi nào. |
| **Cách khắc phục** | Hai hạng mục bắt buộc đi cùng nhau. **(a)** đảo thành `write_batch()` → `maybe_crash()` → `commit()`, chuyển sang **at-least-once**. **(b)** làm phép ghi idempotent: thêm `primary key` cho `event_id` trong `DDL` (điều kiện để DuckDB chấp nhận `ON CONFLICT`), rồi đổi `INSERT` thuần thành `insert ... on conflict (event_id) do update set ...`. |
| **Bằng chứng** | trước: 19.500 hàng / 19.500 event_id, **mất 500** ✗ · sau: **20.000 hàng / 20.000 event_id**, không mất ✓ không trùng ✓, C == A ✓ → `BÀI MỞ RỘNG B: ĐẠT`. `make verify` vẫn 4/4. |

**Vì sao hai hạng mục không thay thế được cho nhau — đo bằng thực nghiệm:**

| | offset commit | số hàng | event_id phân biệt | Vấn đề |
|---|---|---|---|---|
| at-most-once (gốc) | 3.500 | **19.500** | 19.500 | **mất** 500 |
| at-least-once (chỉ đảo thứ tự) | 3.000 | **20.500** | **20.000** | **trùng** 500 |
| at-least-once + idempotent | 3.000 | **20.000** | **20.000** | — ✓ |

Dòng giữa là bước tiến thật chứ không phải đổi lỗi này lấy lỗi khác: cột `event_id phân biệt` đã
đúng **20.000**, tức không mất dữ liệu nào, chỉ 500 bản ghi bị ghi hai lần. Vấn đề chuyển từ *mất
mát* — không sửa được sau khi xảy ra — sang *trùng lặp* — sửa được bằng một dòng `on conflict`.

Chi tiết xác nhận tính idempotent: ở kịch bản C, consumer ghi **17.000** message để tạo ra **20.000**
hàng — nhiều hơn 500 so với số hàng còn thiếu. Đúng 500 message của lô 7 được phát lại và **ghi đè
lên chính chúng**, thay vì tạo hàng mới.

**`DO UPDATE` khác `DO NOTHING` ở đâu khi message được replay với nội dung đã đổi?**

> Cả hai đều cho đúng **số hàng** và đều làm bài này pass. Khác biệt nằm ở **nội dung**:
> `DO NOTHING` giữ nguyên bản cũ, nên nếu message phát lại mang giá trị đã cập nhật thì kho đọng lại
> phiên bản lỗi thời và **âm thầm lệch khỏi nguồn** — đúng loại lỗi im lặng mà cả ba nhiệm vụ chính
> đều mắc. `DO UPDATE` đưa kho **hội tụ** về trạng thái mới nhất.
>
> Tôi chọn **`DO UPDATE`**, vì mục tiêu của idempotency ở đây không chỉ là *không nhân bản* mà là
> *hội tụ đúng trạng thái*; `DO NOTHING` biến lần ghi **đầu tiên** thành lần thắng cuộc — một quy tắc
> không có cơ sở nghiệp vụ nào. `DO NOTHING` chỉ phù hợp khi bản ghi **bất biến theo thiết kế**
> (log append-only), và khi đó nó rẻ hơn vì không phải ghi lại — nhưng phải chọn có ý thức, không
> chọn vì nó ngắn hơn.

**Vì sao exactly-once không tồn tại ở tầng giao vận:** `commit` và `write` nằm trên hai hệ thống khác
nhau (file offset và kho dữ liệu) và không có giao dịch phân tán nào bao trọn cả hai; tiến trình luôn
có thể chết ở khe giữa chúng. Lời giải là chuyển vấn đề từ tầng giao vận — nơi không giải được — sang
tầng lưu trữ, nơi giải được bằng một ràng buộc khoá.

---

## 5 · Bảng tự chấm nhanh *(theo `RUBRIC.md`)*

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | `8dd7c98653` ×3 | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | `f8d3f591f0` ×3 | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 11/11 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,73 ngày** | (ghi số) | ✓ |
| **Tổng verify** | **4/4** | 4/4 tiêu chí | ✓ |
| *(thưởng A)* rows scanned | 5.000.000 → 9.324 (**536,3×**) | giảm ≥ 10× | ✓ |
| *(thưởng A)* result hash | `4379e4c5d9f3` → `4379e4c5d9f3` | không đổi | ✓ |
| *(thưởng B)* `make crash-test` | 20.000 / 20.000, không mất không trùng | ĐẠT | ✓ |

**Đối chiếu các mục trừ điểm của `RUBRIC.md`:**

| Vi phạm | Trạng thái |
|---|---|
| Sửa `expected/`, `tools/verify.py`, `tools/explain.py`, `tools/common.py`, `seed/generate.py` | **Không** — `git diff` xác nhận cả năm nguyên vẹn |
| Xoá bớt dữ liệu nguồn cho số hàng khớp | **Không** — `rows on disk` giữ nguyên 130.683; `compact.py` có `assert src_rows == dst_rows` |
| Nộp kèm `.venv/`, `warehouse.duckdb`, `data/` | **Không** — cả ba đã nằm trong `.gitignore`, và chạy `make clean` trước khi nộp |
| `make verify` không chạy được trên repo nộp | **Không** — verify chạy trọn từ trạng thái sạch, 4/4 |
| `quarantine_tickets` vượt 1.000 hàng | **Không** — đúng 312 |

Mười file đã sửa đều nằm trong vùng được phép (`dbt/`, `ingest/`, `queries/`, `tools/compact.py`,
`dags/`):

```
dags/ai_training_pipeline.py              dbt/models/silver/quarantine_tickets.sql
dbt/macros/normalize_priority.sql         dbt/models/silver/schema.yml
dbt/models/gold/gold_feature_daily.sql    dbt/models/silver/silver_tickets.sql
dbt/models/gold/gold_training_set.sql     ingest/consumer.py
queries/dashboard.sql                     tools/compact.py
```

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lần trên cùng một khoảng thời gian rồi so số hàng — một bảng idempotent thì con số phải bất động. Sau đó rà mọi model `incremental` xem đã khai `unique_key` chưa, và khoá đó có khớp **grain** đã ghi trong chú thích đầu file không. |
| 2 | Với mỗi model `incremental`, xem mốc lọc lấy từ **event-time** hay **ingestion-time**. Nếu là event-time mà không có lookback, đo ngay phân bố `_ingested_at − event_time` và đối chiếu P99 với độ rộng cửa sổ. Kèm theo: một phép đối soát `left join` định kỳ từ nguồn xuống đích để đếm bản ghi bị rơi — tính ổn định không bao giờ phát hiện được lớp lỗi này. |
| 3 | `contract` có được bật không, và các cột quan trọng đã có test **miền giá trị** chưa — contract chỉ ràng buộc kiểu, `priority = 99` vẫn đi qua vì nó đúng là integer. Kèm theo: đếm NULL theo từng cột **và theo thời gian**; một cột đột ngột nhiều NULL từ một mốc ngày cụ thể gần như luôn là schema evolution ở nguồn, không phải lỗi ngẫu nhiên. |

**Điểm chung của cả ba sự cố.** Không sự cố nào **sinh ra lỗi**: pipeline chạy xong, không có job
đỏ, và `dbt test` ban đầu pass 9/9 ở cả ba trường hợp. Thứ nguy hiểm trong data pipeline không phải
job đỏ — job đỏ có người xử lý ngay trong vài phút — mà là **job xanh đang âm thầm nhân bản, bỏ sót
hoặc vứt bỏ dữ liệu**. Hệ quả rút ra cho cách kiểm thử: phải nhắm vào **bất biến** — chạy lại cho
cùng kết quả, số hàng đối soát được với nguồn, miền giá trị đúng contract — chứ không chỉ nhắm vào
việc pipeline có chạy xong hay không. Và một bộ test chỉ bảo vệ được đúng những bất biến mà nó được
viết ra để bảo vệ; sự im lặng của nó không phải bằng chứng dữ liệu sạch.
