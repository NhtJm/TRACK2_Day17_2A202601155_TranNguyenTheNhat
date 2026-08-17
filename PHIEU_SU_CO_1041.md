# Phiếu sự cố #1041 — Kích thước bảng training tăng sau mỗi lần chạy

**Nhiệm vụ 1** · Idempotency · Bảng: `gold_training_set`
**Trạng thái:** ☑ đang điều tra · ☑ đã sửa · ☑ đã kiểm chứng
**Người xử lý:** Trần Nguyễn Thế Nhật

---

## 1 · Triệu chứng (báo từ vận hành)

> "Đêm qua job lỗi mạng, mình vào Airflow bấm Clear Task cho chạy lại. Sáng nay
> `gold_training_set` nhiều hơn hẳn. Chạy lại lần nữa lại nhiều thêm.
> Không thấy báo lỗi gì cả."

Xác nhận từ `make verify` baseline:

| Bảng | Ổn định | Số hàng | Kỳ vọng | Chênh |
|---|---|---|---|---|
| `gold_training_set` | ✗ FAIL | 38.750 | 12.480 | thừa 26.270 |

Checksum ba lượt baseline: `7c461563f4` / `d11657ff21` / `2b76a4f850` → **khác nhau cả ba**.

Điều đáng chú ý ngay từ đầu: pipeline không hề báo lỗi, `dbt test` vẫn pass 9/9. Bảng cứ
lớn dần một cách im lặng. Đây là dấu hiệu điển hình của lỗi *ghi* chứ không phải lỗi *dữ liệu* —
nếu dữ liệu nguồn hỏng thì test đã bắt được.

---

## 2 · Điều tra

### 2.1 Xác nhận hiện tượng — chạy lại có tăng không

Kho phải được xoá trước, nếu không số đo sẽ cộng dồn lên những lượt chạy cũ và không diễn giải được:

```bash
make reset
make pipeline && q "select count(*) as tong, count(distinct ticket_id) as so_ticket from gold_training_set"
make pipeline && q "select count(*) as tong, count(distinct ticket_id) as so_ticket from gold_training_set"
```

**Output:**

```
  kho đã xoá.
=== LƯỢT 1 ===
  ngày 2026-08-16  (14/14)  cdc=1,099  events=  9,583  transcripts=  373
  pipeline xong sau 15.3s
┌───────┬───────────┐
│ tong  │ so_ticket │
│ int64 │   int64   │
├───────┼───────────┤
│ 13790 │     12480 │
└───────┴───────────┘

=== LƯỢT 2 ===
  ngày 2026-08-16  (14/14)  cdc=1,099  events=  9,583  transcripts=  373
  pipeline xong sau 17.8s
┌───────┬───────────┐
│ tong  │ so_ticket │
│ int64 │   int64   │
├───────┼───────────┤
│ 26270 │     12480 │
└───────┴───────────┘
```

| Lượt chạy | Tổng hàng | Ticket phân biệt | Chênh so với lượt trước |
|---|---|---|---|
| 1 | 13.790 | 12.480 | — |
| 2 | 26.270 | 12.480 | **+12.480** |

→ **Nhận xét.** Bảng tăng *đều*, không phải tăng dần: mỗi lượt chạy cộng thêm đúng **12.480**
hàng — bằng đúng số ticket phân biệt, và bằng đúng `expected/gold_training_set.count`. Nói cách
khác, mỗi lượt chạy đang ghi lại **toàn bộ** tập ticket một lần nữa thay vì cập nhật cái đã có.

Đồng thời cột `so_ticket` **đứng yên** ở 12.480 qua cả hai lượt. Đây là chi tiết quan trọng: tập
ticket không hề nở ra, chỉ có số *hàng* nở ra.

Từ hai quan sát này rút ra được công thức của số hàng sau `N` lượt chạy:

```
số hàng = 12.480 × N + 1.310
```

Kiểm chứng ngược lại với baseline: `make verify` chạy pipeline **3 lượt**, cho
`12.480 × 3 + 1.310 = 38.750` — trùng khít con số verify báo. Công thức đúng, và phần dư
**1.310** là một hằng số chỉ xuất hiện đúng một lần ở lượt đầu tiên; mục 2.4 sẽ chỉ ra nó là gì.

### 2.2 Bảng đang lặp, hay đang có ticket lạ?

```bash
q "select count(*) as total, count(distinct ticket_id) as tickets from gold_training_set"
```

**Output:**

```
┌───────┬─────────┐
│ total │ tickets │
│ int64 │  int64  │
├───────┼─────────┤
│ 26270 │   12480 │
└───────┴─────────┘
```

| total | tickets | total / tickets |
|---|---|---|
| 26.270 | 12.480 | ≈ 2,1 |

→ **Kết luận.** Bảng đang **lặp ticket cũ**, không hề có ticket lạ. Số ticket phân biệt khớp
chính xác con số kỳ vọng 12.480 — nghĩa là dữ liệu đầu vào hoàn toàn bình thường và Silver đang
làm đúng việc của nó. Lỗi nằm ở **cách model được materialize**, tức ở phía ghi, không nằm ở
dữ liệu.

Đây là bước khoanh vùng quan trọng: nếu source giữ đúng 1 hàng / 1 ticket mà target thì không,
ta không cần điều tra tiếp phía nguồn nữa.

### 2.3 Nguồn CDC có những loại thao tác nào

```bash
q "select op, count(*) from bronze_tickets_cdc group by 1"
```

**Output:**

```
┌─────────┬──────────────┐
│   op    │ count_star() │
│ varchar │    int64     │
├─────────┼──────────────┤
│ c       │        12735 │
│ d       │          255 │
│ u       │         1310 │
└─────────┴──────────────┘
```

| op | ý nghĩa | count |
|---|---|---|
| `c` | create — ticket được tạo mới | 12.735 |
| `u` | **update** — ticket bị sửa sau khi đã tạo | **1.310** |
| `d` | delete — ticket bị xoá, `silver_tickets` loại ra | 255 |

Tổng bản ghi CDC: **14.300** · Ticket còn lại sau khi trừ delete: 12.735 − 255 = **12.480** ✓

→ **Loại `op='u'`** là thủ phạm. Một ticket được tạo ngày D1 rồi sửa ngày D2 sẽ đi qua mệnh đề
`WHERE` theo `run_date` **hai lần trong cùng một lệnh `make pipeline`**, ở hai `run_date` khác nhau.
*(Một lệnh `make pipeline` chạy lần lượt cả 14 ngày vận hành — xem `tools/run_pipeline.py:71`.)*

Và con số 1.310 này chính là phần dư trong công thức ở mục 2.1. Mục 2.5 sẽ chứng minh mối liên hệ đó.

### 2.4 Ticket nào bị lặp, lặp mấy lần

```bash
q "select ticket_id, count(*) c from gold_training_set group by 1 having c > 1 limit 5"
```

**Output:**

```
┌───────────┬───────┐
│ ticket_id │   c   │
│  varchar  │ int64 │
├───────────┼───────┤
│ T003124   │     6 │
│ T009986   │     6 │
│ T010453   │     6 │
│ T012340   │     6 │
│ T003637   │     6 │
└───────────┴───────┘
```

*(Kết quả này chụp ở thời điểm kho đã qua 6 lượt chạy — mỗi lượt để lại một bản sao.)*

Số bản sao bằng đúng số lượt chạy đã thực hiện. Nhưng con số đó chưa cho biết các bản sao
**nằm ở đâu**, mà chính vị trí mới là thứ quyết định cách sửa.

### 2.5 ⭐ Các bản sao nằm ở đâu — query quyết định của nhiệm vụ này

So sánh hai ticket có số phận khác nhau: một ticket chưa từng bị sửa, và một ticket có `op='u'`.

```bash
q "select _ingested_at::date as ngay, count(*) as ban_sao
   from gold_training_set where ticket_id = 'T003124' group by 1 order by 1"

q "select _ingested_at::date as ngay, count(*) as ban_sao
   from gold_training_set where ticket_id = 'T000009' group by 1 order by 1"

q "select count(*) as so_ticket from (select ticket_id from gold_training_set
   group by 1 having count(distinct _ingested_at::date) > 1)"
```

**Output:** *(trên kho sạch, sau đúng 2 lượt chạy)*

```
--- Ticket KHÔNG bị update (T003124) ---
┌────────────┬─────────┐
│    ngay    │ ban_sao │
├────────────┼─────────┤
│ 2026-08-03 │       2 │
└────────────┴─────────┘

--- Ticket CÓ bị update (T000009) ---
┌────────────┬─────────┐
│    ngay    │ ban_sao │
├────────────┼─────────┤
│ 2026-08-11 │       1 │
│ 2026-08-12 │       2 │
└────────────┴─────────┘

--- Số ticket bị rải trên nhiều partition ---
┌───────────┐
│ so_ticket │
├───────────┤
│      1310 │
└───────────┘
```

Lần theo lịch sử CDC của `T000009` để hiểu vì sao nó nằm ở hai ngày:

```bash
q "select ticket_id, cdc_seq, op, event_time, _ingested_at
   from bronze_tickets_cdc where ticket_id='T000009' order by cdc_seq"

q "select ticket_id, status, updated_at, _ingested_at from silver_tickets where ticket_id='T000009'"
```

**Output:**

```
--- lịch sử CDC ở Bronze ---
┌───────────┬─────────┬─────────┬─────────────────────┬─────────────────────┐
│ ticket_id │ cdc_seq │   op    │     event_time      │    _ingested_at     │
├───────────┼─────────┼─────────┼─────────────────────┼─────────────────────┤
│ T000009   │       0 │ c       │ 2026-08-11 10:23:56 │ 2026-08-11 10:23:56 │
│ T000009   │       2 │ u       │ 2026-08-12 16:12:19 │ 2026-08-12 16:12:19 │
└───────────┴─────────┴─────────┴─────────────────────┴─────────────────────┘

--- Silver chỉ giữ bản mới nhất ---
┌───────────┬──────────┬─────────────────────┬─────────────────────┐
│ ticket_id │  status  │     updated_at      │    _ingested_at     │
├───────────┼──────────┼─────────────────────┼─────────────────────┤
│ T000009   │ resolved │ 2026-08-12 16:12:19 │ 2026-08-12 16:12:19 │
└───────────┴──────────┴─────────────────────┴─────────────────────┘
```

→ **Các bản sao nằm ở NHIỀU NGÀY KHÁC NHAU** — với những ticket bị update.

Diễn giải đầy đủ cơ chế của `T000009`:

1. **Lượt chạy 1, tới ngày vận hành 08-11.** Bronze lúc này mới nạp đến 08-11, nên `silver_tickets`
   (dựng lại toàn bộ mỗi lần) thấy ticket ở trạng thái `c` với `_ingested_at = 08-11`. Mệnh đề
   `WHERE _ingested_at ∈ [08-11, 08-12)` khớp → ghi một hàng vào partition **08-11**.
2. **Lượt chạy 1, tới ngày vận hành 08-12.** Bronze đã nạp thêm bản ghi `u`. Silver bây giờ thấy
   ticket ở trạng thái mới nhất, `_ingested_at = 08-12`. Mệnh đề `WHERE` cho ngày 08-12 khớp
   → ghi thêm một hàng nữa, lần này vào partition **08-12**.
3. **Từ lượt chạy 2 trở đi.** Bronze đã có đủ dữ liệu ngay từ đầu, nên khi xử lý ngày 08-11,
   Silver đã thấy ticket ở 08-12 rồi — không còn khớp `WHERE` của ngày 08-11 nữa. Hàng cũ nằm ở
   partition 08-11 **không bao giờ được ghi đè, cũng không bao giờ bị xoá**. Nó trở thành một
   *bản ghi ma*: mang trạng thái `open` đã lỗi thời, trong khi thực tế ticket đã `resolved`.

Đây chính là nguồn gốc của hằng số **1.310** trong công thức ở mục 2.1: đúng 1.310 ticket bị rải
trên hai partition, khớp chính xác 1.310 bản ghi `op='u'` ở mục 2.3. Ba con số độc lập trùng khớp —
giả thuyết được xác nhận.

→ **Nếu ta xoá partition của một ngày rồi ghi lại, có dọn hết được không?** **Không.** Đây là câu
trả lời quyết định việc chọn chiến lược. Xoá partition 08-12 rồi ghi lại chỉ dọn được bản sao nằm
trong ngày đó; bản ghi ma ở partition 08-11 vẫn nguyên vẹn, vì khi backfill ngày 08-11 thì Silver
không còn trả về ticket này nữa nên không có gì để ghi đè lên nó. Muốn dọn sạch, phép ghi phải
khớp theo **khoá của entity**, không phải theo **ngày**.

### 2.6 dbt đang sinh câu lệnh SQL gì

```bash
head -10 dbt/target/run/lab17/models/gold/gold_training_set.sql
```

**Output:**

```sql
    insert into "warehouse"."main"."gold_training_set" ("ticket_id", "customer_id", ... )
    (
        select "ticket_id", "customer_id", ...
        from "gold_training_set__dbt_tmp20260817214359392224"
    )
```

→ **`INSERT INTO` thuần.** Không có `MERGE`, không có `DELETE`, không có bất kỳ điều kiện khớp nào.

Vì sao dbt sinh ra câu đó: khối `config()` khai `materialized = 'incremental'` nhưng **không khai
`unique_key`**. Không có khoá thì dbt không có cách nào biết hàng nào ở nguồn tương ứng với hàng nào
ở đích, nên nó lùi về hành vi an toàn nhất mà nó biết — nối thêm dữ liệu vào cuối bảng. Mọi lượt
chạy lại vì thế là một lượt *ghi thêm*, không phải *ghi đè*.

### 2.7 Hai tham số của DAG

Mở `dags/ai_training_pipeline.py`, khối `TODO` trong `with DAG(...)`:

| Tham số | Giá trị hiện tại | Rủi ro nó tạo ra |
|---|---|---|
| `catchup` | `True` | `start_date` đặt ở 2026-08-03. Mỗi lần DAG được bật lại hoặc bị tạm dừng rồi chạy tiếp, Airflow tự xếp hàng chạy bù **mọi ngày trong quá khứ**. Mỗi run bù là thêm một lượt ghi vào bảng đích — tức nhân bản hàng loạt chỉ bằng một thao tác quản trị. |
| `max_active_runs` | *(chưa khai — mặc định không giới hạn)* | Nhiều run được phép ghi **đồng thời** vào cùng một bảng. Thao tác Clear Task rất dễ tạo ra tình huống này: run cũ chưa dứt, run mới đã bắt đầu. |

> ⚠️ Hai tham số này chỉ **giảm tần suất kích hoạt** lỗi — chúng **không phải** root cause.
> Root cause nằm ở `config()` của model. Sửa DAG mà không sửa model thì `make verify` vẫn đỏ,
> vì bản thân `make pipeline` chạy tay cũng đủ nhân bản dữ liệu mà không cần Airflow nào cả.
> Khi viết mục 6 bên dưới, đừng nêu `catchup=False` làm nguyên nhân.

---

## 3 · Phân tích — năm câu hỏi

**1. Grain của bảng là entity hay sự kiện? Khoá tự nhiên là gì?**

> Grain là **entity**: chú thích đầu file model ghi rõ *"Grain: 1 hàng / 1 ticket"*, và dữ liệu
> xác nhận điều đó — 12.480 ticket phân biệt đúng bằng con số kỳ vọng. Khoá tự nhiên là `ticket_id`.
>
> Phân biệt này quan trọng vì nó quyết định *cái gì là "cùng một dòng"*. Với bảng sự kiện, hai bản
> ghi cùng khoá nghiệp vụ nhưng khác thời điểm là hai dòng hợp lệ. Với bảng entity, chúng là hai
> phiên bản của **một** dòng, và chỉ phiên bản mới nhất được tồn tại.

**2. Không có `unique_key` thì dbt sinh câu lệnh nào? Chạy lại thì hàng cũ bị thay thế hay ghi thêm?**

> Sinh `INSERT INTO` — đã kiểm chứng trực tiếp ở mục 2.6 bằng file SQL dbt gửi xuống DuckDB.
> Chạy lại cùng một ngày lần thứ hai, hàng cũ bị **ghi thêm**, không phải thay thế.
>
> Điều này hợp lý từ góc nhìn của dbt: không được cho biết khoá, nó không có cơ sở nào để nói hai
> hàng là "cùng một dòng", nên không thể sinh `MERGE`. `INSERT` là lựa chọn duy nhất còn lại.

**3. Một ticket tạo ngày D1, sửa ngày D2 đi qua mệnh đề `WHERE` mấy lần?**

> **Hai lần trong một lượt `make pipeline`** — vì một lệnh đó chạy tuần tự cả 14 ngày vận hành.
> Lần thứ nhất ở `run_date = D1` khi Silver còn thấy ticket với `_ingested_at = D1`; lần thứ hai ở
> `run_date = D2` sau khi bản ghi `u` đã tới và Silver chuyển ticket sang `_ingested_at = D2`.
>
> Từ lượt chạy thứ hai trở đi thì chỉ còn **một lần** (ở D2), vì Bronze đã đầy đủ ngay từ đầu nên
> Silver không bao giờ trả ticket đó về D1 nữa. Đó là lý do 1.310 hàng ma chỉ sinh ra đúng một lần
> ở lượt đầu, còn các lượt sau tăng đều 12.480 — khớp với công thức `12.480 × N + 1.310`.

**4. `append` / `delete+insert` theo partition ngày / `merge` theo khoá — cái nào đúng? Vì sao hai cái kia không đủ?**

> **`merge` theo `ticket_id`.**
>
> `append` bị loại ngay: nó chính là hành vi hiện tại — nối thêm vô điều kiện, tức đúng nguyên nhân
> của sự cố.
>
> `delete+insert` theo partition ngày **thoạt nhìn có vẻ hợp lý** và là cái bẫy chính của nhiệm vụ
> này: nó xử lý được ticket bình thường, nhưng thất bại đúng ở nhóm 1.310 ticket bị update. Mục 2.5
> đã chứng minh: các bản sao của một ticket nằm ở **nhiều partition khác nhau**, nên xoá partition
> D2 rồi ghi lại vẫn để nguyên bản ghi ma ở D1 — và bản ghi ma đó mang trạng thái sai lệch
> (`open` trong khi ticket đã `resolved`), tức nó không chỉ thừa mà còn **đầu độc dữ liệu huấn
> luyện**. Nói ngắn gọn: đơn vị của phép xoá (ngày) không trùng với đơn vị của grain (ticket).
>
> `merge` theo khoá khớp bản ghi bất kể nó nằm ở partition nào, nên xử lý được cả hai nhóm.
> Nguyên tắc chung: **chiến lược ghi phải khớp với grain của bảng**, không khớp với cách dữ liệu
> tình cờ được phân vùng.

**5. Vì sao mệnh đề `WHERE _ingested_at >= run_date` không phải là lỗi, và không được xoá nó?**

> Vì nó phục vụ một mục đích chính đáng: giới hạn khối lượng quét khi backfill một ngày, để không
> phải đọc lại toàn bộ lịch sử mỗi lượt chạy. Đó là tối ưu hiệu năng, không phải khiếm khuyết logic.
>
> Xoá nó đi thì `make verify` cũng sẽ xanh — nhưng đó là chữa triệu chứng bằng cách vứt bỏ tính
> incremental của model, biến nó thành một bảng quét toàn bộ. Bài toán đặt ra là *vừa giữ được
> incremental vừa idempotent*, và `merge` + `unique_key` đạt được cả hai. Trong báo cáo, việc phân
> biệt được đâu là lỗi và đâu là thiết kế có chủ đích chính là thứ chứng minh mình hiểu hệ thống.

---

## 4 · Khắc phục

| File | Thay đổi |
|---|---|
| `dbt/models/gold/gold_training_set.sql` | Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. Giữ nguyên `materialized`, `on_schema_change` và mệnh đề `WHERE` theo `run_date`. |
| `dags/ai_training_pipeline.py` | `catchup=True` → `catchup=False`; bỏ comment và đặt `max_active_runs=1`. |

Diff thực tế:

```diff
--- a/dbt/models/gold/gold_training_set.sql
+++ b/dbt/models/gold/gold_training_set.sql
 {{ config(
-    materialized     = 'incremental',
-    on_schema_change = 'fail'
+    materialized         = 'incremental',
+    unique_key           = 'ticket_id',
+    incremental_strategy = 'merge',
+    on_schema_change     = 'fail'
 ) }}

--- a/dags/ai_training_pipeline.py
+++ b/dags/ai_training_pipeline.py
-    catchup=True,
-    # max_active_runs=?
+    catchup=False,
+    max_active_runs=1,
```

**Xác nhận fix có tác dụng** — đọc lại SQL mà dbt sinh ra sau khi sửa:

```bash
head -20 dbt/target/run/lab17/models/gold/gold_training_set.sql
```

**Output:**

```sql
    MERGE INTO "warehouse"."main"."gold_training_set" AS DBT_INTERNAL_DEST
        USING "gold_training_set__dbt_tmp20260817223050428581" AS DBT_INTERNAL_SOURCE
        ON (DBT_INTERNAL_SOURCE.ticket_id = DBT_INTERNAL_DEST.ticket_id)
    WHEN MATCHED
    THEN
        UPDATE BY NAME
    WHEN NOT MATCHED
    THEN
        INSERT BY NAME
```

`INSERT INTO` đã trở thành `MERGE INTO ... ON ticket_id ... WHEN MATCHED THEN UPDATE`. Đây là
bằng chứng trực tiếp nhất rằng nguyên nhân đã được xử lý đúng chỗ, chứ không phải triệu chứng
tình cờ biến mất.

---

## 5 · Kiểm chứng

```bash
make clean && make verify
```

**Output:**

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8622572a97    8622572a97    8622572a97   ✓
  gold_feature_daily    3269dbe574    3269dbe574    3269dbe574   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✗  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✗  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  2/4 tiêu chí đạt
```

| Chỉ số | Trước | Sau |
|---|---|---|
| Số hàng (sau 3 lượt) | 38.750 | **12.480** ✓ |
| Số ticket phân biệt | 12.480 | 12.480 |
| Checksum lượt 1 | `7c461563f4` | `8622572a97` |
| Checksum lượt 2 | `d11657ff21` | `8622572a97` |
| Checksum lượt 3 | `2b76a4f850` | `8622572a97` |
| Ổn định | ✗ FAIL | **✓ ok** |
| 1 hàng / 1 ticket | ✗ 12.480 ticket bị lặp | **✓ không lặp** |
| DAG catchup / max_active_runs | `True` / `None` | **`False` / `1`** |
| Tổng kết verify | 1/4 | **2/4** |

*(Hai dòng ✗ còn lại thuộc phiếu #1043 và #1047 — lỗi độc lập, chưa xử lý ở phiếu này.
`gold_doc_chunks` giữ nguyên checksum `92d8e50131`, xác nhận nhóm đối chứng không bị ảnh hưởng.)*

### Kiểm chứng sâu hơn mức verify yêu cầu

`make verify` chỉ chạy ba lượt. Chạy thêm lượt 4 và 5 để loại trừ khả năng bảng chỉ tình cờ ổn định
ở đúng ba lượt:

```bash
make pipeline && q "select count(*) as tong, count(distinct ticket_id) as so_ticket from gold_training_set"
make pipeline && q "select count(*) as tong, count(distinct ticket_id) as so_ticket from gold_training_set"
```

**Output:**

```
=== LƯỢT 4 ===
┌───────┬───────────┐
│ tong  │ so_ticket │
├───────┼───────────┤
│ 12480 │     12480 │
└───────┴───────────┘
=== LƯỢT 5 ===
┌───────┬───────────┐
│ tong  │ so_ticket │
├───────┼───────────┤
│ 12480 │     12480 │
└───────┴───────────┘
```

- Lượt 4: **12.480** hàng · Lượt 5: **12.480** hàng · Không đổi: ☑

Và quan trọng nhất — kiểm tra chính nhóm 1.310 ticket từng bị rải trên nhiều partition:

```bash
q "select _ingested_at::date as ngay, count(*) as ban_sao
   from gold_training_set where ticket_id='T000009' group by 1 order by 1"

q "select count(*) as ticket_nhieu_partition from (select ticket_id from gold_training_set
   group by 1 having count(distinct _ingested_at::date) > 1)"
```

**Output:**

```
┌────────────┬─────────┐
│    ngay    │ ban_sao │
├────────────┼─────────┤
│ 2026-08-12 │       1 │
└────────────┴─────────┘

┌────────────────────────┐
│ ticket_nhieu_partition │
├────────────────────────┤
│                      0 │
└────────────────────────┘
```

`T000009` giờ chỉ còn **một** hàng duy nhất, nằm ở partition 08-12 — đúng trạng thái mới nhất
(`resolved`). Bản ghi ma ở 08-11 đã biến mất. Và trên toàn bảng, số ticket bị rải trên nhiều
partition từ **1.310 xuống 0**.

Đây là bằng chứng mạnh hơn cả con số 12.480: nó cho thấy `merge` không chỉ làm bảng đúng *kích
thước*, mà còn đúng *nội dung* — mô hình phân loại giờ đọc được trạng thái thật của ticket thay vì
một hỗn hợp giữa trạng thái cũ và mới.

---

## 6 · Nguyên nhân — câu viết cho báo cáo

> Model `gold_training_set` khai `materialized='incremental'` nhưng **không khai `unique_key`**,
> nên dbt không có khoá để so khớp và sinh ra câu `INSERT INTO` thay vì `MERGE`; mọi lượt chạy lại
> trên cùng một partition vì thế là ghi **thêm** dòng chứ không phải ghi **đè**. Nguồn CDC lại chứa
> 1.310 bản ghi `op='u'`, khiến một ticket được tạo ngày D1 rồi sửa ngày D2 lọt qua mệnh đề `WHERE`
> theo `run_date` ở hai partition khác nhau — nên ngay cả cách khắc phục quen thuộc "xoá partition
> ngày rồi ghi lại" cũng không dọn được bản sao nằm ở partition cũ. Kết quả là bảng tăng đúng 12.480
> hàng mỗi lượt chạy, cộng 1.310 hàng ma mang trạng thái đã lỗi thời, mà không sinh bất kỳ lỗi nào
> để cảnh báo.

---

## 7 · Phòng ngừa tái diễn

Phép kiểm tra rẻ nhất lẽ ra nên chạy từ đầu để phát hiện sự cố này:

> **Chạy pipeline hai lần trên cùng một khoảng thời gian rồi so số hàng.** Một bảng idempotent thì
> con số phải bất động; bất kỳ chênh lệch nào cũng là dấu hiệu phép ghi không idempotent. Phép thử
> này mất chưa tới một phút, không cần biết gì về nghiệp vụ, và bắt được cả ba biến thể của lỗi
> (thiếu khoá, sai chiến lược, sai grain).
>
> Ở mức hệ thống, nên biến nó thành một bất biến được kiểm tra tự động thay vì một thói quen thủ
> công: mỗi model `incremental` phải có `unique_key` khớp với grain đã khai báo, và CI chạy pipeline
> hai lượt rồi so checksum. Lý do là lớp lỗi này **không bao giờ tự báo** — job vẫn xanh, `dbt test`
> vẫn pass, chỉ có số hàng lặng lẽ trôi. Thứ nguy hiểm trong data pipeline không phải job đỏ (đã có
> người xử lý ngay) mà là job xanh đang âm thầm nhân bản dữ liệu.
