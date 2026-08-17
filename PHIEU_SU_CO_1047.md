# Phiếu sự cố #1047 — Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

**Nhiệm vụ 3** · Schema evolution & quarantine · Bảng: `silver_tickets`, `quarantine_tickets`
**Trạng thái:** ☑ đang điều tra · ☑ đã sửa · ☑ đã kiểm chứng
**Người xử lý:** Trần Nguyễn Thế Nhật

---

## 1 · Triệu chứng (báo từ vận hành)

> "Team backend đổi kiểu cột `priority` từ số sang chuỗi hôm 08-10, có thông báo trên Slack.
> Pipeline không hề dừng. Nhưng model phân loại từ hôm đó dự đoán kém hẳn."

Xác nhận từ `make verify` baseline:

| Kiểm tra | Kết quả |
|---|---|
| `silver_tickets.priority ∈ 1..4`, không NULL | ✗ **6.606** hàng sai |
| `quarantine_tickets` đúng số bản ghi lỗi | ✗ **0 / 312** |
| `dbt test` | ✓ **9/9 pass** — vẫn xanh dù hơn một nửa dữ liệu hỏng |

Điểm đáng chú ý: đây là sự cố duy nhất trong ba phiếu mà **nguyên nhân đã được thông báo trước**
trên Slack. Vấn đề không nằm ở chỗ không ai biết, mà ở chỗ hệ thống không có cơ chế nào biến thông
báo đó thành một hàng rào kỹ thuật. `dbt test` vẫn pass vì cột `priority` chưa hề có test nào ràng
buộc miền giá trị, và `contract` đang tắt.

---

## 2 · Điều tra

### 2.1 Nguồn đang gửi gì

```bash
q "select priority_raw, count(*) n from bronze_tickets_cdc group by 1 order by 2 desc"
```

**Output:**

```
┌──────────────┬───────┐
│ priority_raw │   n   │
├──────────────┼───────┤
│ low          │  1845 │
│ urgent       │  1819 │
│ medium       │  1783 │
│ 4            │  1748 │
│ 3            │  1710 │
│ 1            │  1705 │
│ high         │  1695 │
│ 2            │  1683 │
│ 0            │    49 │
│              │    43 │
│ P1           │    39 │
│ unknown      │    39 │
│ P2           │    38 │
│ 5            │    37 │
│ NULL         │    35 │
│ -1           │    32 │
└──────────────┴───────┘
  16 rows
```

Mười sáu giá trị phân biệt cho một cột lẽ ra chỉ có bốn. Nhưng đếm số lượng giá trị là chưa đủ —
điều quyết định là **phân loại** chúng, vì không phải giá trị lạ nào cũng là dữ liệu lỗi.

### 2.2 ⭐ Phân ba nhóm — đây là toàn bộ nhiệm vụ

```bash
q "select case
     when priority_raw in ('1','2','3','4')                                    then '1. so hop le'
     when lower(trim(priority_raw)) in ('urgent','high','medium','low')        then '2. nhan chu'
     else '3. rac'
   end as nhom, count(*) as n
   from bronze_tickets_cdc group by 1 order by 1"
```

**Output:**

```
┌────────────────────────────────┬───────┐
│              nhom              │   n   │
├────────────────────────────────┼───────┤
│ 1. so hop le                   │  6846 │
│ 2. nhan chu (schema evolution) │  7142 │
│ 3. rac                         │   312 │
└────────────────────────────────┴───────┘
```

| Nhóm | Giá trị thấy được | Số bản ghi | Bản chất | Cách xử lý |
|---|---|---|---|---|
| **1** | `'1' '2' '3' '4'` | **6.846** | Đúng contract ban đầu | **Giữ nguyên** |
| **2** | `'urgent' 'high' 'medium' 'low'` | **7.142** | **Schema evolution** — ý nghĩa không đổi, chỉ đổi cách biểu diễn | **Map** về 1..4 theo tài liệu API |
| **3** | `'0'`(49) · `''`(43) · `'P1'`(39) · `'unknown'`(39) · `'P2'`(38) · `'5'`(37) · `NULL`(35) · `'-1'`(32) | **312** | Dữ liệu lỗi thật | **Quarantine** |

- Tổng nhóm 3 = **312** → khớp chính xác `expected/quarantine_tickets.count` = **312** ✓
- Tổng cả ba nhóm = 6.846 + 7.142 + 312 = **14.300** → khớp tổng bản ghi CDC ✓

Tiêu chí phân biệt nhóm 2 và nhóm 3 phải phát biểu được thành một câu:
*giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn hay không?*
`'urgent'` có — nó là cách viết khác của `1`. `'P1'` thì không: dù trông giống một mã ưu tiên, nó
không nằm trong bảng ánh xạ nào mà backend công bố, nên suy diễn ra `1` là **bịa dữ liệu**, không
phải chuẩn hoá.

> ⚠️ Xử lý nhóm 2 như nhóm 3 là lỗi nghiêm trọng nhất có thể mắc ở nhiệm vụ này: quarantine sẽ
> phồng lên **7.454** hàng thay vì 312, và ta vừa vứt bỏ 7.142 bản ghi hoàn toàn hợp lệ — hơn một
> nửa dữ liệu tốt — chỉ vì nguồn đổi format. `RUBRIC.md` phạt riêng trường hợp này: quarantine vượt
> 1.000 hàng mất toàn bộ 4 điểm của hạng mục, kể cả khi `dbt test` pass.

### 2.3 Hậu quả ở tầng Silver

```bash
q "select priority, count(*) n from silver_tickets group by 1 order by 1 nulls last"
```

**Output:**

```
┌──────────┬───────┐
│ priority │   n   │
├──────────┼───────┤
│       -1 │    32 │
│        0 │    49 │
│        1 │  1458 │
│        2 │  1454 │
│        3 │  1456 │
│        4 │  1506 │
│        5 │    37 │
│     NULL │  6488 │
└──────────┴───────┘
```

| Chỉ số | Giá trị |
|---|---|
| Tổng ticket | 12.480 |
| Số hàng `priority` NULL | **6.488** |
| Số hàng ngoài miền 1..4 (`-1`, `0`, `5`) | **118** |
| **Tổng số hàng sai** | **6.606** ✓ *(khớp con số verify báo)* |

Bảng này cho thấy hai bất thường tách biệt — và chính vì tách biệt nên dễ tưởng chúng có hai nguyên
nhân khác nhau. Mục 2.5 sẽ chỉ ra cả hai đều đến từ **cùng một** biểu thức.

### 2.4 Sự cố bắt đầu từ ngày nào

```bash
q "select event_time::date as ngay,
     count(*) filter (where priority_raw in ('1','2','3','4'))                            as dang_so,
     count(*) filter (where lower(trim(priority_raw)) in ('urgent','high','medium','low')) as dang_chu,
     count(*) as tong
   from bronze_tickets_cdc group by 1 order by 1"
```

**Output:**

```
┌────────────┬─────────┬──────────┬───────┐
│    ngay    │ dang_so │ dang_chu │ tong  │
├────────────┼─────────┼──────────┼───────┤
│ 2026-08-03 │     897 │        0 │   897 │
│ 2026-08-04 │     920 │        0 │   927 │
│ 2026-08-05 │    1008 │        0 │  1019 │
│ 2026-08-06 │     953 │        0 │   974 │
│ 2026-08-07 │     987 │        0 │  1012 │
│ 2026-08-08 │    1056 │        0 │  1083 │
│ 2026-08-09 │    1025 │        0 │  1054 │
│ 2026-08-10 │       0 │      961 │   984 │   ← đứt gãy
│ 2026-08-11 │       0 │     1043 │  1071 │
│ 2026-08-12 │       0 │      994 │  1007 │
│ 2026-08-13 │       0 │     1031 │  1051 │
│ 2026-08-14 │       0 │     1038 │  1064 │
│ 2026-08-15 │       0 │     1033 │  1058 │
│ 2026-08-16 │       0 │     1042 │  1099 │
└────────────┴─────────┴──────────┴───────┘
```

→ Ngày nhãn chữ bắt đầu xuất hiện: **2026-08-10** — khớp chính xác thông báo Slack.

Đứt gãy hoàn toàn dứt khoát: trước 08-10 là 100% dạng số, từ 08-10 trở đi là 100% dạng chữ, không
có giai đoạn chuyển tiếp. Đây là dấu vân tay điển hình của **schema evolution có chủ đích** ở phía
nguồn, chứ không phải dữ liệu bị hỏng ngẫu nhiên — dữ liệu hỏng ngẫu nhiên thì rải đều theo thời
gian, đúng như nhóm 3 (312 bản ghi rải suốt 14 ngày).

Nhận diện được điều này quan trọng vì nó quyết định cách xử lý: schema evolution cần **ánh xạ**,
còn dữ liệu hỏng cần **cách ly**. Nhầm lẫn hai thứ là nhầm lẫn tốn kém nhất ở nhiệm vụ này.

### 2.5 `try_cast` hiện tại sai theo hai hướng ngược nhau

Biểu thức hiện tại trong `dbt/macros/normalize_priority.sql` là `try_cast(priority_raw as integer)`.

| Hướng sai | Nó làm gì | Hậu quả |
|---|---|---|
| **1 — quá nghiêm khắc** | Nhãn chữ `'urgent'/'high'/'medium'/'low'` không cast được sang integer → trả về `NULL` | **6.488 hàng NULL** — vứt bỏ 7.142 bản ghi hợp lệ chỉ vì đổi cách biểu diễn |
| **2 — quá dễ dãi** | `'0'`, `'5'`, `'-1'` **đúng là số nguyên** nên cast thành công và lọt qua | **118 hàng** mang giá trị ngoài miền contract 1..4 |

Hai hướng sai này ngược chiều nhau nhưng cùng một gốc: `try_cast` kiểm tra **kiểu dữ liệu**, trong
khi thứ cần kiểm tra là **miền giá trị nghiệp vụ**. Một hàm cast không thể biết `urgent` có ý nghĩa
gì, cũng không thể biết `5` là vô nghĩa — cả hai đều là tri thức nghiệp vụ, phải viết ra tường minh.

### 2.6 Trạng thái contract

Trong `dbt/models/silver/schema.yml`: `contract.enforced` = **`false`**, và khối `tests:` ở cột
`priority` đang bị comment toàn bộ.

→ **Contract ràng buộc cái gì, và không ràng buộc cái gì?**

> Contract ràng buộc **kiểu dữ liệu**: bật lên thì dbt kiểm tra từng cột đúng `data_type` đã khai,
> sai kiểu là dừng model ngay. Nhưng nó **không** ràng buộc **miền giá trị** — `priority = 99` vẫn
> đi qua trơn tru vì 99 đúng là integer, và `priority = -1` cũng vậy.
>
> Miền giá trị là việc của test (`accepted_values`). Cần **cả hai**, và đây là lý do vì sao
> `dbt test` vẫn báo 9/9 pass trong khi 6.606 hàng hỏng: không có test nào được viết cho cột này,
> nên không có gì để fail.

---

## 3 · Phân tích

**1. Vì sao `dbt test` vẫn pass 9/9 dù 6.606 hàng bị hỏng? Test nào đang thiếu?**

> Vì 9 test hiện có đều là `unique`/`not_null` trên các cột **khoá** (`ticket_id`, `event_id`,
> `transcript_id`, `chunk_id`) — chúng kiểm tra tính toàn vẹn định danh, không kiểm tra nội dung
> nghiệp vụ. Cột `priority` không có test nào cả.
>
> Thiếu hai test: `not_null` (bắt 6.488 hàng NULL) và `accepted_values [1,2,3,4]` (bắt 118 hàng
> ngoài miền). Điều đáng suy nghĩ là dữ liệu hỏng tới hơn một nửa mà bộ test vẫn xanh tuyệt đối —
> **một bộ test chỉ bảo vệ được đúng những bất biến mà nó được viết ra để bảo vệ**. Sự im lặng của
> nó không phải bằng chứng dữ liệu sạch.

**2. Thứ tự lọc và xếp hạng — nếu lọc bản ghi hỏng *sau* `row_number()` thì sao?**

> Ticket nào có bản ghi *mới nhất* bị hỏng sẽ biến mất **hoàn toàn** khỏi Silver, vì bản ghi duy
> nhất được chọn (`_rn = 1`) chính là bản ghi bị loại. Tôi đo bằng thực nghiệm thay vì suy luận:
>
> | | Lọc SAU `row_number()` | Lọc TRƯỚC `row_number()` |
> |---|---|---|
> | `silver_tickets` | **12.168** ✗ *(mất 312 ticket)* | **12.480** ✓ |
> | `gold_training_set` | 12.480 — **vẫn "đúng"** | 12.480 ✓ |
> | Ticket mồ côi trong Gold | **312** | **0** |
>
> Phát hiện đáng chú ý ở dòng cuối: cách sai **không hề làm sai số hàng của Gold**, nên nhìn vào
> `gold_training_set` thì mọi thứ vẫn bình thường. Lý do là `merge` chỉ `UPDATE`/`INSERT`, không
> `DELETE`: 312 ticket đó đã được ghi vào Gold ở những ngày chúng còn hợp lệ, và khi biến mất khỏi
> Silver thì hàng cũ vẫn nằm lại — trở thành **ticket mồ côi** mang `priority` đã lỗi thời.
>
> Đây là kiểu lỗi tệ nhất: nó tự che giấu ở đúng tầng mà người ta hay nhìn nhất. `make verify` bắt
> được nó chỉ nhờ có một dòng kiểm tra riêng cho `silver_tickets`.
>
> Cách đúng là **lọc trước, xếp hạng sau**: loại *bản ghi* hỏng chứ không loại cả *ticket*, để
> ticket lùi về trạng thái hợp lệ của lần cập nhật gần nhất.

**3. Câu hỏi thiết kế: nên chặn dữ liệu lỗi ở tầng Bronze hay tầng Silver? Vì sao?**

> **Silver.**
>
> Bronze là bản sao trung thực của nguồn, append-only — nhiệm vụ của nó là *ghi lại những gì nguồn
> đã gửi*, kể cả cái sai. Nếu Bronze từ chối bản ghi lỗi thì ta mất ba thứ cùng lúc: **bằng chứng**
> để điều tra (không còn cách nào biết nguồn đã gửi `'P1'` hay `'P2'`, bao nhiêu lần, từ ngày nào),
> **khả năng xử lý lại** khi quy tắc chuẩn hoá thay đổi (nếu tháng sau backend công bố `P1 → 1` thì
> dữ liệu đã bị vứt không lấy lại được), và **khả năng đối soát** với nguồn.
>
> Cụ thể trong sự cố này: chính vì Bronze giữ nguyên `priority_raw` mà tôi mới dựng được bảng phân
> ba nhóm ở mục 2.2 và xác định được mốc 08-10 ở mục 2.4. Nếu Bronze đã lọc, cuộc điều tra này
> không thể diễn ra.
>
> Silver mới là nơi hợp đồng dữ liệu được áp — đây là ranh giới giữa "dữ liệu thô" và "dữ liệu dùng
> được", và là nơi duy nhất mà việc từ chối một bản ghi có ý nghĩa.

**4. Câu hỏi thiết kế: vì sao không để `dbt test` fail và dừng cả DAG khi gặp bản ghi lỗi?**

> Vì cân nhắc **quy mô**. 312 bản ghi hỏng chiếm 2,18% tổng số bản ghi CDC. Dừng cả DAG vì chúng
> nghĩa là chặn luôn 12.480 ticket sạch, hơn 130.000 event và 31.200 chunk hoàn toàn bình thường —
> biến một sự cố **chất lượng cục bộ** thành một sự cố **ngừng dịch vụ toàn hệ thống**. Ba hệ AI ở
> hạ nguồn (RAG index, classifier, routing agent) mất dữ liệu hoàn toàn, để đổi lấy việc không phải
> nhìn thấy 312 bản ghi xấu.
>
> Quarantine là mô hình **dead-letter queue**: phần sạch chảy tiếp, phần hỏng được giữ lại **kèm
> lý do** trong một hàng đợi có thể đo đếm, cảnh báo và xử lý theo lô. Nó không phải cách né tránh
> vấn đề mà là cách **chuyển vấn đề từ trạng thái ẩn sang trạng thái hiện**: trước khi sửa, 312 bản
> ghi này vẫn tồn tại nhưng nằm lẫn trong Silver dưới dạng NULL và `-1`, không ai đếm được. Sau khi
> sửa, chúng nằm trong một bảng riêng có số đếm và có lý do từ chối.
>
> Điểm sâu hơn: trạng thái tệ nhất không phải pipeline dừng — pipeline dừng thì có người xử lý ngay
> trong vòng vài phút. Tệ nhất là pipeline chạy êm trong khi âm thầm vứt đi một nửa dữ liệu tốt,
> đúng như những gì đã xảy ra suốt từ 08-10 đến khi phiếu này được mở.

---

## 4 · Khắc phục — bốn file, theo đúng thứ tự

| # | File | Thay đổi |
|---|---|---|
| ① | `dbt/macros/normalize_priority.sql` | Thay `try_cast` bằng khối `CASE lower(trim(...))` liệt kê tường minh cả ba nhóm. Mở rộng `priority_reject_reason` để phân loại bốn loại lỗi. |
| ② | `dbt/models/silver/silver_tickets.sql` | Tách CTE `normalized` → `valid_records` (lọc `priority_clean is not null`) → `ranked` (`row_number()`) → `latest`. Lọc **trước** xếp hạng. |
| ③ | `dbt/models/silver/quarantine_tickets.sql` | Thay `where false` bằng `where {{ normalize_priority('priority_raw') }} is null` |
| ④ | `dbt/models/silver/schema.yml` | `contract.enforced: false` → `true`; bỏ comment và điền khối `tests` cho cột `priority`. |

Diff thực tế:

```diff
--- a/dbt/macros/normalize_priority.sql
+++ b/dbt/macros/normalize_priority.sql
 {% macro normalize_priority(col) %}
-    try_cast({{ col }} as integer)
+    case lower(trim(cast({{ col }} as varchar)))
+        when '1' then 1   when '2' then 2
+        when '3' then 3   when '4' then 4
+        when 'urgent' then 1   when 'high'   then 2
+        when 'medium' then 3   when 'low'    then 4
+        else null
+    end
 {% endmacro %}

--- a/dbt/models/silver/silver_tickets.sql
+++ b/dbt/models/silver/silver_tickets.sql
-with ranked as (
-    select *, {{ normalize_priority('priority_raw') }} as priority_clean,
-           row_number() over (...) as _rn
-    from {{ source('bronze', 'bronze_tickets_cdc') }}
-),
+with normalized as (
+    select *, {{ normalize_priority('priority_raw') }} as priority_clean
+    from {{ source('bronze', 'bronze_tickets_cdc') }}
+),
+valid_records as (
+    select * from normalized where priority_clean is not null
+),
+ranked as (
+    select *, row_number() over (...) as _rn from valid_records
+),
 latest as (select * from ranked where _rn = 1)

--- a/dbt/models/silver/quarantine_tickets.sql
+++ b/dbt/models/silver/quarantine_tickets.sql
-where false
+where {{ normalize_priority('priority_raw') }} is null

--- a/dbt/models/silver/schema.yml
+++ b/dbt/models/silver/schema.yml
       contract:
-        enforced: false
+        enforced: true
       - name: priority
+        tests:
+          - not_null
+          - accepted_values:
+              values: [1, 2, 3, 4]
+              quote: false
```

**Vì sao macro là chỗ sửa đúng:** nó được dùng ở **cả hai** model — `silver_tickets` để lấy giá trị
đã chuẩn hoá, `quarantine_tickets` để tìm bản ghi không chuẩn hoá được. Sửa một chỗ là cả hai cùng
đổi, nên hai model **không thể lệch định nghĩa nhau**: bản ghi nào bị loại khỏi Silver thì chắc chắn
xuất hiện ở quarantine, không thừa không thiếu. Đây là một bất biến được bảo đảm bằng cấu trúc code
chứ không bằng kỷ luật của người viết.

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
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  4/4 tiêu chí đạt
```

| Chỉ số | Trước | Sau | Kỳ vọng |
|---|---|---|---|
| `quarantine_tickets` | 0 | **312** ✓ | 312 |
| `silver_tickets` (số ticket) | 12.480 | **12.480** ✓ | 12.480 — không được tụt |
| `priority` NULL | 6.488 | **0** ✓ | 0 |
| `priority` ngoài 1..4 | 118 | **0** ✓ | 0 |
| Ticket mồ côi trong Gold | — | **0** ✓ | 0 |
| `dbt test` | 9/9 | **11/11** ✓ | > 9 test, all pass |
| `contract` enforced | `false` | **`true`** ✓ | true |
| Tổng kết verify | 3/4 | **4/4** ✓ | 4/4 |

*(Checksum `gold_training_set` đổi từ `8622572a97` sang `8dd7c98653` là **đúng như mong đợi**:
checksum tính trên cột `priority`, mà cột này vừa chuyển từ hơn một nửa NULL sang đủ giá trị 1..4.
Nội dung bảng thay đổi vì dữ liệu đã được chữa, không phải vì tính ổn định bị phá.)*

Xác nhận contract thực sự được áp, đọc từ manifest chứ không chỉ từ file cấu hình:

```bash
python -c "import json; m=json.load(open('dbt/target/manifest.json'));
           print([v['contract'] for v in m['nodes'].values() if v['name']=='silver_tickets'])"
```

**Output:**

```
{'enforced': True, 'alias_types': True,
 'checksum': 'd334b86ce0cad2ee179a37a670fd61646806e7dc24301d2d0839f95426a24c05'}
```

### Phân bố priority sau khi sửa

```bash
q "select priority, count(*) n from silver_tickets group by 1 order by 1 nulls last"
```

**Output:**

```
┌──────────┬───────┐
│ priority │   n   │
├──────────┼───────┤
│        1 │  3134 │
│        2 │  3029 │
│        3 │  3115 │
│        4 │  3202 │
└──────────┴───────┘
```

Bốn mức, phân bố cân đối, không còn NULL và không còn `-1`/`0`/`5`. So với trước khi sửa — mỗi mức
chỉ có ~1.450 ticket còn 6.488 rơi vào NULL — lượng dữ liệu dùng được cho model phân loại tăng hơn
gấp đôi.

### Quarantine chia theo lý do

```bash
q "select reject_reason, count(*) n from quarantine_tickets group by 1 order by 2 desc"
q "select count(*) as so_hang, count(distinct ticket_id) as so_ticket_lien_quan from quarantine_tickets"
```

**Output:**

```
┌────────────────────────────────────────────────────────────────────┬───────┐
│                           reject_reason                            │   n   │
├────────────────────────────────────────────────────────────────────┼───────┤
│ priority là số nhưng ngoài miền 1..4 của contract                  │   118 │
│ priority là chuỗi lạ, không thuộc bảng nhãn urgent/high/medium/low │   116 │
│ priority rỗng — nguồn gửi chuỗi trắng                              │    43 │
│ priority NULL — nguồn không gửi giá trị                            │    35 │
└────────────────────────────────────────────────────────────────────┴───────┘

┌─────────┬─────────────────────┐
│ so_hang │ so_ticket_lien_quan │
├─────────┼─────────────────────┤
│     312 │                 312 │
└─────────┴─────────────────────┘
```

Bốn loại cộng lại đúng 312, và đối chiếu ngược được với mục 2.1:
118 = 49(`'0'`) + 37(`'5'`) + 32(`'-1'`) · 116 = 39(`'P1'`) + 39(`'unknown'`) + 38(`'P2'`) ·
43 = chuỗi rỗng · 35 = NULL.

Việc phân loại lý do không chỉ để đẹp — nó cho người trực biết ngay phải làm gì: nhóm 118 cần hỏi
backend về miền giá trị, nhóm 116 cần bổ sung bảng ánh xạ, nhóm 78 còn lại là lỗi truyền dữ liệu.

### ⭐ Kiểm chứng bất biến quan trọng nhất: loại bản ghi, không loại ticket

Tìm một ticket có bản ghi mới nhất bị hỏng và lần theo nó qua cả ba bảng:

```bash
q "select cdc_seq, op, priority_raw, event_time from bronze_tickets_cdc
   where ticket_id='T000714' order by event_time, cdc_seq"

q "select ticket_id, priority, status, updated_at from silver_tickets where ticket_id='T000714'"

q "select ticket_id, cdc_seq, priority_raw, reject_reason from quarantine_tickets
   where ticket_id='T000714'"
```

**Output:**

```
--- Bronze: lịch sử CDC ---
┌─────────┬─────────┬──────────────┬─────────────────────┐
│ cdc_seq │   op    │ priority_raw │     event_time      │
├─────────┼─────────┼──────────────┼─────────────────────┤
│       0 │ c       │ 1            │ 2026-08-08 04:13:11 │   ← hợp lệ
│       2 │ u       │ -1           │ 2026-08-10 18:25:49 │   ← MỚI NHẤT, bị hỏng
└─────────┴─────────┴──────────────┴─────────────────────┘

--- Silver: ticket VẪN CÒN, lùi về trạng thái hợp lệ cũ ---
┌───────────┬──────────┬─────────┬─────────────────────┐
│ ticket_id │ priority │ status  │     updated_at      │
├───────────┼──────────┼─────────┼─────────────────────┤
│ T000714   │        1 │ open    │ 2026-08-08 04:13:11 │
└───────────┴──────────┴─────────┴─────────────────────┘

--- Quarantine: đúng BẢN GHI hỏng được giữ lại kèm lý do ---
┌───────────┬─────────┬──────────────┬───────────────────────────────────────────────────┐
│ ticket_id │ cdc_seq │ priority_raw │                   reject_reason                   │
├───────────┼─────────┼──────────────┼───────────────────────────────────────────────────┤
│ T000714   │       2 │ -1           │ priority là số nhưng ngoài miền 1..4 của contract │
└───────────┴─────────┴──────────────┴───────────────────────────────────────────────────┘
```

Đây là minh chứng trọn vẹn cho nguyên tắc thiết kế của nhiệm vụ này. Ticket `T000714` có bản ghi
mới nhất (`cdc_seq = 2`) mang `priority = '-1'` không hợp lệ. Kết quả:

- **Silver giữ lại ticket** với `priority = 1` và `updated_at = 08-08` — trạng thái hợp lệ của lần
  cập nhật trước đó. Ticket không biến mất, chỉ lùi về phiên bản tin cậy được.
- **Quarantine giữ đúng bản ghi hỏng** (`cdc_seq = 2`) kèm lý do cụ thể, để đội trực xử lý.

Nếu lọc sau `row_number()`, ticket này sẽ biến mất hoàn toàn khỏi Silver — mất luôn cả thông tin
hợp lệ ngày 08-08 mà không có lý do chính đáng nào.

### Kiểm chứng ổn định qua lượt 4 và 5

```bash
make pipeline && q "select (select count(*) from silver_tickets)     as silver,
                           (select count(*) from quarantine_tickets) as quarantine,
                           (select count(*) from gold_training_set)  as gold"
```

**Output:**

```
=== LƯỢT 4 ===                        === LƯỢT 5 ===
┌────────┬────────────┬───────┐       ┌────────┬────────────┬───────┐
│ silver │ quarantine │ gold  │       │ silver │ quarantine │ gold  │
├────────┼────────────┼───────┤       ├────────┼────────────┼───────┤
│  12480 │        312 │ 12480 │       │  12480 │        312 │ 12480 │
└────────┴────────────┴───────┘       └────────┴────────────┴───────┘
```

Cả ba bảng bất động — và quan trọng hơn, pipeline **không dừng** khi gặp bản ghi lỗi: nó tách chúng
ra và tiếp tục chạy bình thường qua mọi lượt.

---

## 6 · Nguyên nhân — câu viết cho báo cáo

> Từ 2026-08-10 backend đổi cách biểu diễn `priority` từ số sang nhãn chữ (`urgent`/`high`/
> `medium`/`low`) — một thay đổi **schema evolution** giữ nguyên ý nghĩa. Macro chuẩn hoá lại dùng
> `try_cast(priority_raw as integer)`, tức kiểm tra **kiểu dữ liệu** thay vì **miền giá trị nghiệp
> vụ**, nên sai theo hai hướng ngược nhau cùng lúc: nó biến toàn bộ 7.142 nhãn chữ hợp lệ thành
> `NULL` (6.488 ticket mất giá trị priority), đồng thời chấp nhận `'0'`, `'5'`, `'-1'` vì chúng
> đúng là số nguyên (118 ticket mang giá trị ngoài contract). Sự cố diễn ra hoàn toàn im lặng vì
> `contract` đang tắt và cột `priority` không có test nào ràng buộc miền giá trị — `dbt test` vẫn
> báo 9/9 pass trong khi 6.606 / 12.480 ticket đã hỏng, và model phân loại tiếp tục được huấn luyện
> trên tập dữ liệu mà hơn một nửa không còn nhãn ưu tiên.

---

## 7 · Phòng ngừa tái diễn

Phép kiểm tra rẻ nhất lẽ ra nên chạy từ đầu để phát hiện sự cố này:

> **Đếm NULL theo từng cột và theo thời gian.** Một cột đột ngột tăng vọt tỷ lệ NULL kể từ một mốc
> ngày cụ thể gần như luôn là schema evolution ở nguồn, không phải lỗi ngẫu nhiên — dữ liệu hỏng
> ngẫu nhiên thì rải đều, còn thay đổi hợp đồng thì tạo ra một đứt gãy sắc nét, đúng như mốc 08-10
> ở mục 2.4. Phép đếm này rẻ, chạy được trên mọi cột, và không cần biết trước lỗi gì sẽ xảy ra.
>
> Ở mức hệ thống, cần hai hàng rào bổ sung cho nhau và **không thay thế được cho nhau**:
> `contract: enforced` để khoá kiểu dữ liệu, và `accepted_values`/`not_null` để khoá miền giá trị.
> Contract một mình vẫn cho `priority = 99` đi qua vì 99 đúng là integer.
>
> Bài học rộng hơn: hợp đồng dữ liệu giữa hai đội không thể chỉ tồn tại dưới dạng **thông báo trên
> Slack**. Ở sự cố này nguyên nhân đã được công bố trước khi hậu quả xảy ra — vấn đề không phải
> thiếu thông tin mà là không có cơ chế nào biến thông tin đó thành hàng rào kỹ thuật. Một hợp đồng
> chỉ có giá trị khi nó được máy kiểm tra ở mỗi lượt chạy.
