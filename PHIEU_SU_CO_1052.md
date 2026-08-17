# Phiếu sự cố #1052 — Query dashboard chậm

**Bài mở rộng A** *(+5 điểm)* · Small-file problem & partition pruning
**File liên quan:** `queries/dashboard.sql`, `tools/compact.py`, `data/gold_events/`
**Trạng thái:** ☑ đang điều tra · ☑ đã sửa · ☑ đã kiểm chứng
**Người xử lý:** Trần Nguyễn Thế Nhật

---

## 1 · Triệu chứng (báo từ vận hành)

> "Dashboard của đội CSKH mất 38 giây mới load. Ba tháng trước chỉ 2 giây.
> Không ai sửa dòng code nào."

Câu cuối là manh mối quan trọng nhất: nếu code không đổi mà hiệu năng suy giảm, thì thứ đã đổi
phải là **dữ liệu** — cụ thể là cách nó nằm trên đĩa.

Chuẩn bị dữ liệu cho bài này *(~30 giây)*:

```bash
make seed-extra
```

> ⚠️ Nếu sau đó gọi `make seed`, dữ liệu này bị xoá — phải chạy lại `make seed-extra`.

---

## 2 · Điều tra — đo TRƯỚC khi sửa

```bash
make explain
ls data/gold_events | wc -l
du -sh data/gold_events
```

**Output:**

```
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000       5,000,000     ≤ 500,000   ✗
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000           5,000        ít hơn   ✗
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —         1,372.2   (tham khảo)

  => giảm 1.0× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)

5000
 20M	data/gold_events
```

| Chỉ số | Baseline |
|---|---|
| rows scanned | **5.000.000** |
| rows on disk | 130.683 |
| files | **5.000** |
| result hash | `4379e4c5d9f3` |
| thời gian | 1.372 ms *(tham khảo, không dùng để chấm)* |
| dung lượng | 20 MB |

### 2.1 Cây EXPLAIN ANALYZE

```bash
make plan
```

**Output** *(phần node đọc Parquet)*:

```json
{
    "operator_type": "TABLE_SCAN",
    "operator_cardinality": 3500,
    "operator_rows_scanned": 5000000,
    "operator_name": "READ_PARQUET",
    "children": []
}
```

Node `READ_PARQUET` **quét 5.000.000 hàng để trả về 3.500** — tỷ lệ lãng phí **1.428 : 1**.
Toàn bộ chi phí nằm ở một chỗ duy nhất, và nó nằm ở tầng đọc file chứ không ở tầng tính toán.

### 2.2 Kích thước file thật

```bash
q "select min(num_rows), max(num_rows), avg(num_rows)::int, sum(num_rows)
   from parquet_file_metadata([...500 file đầu...])"
```

**Output:**

```
┌─────────┬────────────┬────────────┬────────┐
│ it_nhat │ nhieu_nhat │ trung_binh │  tong  │
├─────────┼────────────┼────────────┼────────┤
│      13 │         46 │         26 │  13117 │
└─────────┴────────────┴────────────┴────────┘
```

Mỗi file trung bình **26 hàng**. Nhưng `5.000.000 ÷ 5.000 = 1.000` — mỗi file tốn đúng **1.000 đơn
vị công quét** bất kể nó chỉ chứa 26 hàng.

→ **Vì sao `rows scanned` lớn hơn `rows on disk`?**

> DuckDB đọc Parquet theo lô và làm tròn **lên** theo từng file: mở một file là phải trả chi phí cố
> định của việc đọc footer, giải mã metadata, cấp phát buffer — tương đương khoảng 1.000 hàng, dù
> file đó chỉ có 26 hàng thật. Chênh lệch 5.000.000 so với 130.683 chính là **small-file problem**
> hiện nguyên hình thành một con số: 97,4% công quét là chi phí mở file, không phải chi phí đọc dữ liệu.

→ **Vì sao bài này chấm theo `rows scanned` chứ không theo thời gian?**

> Thời gian phụ thuộc cấu hình máy và trạng thái cache của OS — chạy lần hai luôn nhanh hơn lần đầu
> vì file đã nằm trong page cache. `tools/explain.py` còn ép `threads = 1` để loại nốt ảnh hưởng của
> số nhân CPU. `rows scanned` là đại lượng **tất định**: cùng một layout thì mọi máy cho cùng một số,
> nên so sánh mới có nghĩa.

### 2.3 Đối chiếu filter với storage layout

```bash
ls data/gold_events | head -3
grep -n "where" queries/dashboard.sql
```

**Output:**

```
part-00000.parquet
part-00001.parquet
part-00002.parquet

19:where customer_name = 'ACME'
20:  and strftime(event_time, '%Y-%m-%d') = '2026-08-09'
```

| Câu hỏi | Trả lời |
|---|---|
| Query filter theo những cột nào? | **Hai** điều kiện: `customer_name` và ngày của `event_time` |
| Tên file có mang thông tin của cột nào không? | **Không.** `part-00000.parquet` là số thứ tự thuần tuý, không nói gì về nội dung |
| Dữ liệu có được partition không? | **Không.** Thư mục phẳng, 5.000 file, thứ tự hàng ngẫu nhiên |

Đây là gốc rễ thứ nhất: engine chỉ bỏ qua được file mà nó biết là vô ích **trước khi mở**, và thông
tin đó chỉ có thể đến từ **đường dẫn**. Path không mang thông tin filter thì engine buộc phải mở
toàn bộ 5.000 file rồi mới biết file nào có ích.

### 2.4 ⭐ Predicate có sargable không

Điều kiện lọc ngày hiện tại:

```sql
where strftime(event_time, '%Y-%m-%d') = '2026-08-09'
```

→ Cột `event_time` đang bị **bọc trong một function call**. Hệ quả:

| Cơ chế lược bỏ | Có dùng được không? | Vì sao |
|---|---|---|
| Partition pruning theo tên thư mục | ✗ | Engine không so được kết quả của `strftime(...)` với chuỗi trong tên thư mục — nó phải *tính* hàm đó trên từng hàng mới biết kết quả |
| Row-group pruning theo min/max statistics | ✗ | Statistics lưu min/max của **`event_time` thô**, không lưu min/max của `strftime(event_time)` |

Đây là gốc rễ thứ hai. Ngay cả khi dữ liệu đã được partition đúng, predicate dạng này vẫn vô hiệu
hoá mọi cơ chế lược bỏ. **Cần viết lại sao cho cột đứng một mình ở một vế** — thuật ngữ là
*sargable predicate*.

---

## 3 · Phân tích — ba quyết định, mỗi cái đo bằng thực nghiệm

Thay vì chọn theo cảm tính, tôi dựng thử ba phương án layout và đo `rows scanned` của cùng một truy vấn:

```
PHƯƠNG ÁN                                                        FILE   ROWS SCANNED     HASH
------------------------------------------------------------------------------------------------
1. partition theo customer_name                                   650         49,000  4379e4c5d9f3
2. partition theo event_date, KHÔNG sắp xếp, row_group mặc định    14          9,324  4379e4c5d9f3
3. partition theo event_date + sắp xếp + row_group 2000            14          9,324  4379e4c5d9f3
```

### Quyết định 1 — `PARTITION_BY (event_date)`

Dashboard lọc theo **cả hai** cột, nên phải chọn một cột đưa vào đường dẫn. Số liệu phân bố:

```bash
q "select count(distinct event_date) as so_ngay, count(distinct customer_name) as so_customer,
          count(*) as tong_hang from read_parquet('data/gold_events/*.parquet')"
```

**Output:**

```
┌─────────┬─────────────┬───────────┐
│ so_ngay │ so_customer │ tong_hang │
├─────────┼─────────────┼───────────┤
│      14 │         650 │    130683 │
└─────────┴─────────────┴───────────┘
```

| Cột partition | Số thư mục | Hàng / thư mục | rows scanned đo được |
|---|---|---|---|
| `customer_name` | **650** | ~201 | **49.000** |
| `event_date` | **14** | ~9.334 | **9.324** |

→ **Chọn `event_date`.** Partition theo `customer_name` cho 650 thư mục, mỗi thư mục chỉ ~201 hàng —
tức **tái lập đúng small-file problem đang cần chữa**, chỉ ở quy mô nhỏ hơn, và quét gấp **5,3 lần**.
`event_date` chỉ có 14 giá trị phân biệt nên mỗi file vẫn lành mạnh (~9.334 hàng), đủ lớn để chi phí
mở file không còn đáng kể.

Nguyên tắc rút ra: cột partition tốt phải **vừa nằm trong bộ lọc, vừa có lực lượng thấp**. Một cột
có lực lượng cao dù lọc rất tốt vẫn là lựa chọn sai.

### Quyết định 2 — `ORDER BY event_date, customer_name`

Đo được: **không làm thay đổi `rows scanned`** — 9.324 ở cả phương án 2 và 3. Nhưng nó có một lợi
ích khác đo được rõ:

```bash
# dựng hai phiên bản, so dung lượng
```

**Output:**

```
  KHÔNG sắp xếp                        dung lượng = 4,611 KB
  sắp xếp event_date, customer_name    dung lượng = 3,884 KB
```

→ Giảm **16%** dung lượng, vì dữ liệu đã sắp xếp thì các giá trị giống nhau nằm liền kề, giúp thuật
toán nén (RLE, dictionary) hoạt động hiệu quả hơn. Trên dataset thật, ít byte hơn nghĩa là ít I/O
hơn — chỉ là metric của bài này không đo được điều đó.

### Quyết định 3 — `ROW_GROUP_SIZE 2000`

Mặc định 122.880 gói trọn một ngày (~9.334 hàng) vào **một** row group, nên min/max của nó phủ toàn
bộ 650 khách hàng và hoàn toàn vô dụng cho việc lọc `customer_name`. Về lý thuyết, chia nhỏ row group
cộng với `ORDER BY` sẽ cho phép engine bỏ qua các row group không chứa `ACME`.

Tôi đo thử năm giá trị để kiểm chứng lý thuyết đó:

**Output:**

```
  row_group_size=   None | số row group/ngày=  1 | rows scanned=  9,324
  row_group_size=   5000 | số row group/ngày=  2 | rows scanned=  9,324
  row_group_size=   2000 | số row group/ngày=  5 | rows scanned=  9,324
  row_group_size=    500 | số row group/ngày=  5 | rows scanned=  9,324
  row_group_size=    100 | số row group/ngày=  5 | rows scanned=  9,324
```

→ **Phát hiện trung thực: `rows scanned` hoàn toàn không phản ứng với row-group pruning.** Metric
`OPERATOR_ROWS_SCANNED` của DuckDB đếm số hàng trong các **file được mở**, không trừ đi phần row
group bị bỏ qua. Toàn bộ 536× cải thiện của bài này đến từ **partition pruning ở mức file**, không
một chút nào từ row group.

Hai quan sát phụ đáng ghi nhận:
- DuckDB có **sàn** khoảng 2.048 hàng/row group — đặt 500 hay 100 đều cho cùng 5 row group như 2000.
- Vẫn giữ `2000` vì đó là layout đúng cho các truy vấn lọc hẹp hơn trên dataset lớn hơn, và nó không
  gây hại gì. Nhưng sẽ là không trung thực nếu ghi trong báo cáo rằng nó đóng góp vào con số 536×.

> ⚠️ Sai hướng hay gặp: thêm index. Parquet trên đĩa **không có** index. Thứ duy nhất điều khiển
> được là **file nằm ở đâu** và **hàng nằm theo thứ tự nào trong file**.

---

## 4 · Khắc phục

| File | Thay đổi |
|---|---|
| `tools/compact.py` | Hiện thực `COPY ... TO ...` với `partition_by (event_date)`, `order by event_date, customer_name`, `row_group_size 2000`. Ghi rõ lý do từng quyết định kèm số đo. Thêm `assert src_rows == dst_rows`. |
| `queries/dashboard.sql` | Trỏ sang `data/gold_events_v2/**/*.parquet` với `hive_partitioning = 1`; viết lại điều kiện ngày thành `event_date = DATE '2026-08-09'` (sargable). |

```diff
--- a/queries/dashboard.sql
+++ b/queries/dashboard.sql
-from read_parquet('data/gold_events/*.parquet')
+from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = 1)
 where customer_name = 'ACME'
-  and strftime(event_time, '%Y-%m-%d') = '2026-08-09'
+  and event_date = DATE '2026-08-09'
```

**Vì sao ngữ nghĩa không đổi:** cột `event_date` được sinh từ chính `event_time` (`event_time::date`),
nên `event_date = DATE '2026-08-09'` và `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` chọn đúng
cùng một tập hàng. `tools/explain.py` kiểm chứng điều này bằng hash của kết quả — nếu hash đổi thì
tôi đã sửa ngữ nghĩa chứ không phải sửa hiệu năng, và bài này không được tính điểm.

Đã thêm kiểm tra không mất hàng nào: ☑  *(`assert src_rows == dst_rows` trong `compact.py`)*

```bash
make compact
make explain
```

**Output:**

```
  nguồn : .../data/gold_events  (5,000 file)
  đích  : .../data/gold_events_v2  (14 file)
  hàng  : 130,683 -> 130,683  ✓ không mất hàng nào
```

---

## 5 · Kiểm chứng

```bash
make explain
du -sh data/gold_events data/gold_events_v2
```

**Output:**

```
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000           9,324     ≤ 500,000   ✓
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000              14        ít hơn   ✓
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —             6.8   (tham khảo)

  => giảm 536.3× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)

 20M	data/gold_events
3.8M	data/gold_events_v2
```

| Chỉ số | Trước | Sau | Yêu cầu |
|---|---|---|---|
| **rows scanned** | 5.000.000 | **9.324** | giảm ≥ 10× → đạt **536,3×** ✓ |
| **files** | 5.000 | **14** | giảm nhiều ✓ |
| rows on disk | 130.683 | 130.683 | không đổi — không mất hàng ✓ |
| **result hash** | `4379e4c5d9f3` | **`4379e4c5d9f3`** | **KHÔNG ĐỔI** ✓ |
| thời gian | 1.372,2 ms | **6,8 ms** | *(tham khảo — nhanh hơn 202×)* |
| dung lượng | 20 MB | **3,8 MB** | *(tham khảo — nhỏ hơn 5,3×)* |

`make verify` sau khi sửa: ☑ vẫn **4/4 tiêu chí đạt**, và dòng dashboard chuyển sang
`✓ 5,000,000 → 9,324 (536.3×)`.

Con số 9.324 khớp gần đúng với số hàng thật của một ngày (130.683 ÷ 14 ≈ 9.334) — nghĩa là engine
giờ chỉ đọc đúng partition cần thiết, không thừa file nào.

---

## 6 · Nguyên nhân — câu viết cho báo cáo

> Hai lỗi cộng hưởng. **(a) Storage layout:** `data/gold_events/` là 5.000 file Parquet tí hon
> (~26 hàng/file) không partition; đường dẫn `part-000NN.parquet` không mang thông tin của cột nào
> trong bộ lọc, nên engine buộc phải mở **toàn bộ** 5.000 file rồi mới biết file nào có ích. DuckDB
> lại làm tròn chi phí đọc lên theo từng file (~1.000 hàng/file bất kể file chỉ có 26 hàng), nên một
> tập chỉ 130.683 hàng tốn 5.000.000 đơn vị công quét — 97,4% là chi phí mở file. **(b) Predicate
> không sargable:** `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call,
> nên engine không so được kết quả với tên thư mục partition lẫn min/max statistics của row group,
> làm mọi cơ chế lược bỏ đều tắt. Hiện tượng "không ai sửa dòng code nào" là chính xác: thứ thay đổi
> theo thời gian là **số lượng file**, không phải câu truy vấn.

---

## 7 · Phòng ngừa tái diễn

Vì sao query chậm dần mà "không ai sửa dòng code nào"? Nên giám sát chỉ số nào để phát hiện sớm?

> Vì mỗi lượt ghi sinh thêm file mới, và chi phí truy vấn tỉ lệ với **số file** chứ không với **số
> hàng**. Đây là kiểu suy giảm tuyến tính theo thời gian, không có mốc đứt gãy nào để cảnh báo —
> hôm nay chậm hơn hôm qua 0,5%, và sau ba tháng thì thành 19 lần chậm hơn. Không ai nhận ra vào
> ngày nó bắt đầu.
>
> Chỉ số cần giám sát không phải thời gian truy vấn (nhiễu vì cache và tải máy) mà là hai đại lượng
> tất định: **số file trong dataset** và **số hàng trung bình mỗi file**. Đặt ngưỡng cảnh báo khi số
> hàng trung bình rơi xuống dưới vài nghìn là bắt được vấn đề từ rất sớm. Ở mức quy trình, cần một
> job compaction định kỳ — bố trí lại file là việc bảo trì thường xuyên của hệ dữ liệu dạng file,
> giống như VACUUM của cơ sở dữ liệu truyền thống, không phải việc chỉ làm khi có sự cố.
>
> Bài học rộng hơn: với dữ liệu dạng file, **hiệu năng là một thuộc tính của layout, không phải của
> câu truy vấn**. Không có index để cứu; thứ duy nhất điều khiển được là file nằm ở đâu và hàng nằm
> theo thứ tự nào — và cả hai đều phải được quyết định bằng số đo trên chính hình dạng truy vấn thật.
