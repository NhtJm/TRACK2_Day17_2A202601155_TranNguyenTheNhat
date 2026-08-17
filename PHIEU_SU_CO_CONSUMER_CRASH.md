# Phiếu sự cố — Consumer gặp sự cố giữa batch

**Bài mở rộng B** *(+5 điểm)* · Delivery semantics
**File liên quan:** `ingest/consumer.py`, `ingest/log_client.py`
**Trạng thái:** ☑ đang điều tra · ☑ đã sửa · ☑ đã kiểm chứng
**Người xử lý:** Trần Nguyễn Thế Nhật

> Phiếu này không có số hiệu trong đề bài — `EXTRA.md` mô tả nó dưới dạng kịch bản tái hiện bằng
> `make crash-test`. Bài này **không** cần `make seed-extra`.

---

## 1 · Triệu chứng

`make crash-test` dựng lại đúng tình huống vận hành: consumer đang đọc topic `ai-events` thì bị
`kill -9` ở giữa một batch, sau đó được khởi động lại và chạy nốt. Kịch bản so ba kết quả:

- **A** — chạy một mạch, không sự cố *(mốc đối chiếu)*
- **B** — chạy và bị giết ở lô 7
- **C** — khởi động lại, chạy nốt

Câu hỏi cần trả lời: sau sự cố, ta **mất** bản ghi hay bị **trùng** bản ghi?

---

## 2 · Điều tra — tái hiện TRƯỚC khi sửa

```bash
make crash-test
```

**Output:**

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,500

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 16,500 message
     -> 19,500 hàng / 19,500 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✗ mất 500 hàng
  không trùng bản ghi               ✓
  C == A                            ✗ 19,500 ≠ 20,000
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: CHƯA ĐẠT ✗
```

| Kịch bản | Số hàng | event_id phân biệt |
|---|---|---|
| A. chạy một mạch, không sự cố | 20.000 | 20.000 |
| B. bị giết ở lô 7 — offset đã commit | — | **3.500** = 7 lô × 500 |
| C. khởi động lại, chạy nốt | **19.500** | 19.500 |

| Kết luận | |
|---|---|
| **Mất 500 hàng** / trùng 0 hàng | đúng bằng kích thước một lô |
| C có bằng A không? | **Không** — 19.500 ≠ 20.000 |
| → Consumer đang ở ngữ nghĩa nào? | **at-most-once** |

Con số `offset đã commit: 3.500` là bằng chứng quyết định: offset đã dịch qua đủ **7** lô
(7 × 500 = 3.500), nhưng dữ liệu của lô thứ 7 chưa kịp ghi. 500 message đó nằm giữa hai trạng thái —
nguồn coi như đã giao, kho thì chưa nhận.

### 2.1 Thứ tự thao tác hiện tại

Trong `ingest/consumer.py`, hàm `consume()`:

```python
consumer.commit()                 # ghi nhận offset
maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
write_batch(con, batch)           # ghi dữ liệu
```

| Câu hỏi | Trả lời |
|---|---|
| Nếu tiến trình chết tại `maybe_crash()`, batch hiện tại đã được ghi chưa? | **Chưa** — `write_batch()` nằm sau, chưa kịp chạy |
| Offset đã dịch chưa? | **Rồi** — `commit()` nằm trước và đã hoàn tất, ghi xuống đĩa |
| Lần khởi động lại sẽ đọc từ đâu? | Từ offset 3.500, tức **bỏ qua** lô 7 |
| Batch đang dở đi đâu? | **Mất vĩnh viễn.** Không cơ chế nào biết nó tồn tại |

Điểm nguy hiểm nhất: consumer khởi động lại **thành công**, không có log lỗi nào, và tiếp tục chạy
bình thường. Mất mát chỉ lộ ra khi có người ngồi đối chiếu số hàng với nguồn.

---

## 3 · Phân tích

### 3.1 Ba ngữ nghĩa giao vận

| Ngữ nghĩa | Thứ tự thao tác | Hậu quả khi crash |
|---|---|---|
| **at-most-once** | commit offset **trước** khi ghi | **Mất** dữ liệu — lô đang dở bị bỏ qua vĩnh viễn |
| **at-least-once** | commit offset **sau** khi ghi | **Trùng** dữ liệu — lô đang dở bị phát lại |
| **exactly-once** | — | **Không tồn tại** ở tầng giao vận |

→ **Thứ chọn được là gì?**

> Không có cách sắp xếp nào của hai thao tác `commit` và `write` cho ra exactly-once, vì chúng nằm
> trên hai hệ thống khác nhau (file offset và kho dữ liệu) và không có giao dịch phân tán nào bao
> trọn cả hai. Tiến trình có thể chết ở đúng khe giữa chúng, và luôn có một trong hai đã hoàn tất
> còn cái kia thì chưa.
>
> Thứ đạt được là **at-least-once cộng với một phép ghi idempotent**. Ta chấp nhận việc phát lại là
> điều tất yếu, rồi làm cho việc phát lại trở nên **vô hại**. Đây là chuyển vấn đề từ tầng giao vận
> — nơi không giải được — sang tầng lưu trữ, nơi giải được.

### 3.2 Hệ quả của việc chỉ đảo thứ tự — đo bằng thực nghiệm

Tôi đảo `write_batch()` lên trước `commit()` nhưng **giữ nguyên** câu `INSERT` thuần, rồi chạy lại:

**Output:**

```
  B. chạy và bị giết ở lô 7
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,500 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✗ trùng 500 hàng
  C == A                            ✗ 20,500 ≠ 20,000
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: CHƯA ĐẠT ✗
```

Bằng chứng đối xứng hoàn hảo với baseline:

| | offset commit | số hàng | event_id phân biệt | Vấn đề |
|---|---|---|---|---|
| **at-most-once** (gốc) | 3.500 | **19.500** | 19.500 | **mất** 500 |
| **at-least-once** (chỉ đảo thứ tự) | 3.000 | **20.500** | **20.000** | **trùng** 500 |
| Mục tiêu | — | 20.000 | 20.000 | — |

Cột `event_id phân biệt` là chi tiết đáng chú ý: ở phương án at-least-once nó đã **đúng 20.000** —
tức **không mất dữ liệu nào**, chỉ là 500 bản ghi bị ghi hai lần. Vấn đề đã chuyển từ *mất mát*
(không sửa được sau khi xảy ra) sang *trùng lặp* (sửa được bằng cách làm phép ghi idempotent).
Đây là một bước tiến thật, không phải đổi lỗi này lấy lỗi khác.

### 3.3 Điều kiện để phép ghi idempotent

DuckDB hỗ trợ `insert ... on conflict (...) do update set ...`, nhưng **chỉ khi** cột khoá có ràng
buộc `primary key` hoặc `unique`. Hằng `DDL` hiện tại khai `event_id varchar` trần, không ràng buộc.

→ **Cần sửa gì trong `DDL`? Cột nào làm khoá?**

> `event_id` — đây là định danh nghiệp vụ của một message, do nguồn sinh ra, nên phát lại cùng một
> message luôn mang cùng một `event_id`. Thêm `primary key` cho cột này vừa mở khoá được mệnh đề
> `ON CONFLICT`, vừa biến bất biến "một event chỉ có một hàng" thành ràng buộc được **database bảo
> đảm**, chứ không phải một quy ước mà code phải tự giữ.

### 3.4 `DO UPDATE` khác `DO NOTHING` ở điểm nào khi message được replay với nội dung **đã đổi**?

> Cả hai đều cho đúng **số hàng** — không bản ghi trùng nào được tạo ra, và cả hai đều làm bài
> `make crash-test` này pass. Khác biệt nằm ở **nội dung**:
>
> - `DO NOTHING` giữ nguyên bản ghi cũ. Nếu message được phát lại mang giá trị đã cập nhật, kho đọng
>   lại phiên bản lỗi thời và **âm thầm lệch khỏi nguồn** — đúng loại lỗi im lặng mà cả ba nhiệm vụ
>   chính của lab này đều mắc phải.
> - `DO UPDATE` ghi đè bằng phiên bản mới, đưa kho **hội tụ** về trạng thái mới nhất của nguồn.
>
> **Tôi chọn `DO UPDATE`**, vì mục tiêu của idempotency ở đây không chỉ là *không nhân bản* mà là
> *hội tụ đúng trạng thái*. Với một luồng sự kiện có thể được sửa lại và phát lại, `DO NOTHING`
> biến lần ghi **đầu tiên** thành lần ghi thắng cuộc — một quy tắc không có cơ sở nghiệp vụ nào.
>
> `DO NOTHING` chỉ phù hợp khi bản ghi **bất biến theo thiết kế** (log append-only, sự kiện đã xảy
> ra không thể sửa). Khi đó nó rẻ hơn vì không phải ghi lại. Nhưng cần chọn có ý thức, không chọn
> vì nó ngắn hơn.

---

## 4 · Khắc phục — hai hạng mục, thiếu một là chưa đủ

| Hạng mục | File | Thay đổi |
|---|---|---|
| **(a)** thứ tự thao tác trong `consume()` | `ingest/consumer.py` | Đảo thành `write_batch()` → `maybe_crash()` → `consumer.commit()`, chuyển từ at-most-once sang **at-least-once** |
| **(b)** tính idempotent của `write_batch()` + `DDL` | `ingest/consumer.py` | Thêm `primary key` cho `event_id`; đổi `INSERT` thuần thành `insert ... on conflict (event_id) do update set ...` cho toàn bộ cột còn lại |

Diff thực tế:

```diff
--- a/ingest/consumer.py
+++ b/ingest/consumer.py
 create table if not exists {TABLE} (
-    event_id      varchar,
+    event_id      varchar primary key,

 def write_batch(con, batch):
     con.executemany(
-        f"insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?)",
+        f"""
+        insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?)
+        on conflict (event_id) do update set
+            ticket_id     = excluded.ticket_id,
+            customer_id   = excluded.customer_id,
+            customer_name = excluded.customer_name,
+            event_type    = excluded.event_type,
+            latency_ms    = excluded.latency_ms,
+            event_time    = excluded.event_time,
+            _ingested_at  = excluded._ingested_at
+        """,

 # trong consume():
-            consumer.commit()                 # ghi nhận offset
-            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
-            write_batch(con, batch)           # ghi dữ liệu
+            write_batch(con, batch)           # ghi dữ liệu
+            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
+            consumer.commit()                 # ghi nhận offset
```

**Vì sao hai hạng mục không thay thế được cho nhau:** (a) một mình chuyển mất mát thành trùng lặp —
đã đo ở mục 3.2, vẫn CHƯA ĐẠT. (b) một mình thì vô nghĩa vì với thứ tự cũ, lô đang dở không bao giờ
được ghi lần nào nên chẳng có xung đột nào để giải quyết. Chỉ khi ghép lại: at-least-once bảo đảm
**không mất**, phép ghi idempotent bảo đảm **không trùng**.

---

## 5 · Kiểm chứng

```bash
make crash-test
make verify
```

**Output:**

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

| Chỉ số | at-most-once (gốc) | at-least-once (chỉ đảo) | **at-least-once + idempotent** |
|---|---|---|---|
| Số hàng sau sự cố + restart | 19.500 | 20.500 | **20.000** ✓ |
| event_id phân biệt | 19.500 | 20.000 | **20.000** ✓ |
| Không mất bản ghi | ✗ mất 500 | ✓ | **✓** |
| Không trùng bản ghi | ✓ | ✗ trùng 500 | **✓** |
| C == A | ✗ | ✗ | **✓** |
| Kết luận | CHƯA ĐẠT | CHƯA ĐẠT | **ĐẠT** ✓ |

Chi tiết đáng chú ý ở kịch bản C: consumer ghi **17.000** message để tạo ra **20.000** hàng — nhiều
hơn 500 so với số hàng còn thiếu. Đúng 500 message của lô 7 được **phát lại và ghi đè** lên chính
chúng, thay vì tạo hàng mới. Đó chính là tính idempotent thể hiện thành con số.

`make verify` sau khi sửa: ☑ vẫn **4/4 tiêu chí đạt** — ba nhiệm vụ chính không bị ảnh hưởng.

---

## 6 · Nguyên nhân — câu viết cho báo cáo

> Thứ tự thao tác trong `consume()` là `commit()` → `write_batch()`, tức **at-most-once**: khi tiến
> trình bị `kill -9` giữa hai bước, offset đã dịch qua lô hiện tại nhưng dữ liệu chưa kịp ghi, nên
> lần khởi động lại đọc tiếp từ lô sau và **500 message của lô đang dở mất vĩnh viễn**. Mất mát diễn
> ra hoàn toàn im lặng vì consumer khởi động lại thành công và không sinh log lỗi nào. Đảo thứ tự
> thành ghi-trước-commit-sau chuyển hệ sang **at-least-once** — không mất nữa nhưng lô đó bị phát
> lại, và với câu `INSERT` thuần thì phát lại nghĩa là 500 hàng trùng. Exactly-once không tồn tại ở
> tầng giao vận vì `commit` và `write` nằm trên hai hệ thống không có giao dịch chung; lời giải đúng
> là **at-least-once cộng một phép ghi idempotent** (`primary key` trên `event_id` +
> `on conflict do update`), tức chuyển vấn đề từ tầng giao vận — nơi không giải được — sang tầng lưu
> trữ, nơi giải được.

---

## 7 · Phòng ngừa tái diễn

Vì sao sự cố này khó phát hiện trong vận hành thật?

> Vì **không có tín hiệu nào**. Consumer bị giết rồi khởi động lại thành công, đọc tiếp từ offset đã
> lưu, xử lý bình thường, không sinh log lỗi. Tiến trình giám sát thấy consumer đang chạy, độ trễ
> bình thường, không có message tồn đọng. Mất mát chỉ lộ ra khi có người ngồi đối chiếu số hàng ở
> kho với số message ở nguồn — và trong hệ thống thật, việc đối chiếu đó thường không tồn tại.
>
> Điều đáng nói: mất mát tỉ lệ với **tần suất crash**, không tỉ lệ với lưu lượng. Một hệ thống bị
> restart vài lần mỗi tuần sẽ rò rỉ đều đặn vài nghìn bản ghi mỗi tháng mà không có mốc nào để phát
> hiện, giống hệt kiểu suy giảm âm thầm ở phiếu #1052.
>
> Cần ba thứ: **(1)** một phép đối soát định kỳ giữa số message đã commit ở nguồn và số hàng phân
> biệt ở kho; **(2)** ràng buộc `primary key` trên khoá nghiệp vụ ở mọi bảng nhận dữ liệu streaming —
> nó biến tính idempotent thành thứ database bảo đảm chứ không phải thứ code phải tự nhớ; **(3)**
> quy ước mặc định trong đội là **ghi trước, commit sau**, vì thứ tự ngược lại đánh đổi *mất dữ liệu*
> — thứ không sửa được — lấy *tiết kiệm một phép ghi trùng* — thứ sửa được bằng một dòng
> `on conflict`.
>
> Bài học chung với cả bốn phiếu còn lại: lỗi nguy hiểm trong hệ dữ liệu là lỗi **không sinh ra tín
> hiệu**. Ở đây pipeline vẫn xanh, consumer vẫn chạy, và dữ liệu vẫn mất.
