# Phiếu sự cố #1043 — Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

**Nhiệm vụ 2** · Dữ liệu về muộn · Bảng: `gold_feature_daily`
**Trạng thái:** ☑ đang điều tra · ☑ đã sửa · ☑ đã kiểm chứng
**Người xử lý:** Trần Nguyễn Thế Nhật

---

## 1 · Triệu chứng (báo từ vận hành)

> "`gold_feature_daily` thiếu khoảng 5% so với đối chiếu thủ công. Kỳ lạ là chỉ thiếu
> ở những ngày đã chạy xong từ lâu, ngày mới thì đủ."

Xác nhận từ `make verify` baseline:

| Bảng | Ổn định | Số hàng | Kỳ vọng | Chênh |
|---|---|---|---|---|
| `gold_feature_daily` | ✓ ok | 8.645 | 9.100 | thiếu **455** |

**Điểm đáng chú ý ngay từ đầu:** bảng này **ổn định** — checksum ba lượt giống hệt (`3269dbe574`).
Nó cho ra cùng một kết quả *sai* ở mọi lượt chạy. Đây là loại lỗi khác hẳn phiếu #1041: ở đó bảng
không ổn định nhưng ta còn thấy nó động đậy; ở đây bảng đứng yên một cách đáng tin cậy, và chính sự
đứng yên đó khiến không ai nghi ngờ gì.

Kiểm chứng con số kỳ vọng trước khi điều tra tiếp:

```bash
q "select count(distinct event_date) as so_ngay,
          count(distinct customer_id) as so_customer,
          count(distinct event_date) * count(distinct customer_id) as tich
   from silver_events"
```

**Output:**

```
┌─────────┬─────────────┬───────┐
│ so_ngay │ so_customer │ tich  │
├─────────┼─────────────┼───────┤
│      14 │         650 │  9100 │
└─────────┴─────────────┴───────┘
```

14 ngày × 650 khách hàng = **9.100** cặp. Con số trong `expected/` không phải giá trị tuỳ tiện mà
là tích Descartes đầy đủ — nghĩa là mọi khách hàng đều có hoạt động mỗi ngày, và bất kỳ cặp nào
vắng mặt trong Gold đều là dữ liệu bị mất, không phải "khách hàng đó hôm ấy không dùng dịch vụ".

---

## 2 · Điều tra

### 2.1 ⭐ Đo độ trễ ingest — P99 là số bắt buộc của báo cáo

```bash
q "select
     quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_ngay,
     quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_ngay,
     quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay,
     max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
     avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end) as ty_le_late
   from bronze_events"
```

**Output:**

```
┌────────────────────┬──────────────────┬────────────────────┬───────────┬──────────────────────┐
│      p50_ngay      │     p95_ngay     │      p99_ngay      │ max_ngay  │      ty_le_late      │
├────────────────────┼──────────────────┼────────────────────┼───────────┼──────────────────────┤
│ 0.1280902777777778 │ 1.81369270833333 │ 2.7258333333333336 │ 2.9446875 │ 0.050509029676661876 │
└────────────────────┴──────────────────┴────────────────────┴───────────┴──────────────────────┘
```

| p50 | p95 | **p99** | max | tỷ lệ trễ > 1 ngày |
|---|---|---|---|---|
| 0,128 ngày (~3 giờ) | 1,814 ngày | **2,726 ngày** | 2,945 ngày | **5,05 %** |

- **P99 = 2,73 ngày** ← số bắt buộc phải có trong báo cáo
- Lookback sẽ chọn: **3 ngày** (làm tròn lên từ P99)

Con số **5,05%** trùng khớp với mô tả "thiếu khoảng 5%" trong phiếu sự cố. Đây là xác nhận đầu tiên
rằng độ trễ ingest chính là hướng điều tra đúng, chứ không phải một trùng hợp.

### 2.2 Phân bố độ trễ theo ngày

```bash
q "select date_diff('day', event_time, _ingested_at) as tre_ngay, count(*) as n,
          round(100.0*count(*)/sum(count(*)) over (), 2) as pct
   from bronze_events group by 1 order by 1"
```

**Output:**

```
┌──────────┬────────┬────────┐
│ tre_ngay │   n    │  pct   │
├──────────┼────────┼────────┤
│        0 │ 108862 │  84.09 │
│        1 │  14165 │  10.94 │
│        2 │   3842 │   2.97 │
│        3 │   2593 │   2.00 │
└──────────┴────────┴────────┘
```

→ **84,09%** bản ghi tới kho ngay trong ngày sự kiện xảy ra. Phần còn lại rải ra: 10,94% trễ 1 ngày,
2,97% trễ 2 ngày, 2,00% trễ 3 ngày. **Không có bản ghi nào trễ quá 3 ngày** — phân bố kết thúc dứt
khoát ở đó, phù hợp với `max = 2,94 ngày`.

Phân bố này có hai cụm rõ rệt: một cụm lớn "đúng giờ" và một đuôi "về muộn" kéo dài đúng 3 ngày.
Đuôi đó chính là phần dữ liệu đang bị đánh rơi.

### 2.3 Điều kiện lọc hiện tại

Trong `dbt/models/gold/gold_feature_daily.sql`, khối `is_incremental()` — đọc bản đã biên dịch để
chắc chắn đây là câu lệnh thật:

```bash
grep "event_date" dbt/target/compiled/lab17/models/gold/gold_feature_daily.sql
```

**Output:**

```sql
where event_date > (select max(event_date) from "warehouse"."main"."gold_feature_daily")
```

→ **Mốc so sánh là `max(event_date)` của chính bảng đích** — tức "ngày sự kiện lớn nhất đã từng được
ghi". Đại lượng này chỉ **tăng đơn điệu** theo thời gian, không bao giờ giảm.

Đọc thành lời: *chỉ xử lý những sự kiện có ngày xảy ra mới hơn ngày mới nhất tôi từng thấy*. Điều
kiện này ngầm giả định rằng dữ liệu luôn tới theo đúng thứ tự thời gian nó xảy ra — một giả định
mà mục 2.1 vừa chứng minh là sai với 15,91% số bản ghi.

### 2.4 ⭐ Xác định chính xác tập hàng bị thiếu

```bash
q "select s.event_date, count(distinct s.customer_id) as so_cap_thieu
   from silver_events s
   left join gold_feature_daily g
     on g.event_date = s.event_date and g.customer_id = s.customer_id
   where g.customer_id is null
   group by 1 order by 1"
```

**Output:**

```
┌────────────┬──────────────┐
│ event_date │ so_cap_thieu │
├────────────┼──────────────┤
│ 2026-08-03 │           46 │
│ 2026-08-04 │           41 │
│ 2026-08-05 │           46 │
│ 2026-08-06 │           30 │
│ 2026-08-07 │           39 │
│ 2026-08-08 │           41 │
│ 2026-08-09 │           31 │
│ 2026-08-10 │           48 │
│ 2026-08-11 │           43 │
│ 2026-08-12 │           43 │
│ 2026-08-13 │           47 │
└────────────┴──────────────┘
  11 rows        (tổng: 455)
```

→ **Các cặp bị thiếu tập trung ở ngày CŨ.** Danh sách dừng lại ở 08-13; ba ngày mới nhất
(08-14, 08-15, 08-16) **không thiếu cặp nào**. Điều này khớp chính xác với mô tả của phiếu: *"chỉ
thiếu ở những ngày đã chạy xong từ lâu, ngày mới thì đủ"*.

Lý do ba ngày cuối đủ: dữ liệu về muộn của chúng vẫn đang tới trong khoảng thời gian mà
`max(event_date)` chưa vượt qua — chúng còn kịp lọt qua điều kiện `>`. Càng lùi về quá khứ, mốc so
sánh càng bỏ xa, và cơ hội càng khép lại.

Kiểm chứng giả thuyết bằng thời điểm dữ liệu tới kho — so sánh nhóm **thiếu** với nhóm **có mặt**:

```bash
q "select s.event_date, min(s.ingested_date) as toi_som_nhat, max(s.ingested_date) as toi_muon_nhat,
          count(*) as so_event
   from silver_events s
   left join gold_feature_daily g
     on g.event_date = s.event_date and g.customer_id = s.customer_id
   where g.customer_id is null
   group by 1 order by 1 limit 5"
```

**Output:**

```
--- Nhóm THIẾU ---
┌────────────┬──────────────┬───────────────┬──────────┐
│ event_date │ toi_som_nhat │ toi_muon_nhat │ so_event │
├────────────┼──────────────┼───────────────┼──────────┤
│ 2026-08-03 │ 2026-08-04   │ 2026-08-06    │      416 │
│ 2026-08-04 │ 2026-08-05   │ 2026-08-07    │      388 │
│ 2026-08-05 │ 2026-08-06   │ 2026-08-08    │      412 │
│ 2026-08-06 │ 2026-08-07   │ 2026-08-09    │      274 │
│ 2026-08-07 │ 2026-08-08   │ 2026-08-10    │      332 │
└────────────┴──────────────┴───────────────┴──────────┘

--- Nhóm CÓ MẶT (đối chứng) ---
┌────────────┬──────────────┬───────────────┐
│ event_date │ toi_som_nhat │ toi_muon_nhat │
├────────────┼──────────────┼───────────────┤
│ 2026-08-03 │ 2026-08-03   │ 2026-08-06    │
│ 2026-08-04 │ 2026-08-04   │ 2026-08-07    │
│ 2026-08-05 │ 2026-08-05   │ 2026-08-08    │
│ 2026-08-06 │ 2026-08-06   │ 2026-08-09    │
│ 2026-08-07 │ 2026-08-07   │ 2026-08-10    │
└────────────┴──────────────┴───────────────┘
```

Khác biệt nằm gọn trong một cột. Nhóm **thiếu** có `toi_som_nhat` = `event_date` **+ 1** — không một
sự kiện nào của cặp đó tới kho trong chính ngày nó xảy ra. Nhóm **có mặt** có `toi_som_nhat` =
**chính** `event_date`. Cột `toi_muon_nhat` thì giống hệt nhau ở cả hai nhóm, nên nó không phải yếu
tố phân biệt.

Từ đó rút ra một phát biểu sắc gọn có thể kiểm chứng được:

> Một cặp `(event_date, customer_id)` sống sót **khi và chỉ khi** có ít nhất một sự kiện của nó
> tới kho ngay trong chính ngày sự kiện xảy ra.

### 2.5 ⭐ Kiểm chứng định lượng giả thuyết

Nếu phát biểu trên đúng, số cặp mà *toàn bộ* sự kiện đều tới muộn phải bằng đúng 455 — và phải là
đúng những cặp đó, không phải một tập khác có cùng kích thước:

```bash
q "with cap as (select event_date, customer_id, min(ingested_date) as toi_som_nhat
               from silver_events group by 1,2),
     thieu as (select distinct s.event_date, s.customer_id from silver_events s
               left join gold_feature_daily g
                 on g.event_date=s.event_date and g.customer_id=s.customer_id
               where g.customer_id is null)
   select (select count(*) from cap where toi_som_nhat > event_date) as du_doan,
          (select count(*) from thieu)                               as thuc_te,
          (select count(*) from cap c join thieu t using (event_date, customer_id)
             where c.toi_som_nhat > c.event_date)                    as trung_khop"
```

**Output:**

```
┌────────────────────────┬───────────────┬────────────┐
│ du_doan_theo_gia_thuyet│ thuc_te_thieu │ trung_khop │
├────────────────────────┼───────────────┼────────────┤
│                    455 │           455 │        455 │
└────────────────────────┴───────────────┴────────────┘

┌────────────────────────┬────────┐
│          nhom          │ so_cap │
├────────────────────────┼────────┤
│ toàn bộ tới MUỘN       │    455 │
│ có event tới ĐÚNG ngày │   8645 │
└────────────────────────┴────────┘
```

**455 dự đoán = 455 thực tế = 455 trùng khớp**, và nhóm còn lại đúng bằng **8.645** — con số hiện
có trong Gold. Giả thuyết được xác nhận ở mức chính xác tuyệt đối, không phải xấp xỉ.

### 2.6 Lần theo một cặp bị mất cụ thể

```bash
q "select event_id, event_time, _ingested_at, event_date, ingested_date
   from silver_events where customer_id='C0004' and event_date=date '2026-08-12'
   order by _ingested_at"
```

**Output:**

```
┌───────────┬─────────────────────┬─────────────────────┬────────────┬───────────────┐
│ event_id  │     event_time      │    _ingested_at     │ event_date │ ingested_date │
├───────────┼─────────────────────┼─────────────────────┼────────────┼───────────────┤
│ E00054565 │ 2026-08-12 00:35:17 │ 2026-08-14 05:18:11 │ 2026-08-12 │ 2026-08-14    │
│ E00038580 │ 2026-08-12 01:59:38 │ 2026-08-14 15:51:59 │ 2026-08-12 │ 2026-08-14    │
│ E00053839 │ 2026-08-12 20:37:57 │ 2026-08-14 15:54:54 │ 2026-08-12 │ 2026-08-14    │
│ E00040628 │ 2026-08-12 14:47:40 │ 2026-08-14 16:40:37 │ 2026-08-12 │ 2026-08-14    │
│ E00048622 │ 2026-08-12 10:13:55 │ 2026-08-14 19:37:19 │ 2026-08-12 │ 2026-08-14    │
│ E00042676 │ 2026-08-12 08:14:48 │ 2026-08-14 19:56:48 │ 2026-08-12 │ 2026-08-14    │
│ E00051697 │ 2026-08-12 23:16:26 │ 2026-08-14 20:50:20 │ 2026-08-12 │ 2026-08-14    │
│ E00044724 │ 2026-08-12 09:27:10 │ 2026-08-14 22:08:34 │ 2026-08-12 │ 2026-08-14    │
│ E00050271 │ 2026-08-12 18:24:09 │ 2026-08-15 06:12:45 │ 2026-08-12 │ 2026-08-15    │
│ E00046772 │ 2026-08-12 22:11:09 │ 2026-08-15 11:25:33 │ 2026-08-12 │ 2026-08-15    │
│ E00052887 │ 2026-08-12 22:03:57 │ 2026-08-15 12:14:27 │ 2026-08-12 │ 2026-08-15    │
└───────────┴─────────────────────┴─────────────────────┴────────────┴───────────────┘
  11 rows
```

Khách hàng `C0004` có **11 sự kiện** trong ngày 08-12, và **toàn bộ 11 sự kiện** đều tới kho vào
08-14 hoặc 08-15 — không một cái nào tới trong ngày 08-12.

Diễn giải đầy đủ số phận của cặp `(08-12, C0004)`:

| Câu hỏi | Trả lời |
|---|---|
| Tại lượt chạy ngày vận hành 08-12, Gold có gì cho `C0004`? | **Không có gì** — Bronze chưa nhận được sự kiện nào của cặp này. |
| Ngày 08-14, khi dữ liệu tới, `max(event_date)` trong bảng đích là bao nhiêu? | **08-13** — vì ngày vận hành 08-13 đã chạy xong và ghi dữ liệu của nó. |
| Điều kiện `event_date > max(event_date)` có khớp không? | **Không**: `08-12 > 08-13` là sai. Dữ liệu bị bỏ qua. |
| **Ngày hôm sau (08-15) thì sao?** | **Càng không** — `max(event_date)` lúc này đã là 08-14. Khoảng cách còn nới rộng thêm. |
| Vậy nó được xử lý ở lượt chạy nào? | **Không bao giờ.** Mốc so sánh chỉ tăng, nên cánh cửa đã đóng vĩnh viễn. |

→ **Kết luận.** Đây không phải trường hợp dữ liệu "đến chậm rồi sẽ được bù". Nó là dữ liệu bị đánh
rơi **vĩnh viễn và im lặng**: pipeline không báo lỗi, `dbt test` vẫn pass, bảng vẫn ổn định qua mọi
lượt chạy. Không có cơ chế nào trong hệ thống hiện tại có thể phát hiện ra mất mát này.

---

## 3 · Phân tích

**1. Vì sao bảng vừa ổn định vừa sai? Hai đại lượng này đo cái gì khác nhau?**

> `ỔN ĐỊNH` đo **tính tất định**: chạy lại có cho cùng kết quả không. `SỐ HÀNG` đo **tính đúng**:
> kết quả đó có khớp thực tế không. Đây là hai thuộc tính hoàn toàn độc lập.
>
> Model này tất định vì logic của nó không phụ thuộc thứ tự hay thời điểm chạy — nó luôn bỏ sót
> **đúng cùng một tập** 455 cặp. Sai lệch được tái tạo trung thực ở mọi lượt chạy, nên checksum
> giống hệt nhau và mọi phép kiểm tra dựa trên tính lặp lại đều báo xanh.
>
> Đây là lý do `make verify` in tách riêng hai cột. Một hệ thống chỉ giám sát tính ổn định sẽ mù
> hoàn toàn trước lớp lỗi này — và trong vận hành thực tế, "kết quả không đổi" thường bị hiểu nhầm
> thành "kết quả đúng".

**2. Đổi `>` thành `>=` đã đủ chưa? Toán tử đó nới window thêm đúng mấy ngày?**

> **Không đủ.** Về mặt lý thuyết, `>=` chỉ kéo thêm đúng **một ngày biên** — chính ngày `max` —
> chứ không chạm tới các ngày trước đó. Nhưng thay vì lập luận suông, tôi đã đo bằng thực nghiệm:
>
> | Chỉ số | `>` (gốc) | `>=` |
> |---|---|---|
> | Tổng hàng | 8.645 | **16.704** |
> | Cặp phân biệt | 8.645 | 8.709 |
> | Còn thiếu | 455 | **391** |
> | Hàng trùng lặp | 0 | **7.995** |
>
> `>=` thu hồi được **64 / 455** cặp — chỉ 14% — trong khi đẻ ra gần 8.000 hàng trùng, vì ngày biên
> bị tính đi tính lại ở mỗi ngày vận hành mà không có khoá để ghi đè. Nói cách khác, đổi toán tử
> vừa *không giải quyết* vấn đề vừa *tạo thêm* một vấn đề mới thuộc đúng loại của phiếu #1041.
>
> Điều này cho thấy vấn đề không nằm ở **biên** của điều kiện, mà ở **bản chất của mốc so sánh**:
> `max(event_date)` là một đại lượng tăng đơn điệu, nên mọi cách tinh chỉnh toán tử quanh nó đều
> không cứu được dữ liệu đã nằm lại phía sau.

**3. Mỗi ngày lookback thêm phải trả giá gì — ở lượt chạy này, và ở mọi lượt chạy sau này?**

> Mỗi ngày lùi thêm là thêm 650 cặp phải tính lại **trong từng ngày vận hành, ở mọi lượt chạy, mãi
> mãi** — không phải một chi phí một lần.
>
> | Cấu hình | Cặp tính lại mỗi ngày vận hành |
> |---|---|
> | Trước khi sửa (`> max`) | 650 |
> | Lookback 3 ngày | **2.600** (gấp 4) |
> | Quét toàn bộ (bỏ incremental) | 9.100 (gấp 14) |
>
> Đây chính là sự đánh đổi cần cân nhắc: lookback 3 ngày tốn gấp 4 lần công tính so với hiện tại,
> nhưng vẫn rẻ hơn 3,5 lần so với việc vứt bỏ tính incremental và quét lại toàn bộ lịch sử. Và cái
> giá đó mua lại 455 cặp dữ liệu vốn đang bị mất vĩnh viễn.
>
> Điểm mấu chốt: chi phí này **cộng dồn theo thời gian**. Với 14 ngày dữ liệu thì gấp 4 nghe không
> đáng kể, nhưng đây là quyết định sẽ chạy mỗi ngày trong nhiều năm — nên nó phải dựa trên số đo,
> không dựa trên cảm tính "cứ để rộng cho chắc".

**4. Khi window mở rộng, cần thêm gì vào `config()`? Grain này có mấy cột khoá?**

> Grain là **1 hàng / 1 cặp `(event_date, customer_id)`** — hai cột, đúng như chú thích đầu file
> model ghi. Nên `unique_key` phải là **danh sách hai cột**: `['event_date', 'customer_id']`, kèm
> `incremental_strategy = 'merge'`.
>
> Tôi đã kiểm chứng hậu quả của việc quên ý này bằng thực nghiệm — nới lookback 3 ngày nhưng giữ
> nguyên `config()` gốc:
>
> | | Tổng hàng | Cặp phân biệt |
> |---|---|---|
> | Lượt chạy 1 | **38.152** | 9.100 |
> | Lượt chạy 2 | **74.552** | 9.100 |
>
> Cột "cặp phân biệt" cho thấy **cửa sổ 3 ngày là đúng** — nó thu hồi đủ cả 9.100 cặp. Nhưng thiếu
> khoá thì mỗi cặp bị ghi lại nhiều lần: bảng phình lên 38.152 hàng ngay ở lượt đầu (vì 14 ngày vận
> hành trong một lượt `make pipeline` đã có cửa sổ chồng lấn nhau), rồi gấp đôi ở lượt sau.
>
> Nói cách khác: **lookback sửa tính đúng, khoá composite sửa tính ổn định.** Hai thay đổi giải
> quyết hai vấn đề khác nhau và không thay thế được cho nhau.

**5. Vì sao căn cứ vào P99 chứ không phải `max`? Chi phí của mỗi lựa chọn là gì?**

> `max` là một quan sát **đơn lẻ**, không có tính thống kê. Nó bị quyết định bởi đúng một bản ghi
> cực đoan: chỉ cần một sự kiện lạc trễ 30 ngày là cửa sổ phải mở 30 ngày, và cái giá đó — theo
> bảng ở câu 3 — phải trả ở **mọi lượt chạy về sau, vĩnh viễn**, để phục vụ một bản ghi duy nhất.
> Tệ hơn, `max` không ổn định: nó có thể nhảy vọt bất cứ lúc nào một sự cố mạng đơn lẻ xảy ra,
> khiến chi phí hệ thống bị điều khiển bởi nhiễu.
>
> P99 chấp nhận bỏ sót 1% cực đoan để đổi lấy một chi phí **ổn định và dự đoán được**. Nó là phát
> biểu về *phân bố*, nên ít nhạy với ngoại lệ.
>
> Ở bộ dữ liệu này P99 (2,73) và max (2,94) gần nhau nên cả hai đều quy về 3 ngày — kết quả trùng
> nhau. Nhưng **nguyên tắc chọn thì khác nhau**, và đó mới là thứ đáng mang sang hệ thống khác:
> lookback phải là một quyết định dựa trên phân bố, không phải phản ứng với ngoại lệ.
>
> Cần nói rõ cả chiều ngược lại, vì đây là đánh đổi hai phía: cửa sổ **quá hẹp** thì dữ liệu mất
> **im lặng** — đúng như sự cố đang xử lý, không có cảnh báo nào bắt được. Cửa sổ **quá rộng** thì
> chỉ tốn thêm tài nguyên, và tài nguyên là thứ đo được, có ngân sách, có thể tối ưu dần. Khi phải
> chọn sai một phía, sai về phía rộng an toàn hơn nhiều — nhưng "an toàn hơn" không có nghĩa là
> "không cần đo".

---

## 4 · Khắc phục

> ⚠️ **Hai thay đổi phải đi cùng nhau** — mục 3 câu 4 đã chứng minh bằng thực nghiệm rằng thiếu
> một trong hai thì bảng hoặc sai số hàng, hoặc mất ổn định.

| File | Thay đổi |
|---|---|
| `dbt/models/gold/gold_feature_daily.sql` | **(1) điều kiện `is_incremental()`:** `where event_date > (select max(event_date) from {{ this }})` → `where event_date >= (select max(event_date) - interval 3 day from {{ this }})` |
| | **(2) `config()`:** thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` |

Diff thực tế:

```diff
--- a/dbt/models/gold/gold_feature_daily.sql
+++ b/dbt/models/gold/gold_feature_daily.sql
 {{ config(
-    materialized     = 'incremental',
-    on_schema_change = 'fail'
+    materialized         = 'incremental',
+    unique_key           = ['event_date', 'customer_id'],
+    incremental_strategy = 'merge',
+    on_schema_change     = 'fail'
 ) }}

 {% if is_incremental() %}
-where event_date > (select max(event_date) from {{ this }})
+where event_date >= (select max(event_date) - interval 3 day from {{ this }})
 {% endif %}
```

**Xác nhận fix có tác dụng** — đọc SQL dbt sinh ra sau khi sửa:

```bash
head -14 dbt/target/run/lab17/models/gold/gold_feature_daily.sql
grep "event_date >=" dbt/target/compiled/lab17/models/gold/gold_feature_daily.sql
```

**Output:**

```sql
    MERGE INTO "warehouse"."main"."gold_feature_daily" AS DBT_INTERNAL_DEST
        USING "gold_feature_daily__dbt_tmp20260817224841216964" AS DBT_INTERNAL_SOURCE
        ON (DBT_INTERNAL_SOURCE.event_date  = DBT_INTERNAL_DEST.event_date)
       AND (DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id)

where event_date >= (select max(event_date) - interval 3 day
                     from "warehouse"."main"."gold_feature_daily")
```

Điều kiện `ON` khớp trên **cả hai** cột — đúng grain của bảng.

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
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8622572a97    8622572a97    8622572a97   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓

  TỔNG KẾT
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✗  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  3/4 tiêu chí đạt
```

| Chỉ số | Trước | Sau |
|---|---|---|
| Số hàng | 8.645 | **9.100** ✓ |
| Cặp bị thiếu so với Silver | 455 | **0** |
| Checksum 3 lượt | `3269dbe574` ×3 | `f8d3f591f0` ×3 ✓ |
| Ổn định | ✓ ok | **✓ ok** (giữ nguyên) |
| **Phiếu #1041 còn nguyên?** | — | **☑** 12.480 · `8622572a97` ×3 không đổi |
| Tổng kết verify | 2/4 | **3/4** |

*(`gold_doc_chunks` giữ nguyên checksum `92d8e50131` — nhóm đối chứng không bị ảnh hưởng.)*

### Kiểm chứng sâu hơn mức verify yêu cầu

```bash
make pipeline && q "select count(*) as tong, count(distinct (event_date,customer_id)) as cap from gold_feature_daily"
make pipeline && q "select count(*) as tong, count(distinct (event_date,customer_id)) as cap from gold_feature_daily"
```

**Output:**

```
=== LƯỢT 4 ===              === LƯỢT 5 ===
┌───────┬───────┐           ┌───────┬───────┐
│ tong  │  cap  │           │ tong  │  cap  │
├───────┼───────┤           ├───────┼───────┤
│  9100 │  9100 │           │  9100 │  9100 │
└───────┴───────┘           └───────┴───────┘
```

Cột `tong` bằng đúng cột `cap` — không có hàng trùng nào, khác hẳn hai thí nghiệm ở mục 3.

Và quan trọng nhất — kiểm tra chính cặp đã lần theo ở mục 2.6:

```bash
q "select event_date, customer_id, n_events, n_tickets from gold_feature_daily
   where customer_id='C0004' and event_date=date '2026-08-12'"

q "select count(*) as van_con_thieu from (
     select distinct s.event_date, s.customer_id from silver_events s
     left join gold_feature_daily g on g.event_date=s.event_date and g.customer_id=s.customer_id
     where g.customer_id is null)"
```

**Output:**

```
┌────────────┬─────────────┬──────────┬───────────┐
│ event_date │ customer_id │ n_events │ n_tickets │
├────────────┼─────────────┼──────────┼───────────┤
│ 2026-08-12 │ C0004       │       11 │        11 │
└────────────┴─────────────┴──────────┴───────────┘

┌───────────────┐
│ van_con_thieu │
├───────────────┤
│             0 │
└───────────────┘
```

Cặp `(08-12, C0004)` đã có mặt với **đúng 11 sự kiện** — khớp chính xác 11 bản ghi tìm thấy ở mục
2.6. Và trên toàn bảng, số cặp thiếu từ **455 xuống 0**.

---

## 6 · Nguyên nhân — câu viết cho báo cáo

> Điều kiện lọc incremental so `event_date` với `max(event_date)` của **chính bảng đích** — một đại
> lượng chỉ tăng đơn điệu — nên nó ngầm giả định dữ liệu luôn tới kho theo đúng thứ tự thời gian
> sự kiện xảy ra. Thực đo cho thấy 15,91% bản ghi vi phạm giả định đó, với P99 độ trễ ingest là
> 2,73 ngày. Hệ quả: một cặp `(event_date, customer_id)` mà **toàn bộ** sự kiện đều tới muộn sẽ
> không tồn tại trong bảng đích ở ngày vận hành của nó, và khi dữ liệu tới thì `max(event_date)` đã
> vượt qua — mốc so sánh chỉ tiến về phía trước nên cặp đó **không bao giờ** được xử lý ở bất kỳ
> lượt chạy nào sau đó. Đúng 455 cặp rơi vào tình trạng này, và vì sai lệch được tái tạo y hệt ở
> mọi lượt chạy nên bảng vẫn báo `ỔN ĐỊNH ✓` — dữ liệu mất hoàn toàn im lặng.

---

## 7 · Phòng ngừa tái diễn

Phép kiểm tra rẻ nhất lẽ ra nên chạy từ đầu để phát hiện sự cố này:

> **Với mỗi model incremental, kiểm tra mốc lọc lấy từ đại lượng nào: thời điểm sự kiện *xảy ra*
> (event-time) hay thời điểm dữ liệu *tới kho* (ingestion-time).** Nếu mốc là event-time mà không
> có lookback, đó là lỗi chờ xảy ra — chỉ cần đo phân bố `_ingested_at − event_time` và đối chiếu
> P99 với độ rộng cửa sổ là biết ngay hệ thống đang đánh rơi bao nhiêu phần trăm dữ liệu.
>
> Ở mức hệ thống, cần một phép kiểm tra **đối soát** chứ không chỉ kiểm tra tính ổn định: định kỳ
> `left join` từ tầng nguồn xuống tầng đích để đếm số bản ghi có ở nguồn mà thiếu ở đích. Đây chính
> là query đã dùng ở mục 2.4, và nó phát hiện ra sự cố trong vài giây — trong khi `dbt test`,
> checksum và mọi cảnh báo hiện có đều báo xanh suốt.
>
> Bài học chung: **tính ổn định không phải tính đúng**. Một pipeline tất định vẫn có thể sai một
> cách tất định. Kiểm thử phải nhắm vào *bất biến đối chiếu với nguồn*, không chỉ nhắm vào *kết quả
> có lặp lại được hay không*.
