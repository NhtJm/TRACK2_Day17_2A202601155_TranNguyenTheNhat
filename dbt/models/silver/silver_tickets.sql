-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- Model này materialized = 'table': dựng lại toàn bộ mỗi lần chạy, nên luôn
-- ổn định.
--
-- Phần xử lý CDC dưới đây đã đúng và không cần sửa:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất  -> _rn = 1
--   * ticket có op = 'd' bị loại khỏi Silver
--
-- ===========================================================================
-- NHIỆM VỤ 3 — phần 2/3
-- ===========================================================================
-- Giá trị `priority` được chuẩn hoá bằng macro
-- `dbt/macros/normalize_priority.sql` — sửa logic quy đổi ở đó, không sửa ở
-- đây. Việc còn lại của file này là **loại bỏ những bản ghi không chuẩn hoá
-- được** ra khỏi Silver.
--
-- ⚠️ THỨ TỰ QUYẾT ĐỊNH SỐ HÀNG CỦA BẢNG.
--
--    Hiện tại: xếp hạng (row_number) trước, rồi mới chọn.
--    Nếu bạn chỉ thêm điều kiện lọc vào cuối, ticket nào có bản ghi MỚI NHẤT
--    bị hỏng sẽ biến mất hoàn toàn khỏi Silver — số ticket tụt từ 12.480
--    xuống 12.168, và gold_training_set hụt theo.
--
--    Đúng phải là: LỌC bỏ bản ghi hỏng trước → SAU ĐÓ mới xếp hạng.
--    Bạn loại BẢN GHI hỏng, không loại cả TICKET: ticket đó vẫn còn trạng
--    thái hợp lệ từ lần cập nhật trước.
--
--    `make verify` có một dòng riêng ("silver_tickets giữ đủ 12.480 ticket")
--    để bắt đúng lỗi này.
--
-- Xong file này thì sang models/silver/quarantine_tickets.sql (phần 3/3).
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

with normalized as (

    select
        *,
        {{ normalize_priority('priority_raw') }}             as priority_clean
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

-- Loại BẢN GHI không chuẩn hoá được, TRƯỚC khi xếp hạng.
--
-- Nếu lọc SAU row_number(), ticket nào có bản ghi mới nhất bị hỏng sẽ biến mất
-- hoàn toàn khỏi Silver: đo được 12.480 -> 12.168, mất đúng 312 ticket. Tệ hơn,
-- gold_training_set vẫn giữ 312 hàng mồ côi mang priority cũ đã lỗi thời, nên
-- số hàng Gold vẫn đúng 12.480 và lỗi bị che giấu hoàn toàn.
--
-- Lọc TRƯỚC thì ta loại BẢN GHI hỏng chứ không loại cả TICKET — ticket đó lùi
-- về trạng thái hợp lệ của lần cập nhật gần nhất.
valid_records as (

    select * from normalized where priority_clean is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid_records

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority_clean                                           as priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
