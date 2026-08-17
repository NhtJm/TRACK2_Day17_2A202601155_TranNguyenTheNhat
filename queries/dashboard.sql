-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- Dataset đã được tools/compact.py bố trí lại: partition theo event_date theo
-- layout Hive, trong mỗi file các hàng sắp theo customer_name.
--
--   hive_partitioning = 1   -> event_date đọc được từ TÊN THƯ MỤC, nên engine
--                              loại 13/14 partition mà không mở file nào.
--
--   event_date = DATE '…'   -> cột đứng một mình một vế (sargable). Dạng cũ
--                              strftime(event_time, '%Y-%m-%d') = '…' bọc cột
--                              trong một function call, nên engine không so
--                              được kết quả với tên thư mục partition lẫn
--                              min/max của row group — buộc phải mở toàn bộ
--                              5.000 file rồi mới biết file nào có ích.
--
-- Ngữ nghĩa không đổi: event_date được sinh từ chính event_time (event_time::date),
-- nên hai điều kiện chọn đúng cùng một tập hàng. tools/explain.py kiểm chứng
-- bằng hash của kết quả.
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = 1)
where customer_name = 'ACME'
  and event_date = DATE '2026-08-09'
group by 1
