#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — bài mở rộng A (phiếu #1052).

Hiện trạng ban đầu: `data/gold_events/` gồm 5.000 file, mỗi file ~26 hàng, không
partition, thứ tự hàng ngẫu nhiên. Truy vấn dashboard vì thế quét 5.000.000 hàng
để trả về 3.500 — 97,4% công quét là chi phí mở file.

Đã hiện thực: đọc toàn bộ dataset cũ, ghi ra `data/gold_events_v2` theo layout
Hive partition, và `queries/dashboard.sql` đã trỏ sang dataset mới.
Kết quả đo được: rows scanned 5.000.000 -> 9.324 (536,3x), files 5.000 -> 14,
result hash không đổi.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# ---------------------------------------------------------------------------
# BA QUYẾT ĐỊNH — mỗi cái kèm số đo, không chọn theo cảm tính.
#
# (1) PARTITION_BY (event_date)
#     Dashboard lọc theo customer_name VÀ event_date. Chỉ một trong hai nên
#     nằm trong đường dẫn thư mục, vì đó là thông tin engine đọc được TRƯỚC khi
#     mở file. Đo cả hai phương án:
#
#         partition theo customer_name  -> 650 thư mục,  49.000 rows scanned
#         partition theo event_date     ->  14 thư mục,   9.324 rows scanned
#
#     event_date có 14 giá trị phân biệt (~9.334 hàng/ngày) nên mỗi file vẫn
#     lành mạnh. customer_name có 650 giá trị, mỗi thư mục chỉ ~201 hàng —
#     tức tái lập đúng small-file problem đang cần chữa, và quét gấp 5,3 lần.
#
# (2) ORDER BY event_date, customer_name
#     Đo được: KHÔNG làm thay đổi rows scanned (9.324 ở cả hai trường hợp) —
#     xem ghi chú ở (3). Lợi ích thật nằm ở chỗ khác: dữ liệu đã sắp xếp nén
#     tốt hơn nhờ các giá trị giống nhau nằm liền kề.
#
#         không sắp xếp                  -> 4.611 KB
#         sắp xếp event_date, customer   -> 3.884 KB   (giảm 16%)
#
# (3) ROW_GROUP_SIZE 2000
#     Mặc định 122.880 gói trọn một ngày (~9.334 hàng) vào MỘT row group, nên
#     min/max của nó phủ toàn bộ 650 khách hàng và vô dụng cho filter.
#     Giá trị 2000 chia mỗi ngày thành 5 row group.
#
#     Lưu ý trung thực: metric `rows scanned` mà tools/explain.py đo KHÔNG phản
#     ứng với row-group pruning — đo thử 122.880 / 5.000 / 2.000 / 500 / 100
#     đều cho đúng 9.324. Nó chỉ phản ứng với partition pruning ở mức file.
#     Vẫn giữ 2000 vì đó là layout đúng cho các truy vấn lọc hẹp hơn, và vì
#     DuckDB có sàn ~2.048 hàng/row group nên đặt thấp hơn cũng không đổi gì.
# ---------------------------------------------------------------------------

PARTITION_COL = "event_date"
ORDER_BY = "event_date, customer_name"
ROW_GROUP_SIZE = 2_000


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    src_rows = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC}/*.parquet')
            order by {ORDER_BY}
        ) to '{DST}' (
            format          parquet,
            partition_by    ({PARTITION_COL}),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    dst_rows = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet')"
    ).fetchone()[0]
    con.close()

    n_dst = len(list(DST.glob("**/*.parquet")))

    # Không được mất hàng nào: nén lại là bài toán bố trí, không phải bài toán lọc.
    assert src_rows == dst_rows, f"mất hàng khi nén: {src_rows:,} -> {dst_rows:,}"

    print(f"  đích  : {DST}  ({n_dst:,} file)")
    print(f"  hàng  : {src_rows:,} -> {dst_rows:,}  ✓ không mất hàng nào")
    print("\n  Bước tiếp theo: python tools/explain.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
