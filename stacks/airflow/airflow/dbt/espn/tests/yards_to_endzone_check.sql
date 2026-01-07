select *
from {{ ref('stg_espn_play') }}
where start_yard_line > 50
or end_yard_line > 50
or start_yard_line < 0
or end_yard_line < 0