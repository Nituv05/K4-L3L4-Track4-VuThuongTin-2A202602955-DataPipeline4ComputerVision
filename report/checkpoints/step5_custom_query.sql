select drive_id, trip_id, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 2) as dropped_pct,
       avg_speed_kmh, split
from marts.mart_trip_summary
order by dropped_pct desc, frames desc, trip_id;
