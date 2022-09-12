import pandas as pd
from datetime import timedelta


def filer_by_days_before_after_event(dt, event, days_before, days_after):
    rows_indexes_in_event_range = set()
    for cow_id in dt["Номер животного"].unique():
        dt_cow_rows = dt[dt["Номер животного"] == cow_id].copy()
        dt_cow_rows[dt_cow_rows["Событие"] == event]
        event_dates = dt_cow_rows[dt_cow_rows["Событие"] == event][
            "Дата события"
        ].values
        for event_date in event_dates:
            opened_range_date = event_date - timedelta(days=days_before)
            solved_range_date = event_date + timedelta(days=days_after)
            rows_in_range = dt_cow_rows[
                (dt_cow_rows["Дата события"] >= opened_range_date)
                & (dt_cow_rows["Дата события"] <= solved_range_date)
            ]
            rows_indexes_in_event_range.update(set(rows_in_range.index))
    return dt[dt.index.isin(rows_indexes_in_event_range)].copy()
