import pandas as pd

from datetime import timedelta

import event_categories


def filer_by_days_before_after_event(df, event, days_before, days_after):
    rows_indexes_in_event_range = set()
    for cow_id in df["Номер животного"].unique():
        df_cow_rows = df[df["Номер животного"] == cow_id].copy()
        df_cow_rows[df_cow_rows["Событие"] == event]
        event_dates = df_cow_rows[df_cow_rows["Событие"] == event][
            "Дата события"
        ].values
        for event_date in event_dates:
            opened_range_date = event_date - timedelta(days=days_before)
            solved_range_date = event_date + timedelta(days=days_after)
            rows_in_range = df_cow_rows[
                (df_cow_rows["Дата события"] >= opened_range_date)
                & (df_cow_rows["Дата события"] <= solved_range_date)
            ]
            rows_indexes_in_event_range.update(set(rows_in_range.index))
    return df[df.index.isin(rows_indexes_in_event_range)].copy()

def filter_rows_by_indexes(df, indexes):
    return df[df.index.isin(indexes)].copy()
