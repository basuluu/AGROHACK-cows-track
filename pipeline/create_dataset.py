import pandas as pd

from datetime import timedelta

import event_categories
import prepare_data
import filter_rows


# Функция возвращает все события в интервале
# между Маститом и результирующим событием (здоров, продан, помер)
# [{
#   "rows": dataframe, события интервала включая мастит и результирующее
#   "is_positive_result": bool, True если здоров, False если плохой исход
#   "start_event_row_index": int, индекс строки болезни которую будем
#                                 насыщать информацией и доп колонками
#   "finish_event_type": int, каким ивентом закончилось,
#                             можно будет разбить колонку исхода
# },]
def get_intervals_between_event_and_result_event(
    df, start_event, max_days_between=100, include_finish_event=True
):
    df = df[["Номер животного", "Дата события", "Событие", "Примечание события"]].copy()
    intervals = []

    positive_result_events = event_categories.EVENTS_BY_CATEGORIES["ЛЕЧЕНИЕ ПОМОГЛО"]
    negative_result_events = event_categories.EVENTS_BY_CATEGORIES["ИСХОД"]

    for cow_id in df["Номер животного"].unique():
        df_cow_rows = df[df["Номер животного"] == cow_id].copy()
        df_cow_rows[df_cow_rows["Событие"] == start_event]
        event_dates = df_cow_rows[df_cow_rows["Событие"] == start_event][
            "Дата события"
        ].values
        for event_date in event_dates:
            opened_range_date = event_date
            solved_range_date = event_date + timedelta(days=max_days_between)
            rows_in_date_range = df_cow_rows[
                (df_cow_rows["Дата события"] >= opened_range_date)
                & (df_cow_rows["Дата события"] <= solved_range_date)
            ]

            result_events = rows_in_date_range[
                df_cow_rows["Событие"].isin(
                    positive_result_events + negative_result_events
                )
            ].sort_values("Дата события", ascending=True)

            if not result_events.shape[0]:
                continue

            finish_event = result_events.head(1)
            finish_event_type = finish_event["Событие"].values[0]
            finish_event_date = finish_event["Дата события"].values[0]

            if include_finish_event:
                rows_in_events_range = rows_in_date_range[
                    rows_in_date_range["Дата события"] <= finish_event_date
                ]
            else:
                rows_in_events_range = rows_in_date_range[
                    rows_in_date_range["Дата события"] < finish_event_date
                ]

            # Чтобы убрать ивенты которые произошли в тот же день, но раньше события Y
            # intervals строго начинается со строки события, на это же событие будет навешиваться остальное
            while rows_in_events_range.head(1)["Событие"].values[0] != start_event:
                rows_in_events_range = rows_in_events_range.iloc[1:, :]

            is_positive_result = (
                True if finish_event_type in positive_result_events else False
            )
            intervals.append(
                {
                    "rows": rows_in_events_range,
                    "is_positive_result": is_positive_result,
                    "start_event_row_index": rows_in_events_range.head(1).index.values[
                        0
                    ],
                    "start_event_type": start_event,
                    "finish_event_type": start_event,
                }
            )
    return intervals


def get_udder_parts_affected(protocol_note):
    udder_parts_note = "".join(protocol_note.split("_")[1:])
    udder_parts_note = udder_parts_note.replace(",", "")
    udder_parts_note = udder_parts_note.replace("1-4", "1234")
    udder_parts_note = udder_parts_note.replace("1_4", "1234")
    udder_parts_note = udder_parts_note.replace("2-4", "234")
    udder_parts_note = udder_parts_note.replace("2_4", "234")
    udder_parts_note = udder_parts_note.replace("1-3", "123")
    udder_parts_note = udder_parts_note.replace("1_3", "123")

    if udder_parts_note == "?" or udder_parts_note == "" or udder_parts_note == "T":
        udder_parts_note = "1234"
    udder_parts_note = udder_parts_note.replace("?", "")

    return {dig for dig in udder_parts_note if dig.isdigit()}


def get_event_row_indexes(intervals):
    return [interval["start_event_row_index"] for interval in intervals]


def get_mastit_possible_protocols():
    possible_protocols = event_categories.DESEASE_TREATMENT_PROTOCOLS[
        event_categories.EVENTS["МАСТИТ"]
    ]
    return possible_protocols


def fill_protocol_and_udder_parts_columns(dataset, intervals):
    possible_protocols = get_mastit_possible_protocols()

    for interval in intervals:
        protocol_history = []
        udder_parts_affected = set()
        for index, row in interval["rows"].iterrows():
            note = row["Примечание события"]
            for protocol in possible_protocols:
                if protocol in note:
                    udder_parts_affected.update(get_udder_parts_affected(note))
                    protocol_history.append(protocol)
                    break

        event_row_index = interval["start_event_row_index"]
        for i, protocol in enumerate(protocol_history):
            protocol_id = event_categories.TREATMENT_PROTOCOLS[protocol]
            dataset.at[event_row_index, f"PROTOCOL_TYPE_{i+1}"] = protocol_id

        dataset.at[event_row_index, "udder_parts_affected"] = len(udder_parts_affected)
    return dataset


def add_empty_protocol_columns(df):
    possible_categories = get_mastit_possible_protocols()
    df[
        [
            "PROTOCOL_TYPE_1",
            "PROTOCOL_TYPE_2",
            "PROTOCOL_TYPE_3",
            "PROTOCOL_TYPE_4",
            "PROTOCOL_TYPE_5",
            "PROTOCOL_TYPE_6",
            "PROTOCOL_TYPE_7",
            "PROTOCOL_TYPE_8",
            "PROTOCOL_TYPE_9",
            "PROTOCOL_TYPE_10",
        ]
    ] = 0
    return df


def add_empty_udder_parts_number_column(df):
    df[["udder_parts_affected"]] = 0
    return df


def main(df):
    intervals = get_intervals_between_event_and_result_event(
        df, event_categories.EVENTS["МАСТИТ"]
    )
    indexes = get_event_row_indexes(intervals)
    dataset = filter_rows.filter_rows_by_indexes(df, indexes)

    dataset = add_empty_protocol_columns(dataset)
    dataset = add_empty_udder_parts_number_column(dataset)
    dataset = fill_protocol_and_udder_parts_columns(dataset, intervals)
    dataset.to_csv("test_protocols_order.csv")


if __name__ == "__main__":
    main(prepare_data.get_source_data())
