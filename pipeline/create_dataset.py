import pandas as pd

from datetime import timedelta

import event_categories
import prepare_data
import filter_rows


# Функция возвращает все события в интервале
# между событием А и результирующим событием
# например для мастита между МАСТИТ и (здоров, продан, помер)
# [{
#   "rows": dataframe, события интервала включая мастит и результирующее
#   "is_positive_result": bool, True если здоров, False если плохой исход
#   "start_event_row_index": int, индекс строки болезни которую будем
#                                 насыщать информацией и доп колонками
#   "finish_event_type": int, каким ивентом закончилось,
#                             можно будет разбить колонку исхода
# },]
def get_intervals_between_event_and_result_event(
    df,
    start_event,
    positive_note=[],
    negative_note=[],
    positive_result_events=[],
    negative_result_events=[],
    max_days_between=100,
    include_finish_event=True,
):
    df = df[["Номер животного", "Дата события", "Событие", "Примечание события"]].copy()
    intervals = []

    result_events_types = positive_result_events + negative_result_events

    for cow_id in df["Номер животного"].unique():
        df_cow_rows = df[df["Номер животного"] == cow_id].copy()
        df_cow_rows[df_cow_rows["Событие"] == start_event]
        event_dates = df_cow_rows[df_cow_rows["Событие"] == start_event][
            "Дата события"
        ].values

        finish_event_date = None
        for event_date in event_dates:
            if finish_event_date is not None and event_date <= finish_event_date:
                continue

            opened_range_date = event_date
            solved_range_date = event_date + timedelta(days=max_days_between)
            rows_in_date_range = df_cow_rows[
                (df_cow_rows["Дата события"] >= opened_range_date)
                & (df_cow_rows["Дата события"] <= solved_range_date)
            ]

            result_events = rows_in_date_range[
                df_cow_rows["Событие"].isin(result_events_types)
            ].sort_values("Дата события", ascending=True)

            finish_event_row = None
            for index, row in result_events.iterrows():
                if row["Событие"] in positive_result_events:
                    if positive_note:
                        if row["Примечание события"].upper() in positive_note:
                            finish_event_row = row
                            break
                    else:
                        finish_event_row = row
                        break
                elif row["Событие"] in negative_result_events:
                    finish_event_row = row
                    break

            if finish_event_row is None:
                continue

            finish_event_type = finish_event_row["Событие"]
            finish_event_date = finish_event_row["Дата события"]

            if include_finish_event:
                rows_in_events_range = rows_in_date_range[
                    rows_in_date_range["Дата события"] <= finish_event_date
                ]
            else:
                rows_in_events_range = rows_in_date_range[
                    rows_in_date_range["Дата события"] < finish_event_date
                ]

            # Если все в один произошло, выкинуть
            if len(rows_in_events_range["Дата события"].unique()) == 1:
                break

            # Чтобы убрать ивенты которые произошли в тот же день, но раньше события Y
            # intervals строго начинается со строки события, на это же событие будет навешиваться остальное
            while rows_in_events_range.head(1)["Событие"].values[0] != start_event:
                rows_in_events_range = rows_in_events_range.iloc[1:, :]

            while (
                rows_in_events_range.tail(1)["Событие"].values[0]
                not in result_events_types
            ):
                rows_in_events_range = rows_in_events_range.iloc[:-1, :]

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

    return [dig for dig in udder_parts_note if dig.isdigit()]


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
        udder_parts_affected = []
        for index, row in interval["rows"].iterrows():
            note = row["Примечание события"]
            for protocol in possible_protocols:
                if protocol in note:
                    udder_parts_affected.append(get_udder_parts_affected(note))
                    protocol_history.append(protocol)
                    break

        event_row_index = interval["start_event_row_index"]
        for i, protocol in enumerate(protocol_history):
            protocol_id = event_categories.TREATMENT_PROTOCOLS[protocol]
            dataset.at[event_row_index, f"PROTOCOL_STAGE_{i+1}"] = protocol_id

        for i, udder_parts in enumerate(udder_parts_affected):
            if i != 0:
                previous_parts = set(udder_parts_affected[i - 1])
                current_parts = set(udder_parts)

                dataset.at[event_row_index, f"UDDER_PARTS_CURED_STAGE_{i+1}"] = len(
                    previous_parts - current_parts
                )
            else:
                dataset.at[event_row_index, f"UDDER_PARTS_CURED_STAGE_{i+1}"] = 0
            dataset.at[event_row_index, f"UDDER_PARTS_AFFECTED_STAGE_{i+1}"] = len(
                udder_parts
            )
    return dataset


def add_empty_protocol_columns(df):
    possible_categories = get_mastit_possible_protocols()
    df[
        [
            "PROTOCOL_STAGE_1",
            "PROTOCOL_STAGE_2",
            "PROTOCOL_STAGE_3",
            "PROTOCOL_STAGE_4",
            "PROTOCOL_STAGE_5",
            "PROTOCOL_STAGE_6",
            "PROTOCOL_STAGE_7",
            "PROTOCOL_STAGE_8",
            "PROTOCOL_STAGE_9",
            "PROTOCOL_STAGE_10",
            "PROTOCOL_STAGE_11",
        ]
    ] = 0
    return df


def add_empty_udder_parts_number_column(df):

    df[
        [
            "UDDER_PARTS_AFFECTED_STAGE_1",
            "UDDER_PARTS_CURED_STAGE_1",
            "UDDER_PARTS_AFFECTED_STAGE_2",
            "UDDER_PARTS_CURED_STAGE_2",
            "UDDER_PARTS_AFFECTED_STAGE_3",
            "UDDER_PARTS_CURED_STAGE_3",
            "UDDER_PARTS_AFFECTED_STAGE_4",
            "UDDER_PARTS_CURED_STAGE_4",
            "UDDER_PARTS_AFFECTED_STAGE_5",
            "UDDER_PARTS_CURED_STAGE_5",
            "UDDER_PARTS_AFFECTED_STAGE_6",
            "UDDER_PARTS_CURED_STAGE_6",
            "UDDER_PARTS_AFFECTED_STAGE_7",
            "UDDER_PARTS_CURED_STAGE_7",
            "UDDER_PARTS_AFFECTED_STAGE_8",
            "UDDER_PARTS_CURED_STAGE_8",
            "UDDER_PARTS_AFFECTED_STAGE_9",
            "UDDER_PARTS_CURED_STAGE_9",
            "UDDER_PARTS_AFFECTED_STAGE_10",
            "UDDER_PARTS_CURED_STAGE_10",
            "UDDER_PARTS_AFFECTED_STAGE_11",
            "UDDER_PARTS_CURED_STAGE_11",
        ]
    ] = 0
    return df


def add_column_days_of_treatment(dataset, intervals):
    for interval in intervals:
        start_date = interval["rows"].head(1)["Дата события"].values[0]
        finish_date = interval["rows"].tail(1)["Дата события"].values[0]

        event_row_index = interval["start_event_row_index"]
        days_of_treatment = (finish_date - start_date).days

        # Ошибка. Слишком раннее завершение протокола.
        # Если далее (8 дней от 12.03)
        # животное не имеет мастита - ошибочно событие от 12.03.
        if days_of_treatment < 8 and interval["is_positive_result"] == True:
            dataset = dataset.drop(event_row_index)
        else:
            dataset.at[event_row_index, "DAYS_OF_TREATMENT"] = days_of_treatment

    dataset["DAYS_OF_TREATMENT"] = dataset["DAYS_OF_TREATMENT"].astype("int")
    return dataset


def add_column_cow_full_years(dataset):
    for index, row in dataset.iterrows():
        full_years = (row["Дата события"] - row["Дата рождения"]).days // 365
        dataset.at[index, "COW_FULL_YEARS"] = full_years
    dataset["COW_FULL_YEARS"] = dataset["COW_FULL_YEARS"].astype("int")
    return dataset


def add_columns_about_cow_relatives(dataset):
    relatives_data = pd.read_csv("../Предки utf8.csv", sep=";")

    relatives_data = relatives_data.fillna("0")
    relatives_data = relatives_data.replace("nan", "0")
    relatives_data = relatives_data.replace("", "0")
    relatives_data = relatives_data.replace("Итого:25498", "0")
    relatives_data = relatives_data.replace("-         ", "0")

    relatives_data["Номер Матери"] = relatives_data["Номер Матери"].astype("int")
    relatives_data["Номер животного"] = relatives_data["Номер животного"].astype(
        "int", errors="ignore"
    )

    dataset["MOTHER_ID"] = 0
    dataset["FATHER_ID"] = ""
    dataset["MOTHER_FATHER_ID"] = ""

    for index, row in dataset.iterrows():
        father_id = ""
        mother_id = 0
        mother_father_id = ""

        cow_id = row["Номер животного"]
        if relatives_data[relatives_data["Номер животного"] == cow_id].empty:
            if not relatives_data[relatives_data["Номер Матери"] == cow_id].empty:
                potential_father_id = list(
                    relatives_data[relatives_data["Номер Матери"] == cow_id][
                        "Отец Матери"
                    ].unique()
                )
                father_id = potential_father_id[0]
        else:
            for _, rel_row in relatives_data[
                relatives_data["Номер животного"] == cow_id
            ].iterrows():
                mother_id = rel_row["Номер Матери"]
                father_id = rel_row["Код Отца"]
                mother_father_id = rel_row["Отец Матери"]

        dataset.at[index, "MOTHER_ID"] = mother_id
        dataset.at[index, "FATHER_ID"] = father_id
        dataset.at[index, "MOTHER_FATHER_ID"] = mother_father_id
    return dataset


def add_column_is_cow_on_hormones(df, gormoni_intervals):
    df["IS_COW_ON_HORMONES"] = 0
    for interval in gormoni_intervals:
        for row_index in interval["rows"].index:
            df.at[row_index, "IS_COW_ON_HORMONES"] = 1
    return df


def main(df):
    GORMONI_POSITIVE_RESULT_EVENTS = [
        event_categories.EVENTS["СТЕЛН"],
        event_categories.EVENTS["СО_СХЕМЫ"],
    ]

    MASTIT_POSITIVE_NOTE = ["МАСТИТ", "МАТСТИТ", "МАСТИТ", "МАСТИТ3", "МАСТ"]
    MASTIT_POSITIVE_RESULT_EVENTS = event_categories.EVENTS_BY_CATEGORIES[
        "ЛЕЧЕНИЕ ПОМОГЛО"
    ]
    MASTIT_NEGATIVE_RESULT_EVENTS = event_categories.EVENTS_BY_CATEGORIES["ИСХОД"]

    gormoni_intervals = get_intervals_between_event_and_result_event(
        df,
        event_categories.EVENTS["НА_СХЕМУ"],
        positive_result_events=GORMONI_POSITIVE_RESULT_EVENTS,
        max_days_between=200,
    )
    df = add_column_is_cow_on_hormones(df, gormoni_intervals)

    mastit_intervals = get_intervals_between_event_and_result_event(
        df,
        event_categories.EVENTS["МАСТИТ"],
        positive_note=MASTIT_POSITIVE_NOTE,
        positive_result_events=MASTIT_POSITIVE_RESULT_EVENTS,
        negative_result_events=MASTIT_NEGATIVE_RESULT_EVENTS,
        max_days_between=100,
        include_finish_event=True,
    )
    indexes = get_event_row_indexes(mastit_intervals)
    dataset = filter_rows.filter_rows_by_indexes(df, indexes)

    dataset = add_empty_protocol_columns(dataset)
    dataset = add_empty_udder_parts_number_column(dataset)
    dataset = fill_protocol_and_udder_parts_columns(dataset, mastit_intervals)
    dataset = add_column_days_of_treatment(dataset, mastit_intervals)
    dataset = add_column_cow_full_years(dataset)
    dataset = add_columns_about_cow_relatives(dataset)
    dataset.to_csv("test_protocols_order.csv")


if __name__ == "__main__":
    main(prepare_data.get_source_data())
