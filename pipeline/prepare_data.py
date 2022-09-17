import pandas as pd

import event_categories

def digitize_event_category_col(dt):
    dt_copy = dt.copy()
    dt_copy['Событие'] = pd.Series(map(lambda n: event_categories.EVENTS[n], list(dt_copy['Событие'])))
    return dt_copy

def convert_to_datetime(dt):
    dt_copy = dt.copy()
    dt_copy['Дата рождения'] = pd.to_datetime(dt_copy['Дата рождения'], format='%d.%m.%Y').dt.date
    dt_copy['Дата события'] = pd.to_datetime(dt_copy['Дата события'], format='%d.%m.%Y').dt.date
    return dt_copy
    
def sort_by_event_date(dt):
    dt_copy = dt.copy()
    dt_copy = dt_copy.sort_values("Дата события", ascending=True)
    return dt_copy

def get_source_data():
    data = pd.read_csv("../Дамп событий 2019-2022 utf8.csv", on_bad_lines='skip', sep=";")
    data = convert_to_datetime(data)
    data = digitize_event_category_col(data)
    data = sort_by_event_date(data)
    return data