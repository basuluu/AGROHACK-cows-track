import event_categories
import pandas as pd

def digitize_event_category_col(dt):
    dt_copy = dt.copy()
    dt_copy['Событие'] = pd.Series(map(lambda n: event_categories.EVENTS[n], list(dt_copy['Событие'])))
    return dt_copy

def convert_to_datetime(dt):
    dt_copy = dt.copy()
    dt_copy['Дата рождения'] = pd.to_datetime(dt_copy['Дата рождения'], format="%m/%d/%Y").dt.date
    dt_copy['Дата события'] = pd.to_datetime(dt_copy['Дата события'], format="%m/%d/%Y").dt.date
    return dt_copy
    