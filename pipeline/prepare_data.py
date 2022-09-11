import event_categories
import pandas as pd

def digitize_event_category_col(dt):
    dt_copy = dt.copy()
    dt_copy['Событие'] = pd.Series(map(lambda n: event_categories.EVENTS[n], list(dt_copy['Событие'])))
    return dt_copy
