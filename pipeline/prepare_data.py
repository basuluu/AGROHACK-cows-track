import event_categories

def digitize_event_category_col(dt):
    dt_copy = dt.copy()
    dt_copy['Событие'] = pd.Series(map(lambda n: EVENTS[n], list(dt_copy['Событие'])))
    return dt_copy
