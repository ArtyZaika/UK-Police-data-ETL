import re

def check_date_month(date_month: str):
    pattern = r'[0-3]{4}-[0-9]{2}'
    match = re.fullmatch(pattern, date_month)

    if match is not None:
        return True
    return False