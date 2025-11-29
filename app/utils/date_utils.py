import datetime

def get_current_roc_year():
    """取得民國當年"""
    return datetime.datetime.now().year - 1911
