
from datetime import datetime


def timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def timestamp_filename():
    return datetime.now().strftime('%Y%m%d%H%M%S')
