from datetime import datetime, timedelta

def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = datetime.today().replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
    return today - timedelta(days=n)


def name_with_prefix(prefix, name, separator="_"):
    if prefix is None:
        return name
    elif prefix == "None":
        return name
    else:
        return "{0}{1}{2}".format(prefix, separator, name)


def prefixed_name(name, prefix):
    if prefix is None or prefix == "None" or prefix == "aliyun01":
        return name_with_prefix(None, name)
    else:
        return name_with_prefix(prefix, name)
