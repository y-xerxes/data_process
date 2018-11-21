from datetime import datetime, timedelta

def name_with_prefix(prefix, name, seperator="_"):
    if prefix is None:
        return name
    elif prefix == "None":
        return name
    else:
        return "{0}{1}{2}".format(prefix, seperator, name)