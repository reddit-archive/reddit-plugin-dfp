def merge(obj, *sources):
    if not sources:
        return obj

    for source in sources:
        for prop, value in source.iteritems():
            if value is not None:
                obj[prop] = value

    return obj

def merge_deep(obj, *sources):
    if not sources:
        return obj

    for source in sources:
        for prop, value in source.iteritems():
            if value is None:
                continue
            elif (isinstance(value, dict) and
                    isinstance(obj[prop], dict)):
                obj[prop] = merge_deep(obj[prop], value)
            else:
                obj[prop] = value

    return obj

