def deep_getattr(obj, path, default=None):
    if not obj:
        return default

    keys = str(path).split(".")

    while keys:
        key = keys.pop(0)
        obj = getattr(obj, key, None)

        if obj is None:
            return default

    return obj

