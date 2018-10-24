def process_data(*args, **kwargs):
    """Process data - sum or concatenate all arguments.
    """
    result = None
    for arg in args:
        if result:
            result += arg
        else:
            result = arg
    return result
