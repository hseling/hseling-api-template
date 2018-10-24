from numbers import Real


def validate_data(*args, **kwargs):
    """Validates that all arguments are of String or Real types.
    """
    result = True
    previous_value = None
    for arg in args:
        result = result and \
                 (isinstance(arg, str) or isinstance(arg, Real)) and \
                 (not previous_value or isinstance(arg, type(previous_value)))
        previous_value = arg
    return result
