def query_data(data_to_process, query_type=None):
    """Query data.
    """
    if query_type == 'lines':
        lines = 0
        for _, contents in data_to_process.items():
            if isinstance(contents, bytes):
                text = contents.decode('utf-8')
            else:
                text = contents
            lines += len(text.split('\n'))
        return lines
    return None
