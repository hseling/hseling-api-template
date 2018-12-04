def process_data(data_to_process):
    """Split all files contents and then combine unique words into resulting file.
    """
    result = set()

    for _, contents in data_to_process.items():
        if isinstance(contents, bytes):
            text = contents.decode('utf-8')
        else:
            text = contents
        result |= set([word + "!!!" for word in text.split()])

    if result:
        yield None, '\n'.join(sorted(list(result)))
