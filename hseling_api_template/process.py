def process_data(data_to_process):
    """Split all files contents and then combine unique words into resulting file.
    """
    result = set()

    for _, contents in data_to_process.items():
        result |= set((contents.decode('utf-8') if isinstance(contents, bytes) else contents).split())

    if result:
        yield None, '\n'.join(sorted(result))
