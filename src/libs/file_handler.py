from os import listdir


def read_file(
        file_path: str,
        allow_empty: bool = True
) -> str:
    """"
    Read contents of a file as a string and (optionally) raise an exception if 
    the file is empty

    :param file_path: Path to the local file
    :param allow_empty: If False, raises an error when the file is empty
    """

    with open(f"{file_path}", 'r') as f:
        contents = f.read()

    if not allow_empty and not contents:
        raise ValueError(f'Empty file found for {file_path}')

    return contents


def list_files_in_dir(
        path: str,
        filter_extension: str = None,
        include_extension: bool = True
):
    """
    Returns a list of names of files within a directory

    :param path: Path to local directory
    :param filter_extension: Whether to filter files using a file extension
    :param include_extension: True = include file extension in the list of 
        results; False = display base filenames without file extensions
    """

    if filter_extension:
        files = []
        for f in listdir(path):
            if f.endswith(filter_extension):
                files.append(f) if include_extension \
                    else files.append('.'.join(f.split('.')[0:-1]) )
        return files
        
    else:
        if include_extension:
            return listdir()
        else:
            return [ '.'.join(f.split('.')[0:-1]) for f in listdir() ]
