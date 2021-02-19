import os
import tarfile
import logging


def un_tar_directory(fileobj, directory_path, compression='', force=False):
    """
    Extracts the given file-like object containing a tar archive into the given
    directory, which will be created and should not already exist. If it already exists,
    and `force` is `False`, an error is raised. If it already exists, and `force` is `True`,
    the directory is removed and recreated.

    compression specifies the compression scheme and can be one of '', 'gz' or
    'bz2'.

    Raises tarfile.TarError if the archive is not valid.
    """
    directory_path = os.path.realpath(directory_path)
    if force:
        from codalab.worker.file_util import remove_path

        remove_path(directory_path)
    os.mkdir(directory_path)
    with tarfile.open(fileobj=fileobj, mode='r|' + compression) as tar:
        logging.info("un_tar_directory, opening tar file, with directory path: %s", directory_path)
        for member in tar:
            # Make sure that there is no trickery going on (see note in
            # TarFile.extractall() documentation.
            logging.info(
                "un_tar_directory, extracting member: %s, directory_path: %s",
                member.name,
                directory_path,
            )
            member_path = os.path.realpath(os.path.join(directory_path, member.name))
            if not member_path.startswith(directory_path):
                raise tarfile.TarError('Archive member extracts outside the directory.')
            tar.extract(member, directory_path)
