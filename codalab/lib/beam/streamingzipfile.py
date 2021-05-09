import struct
from zipfile import (
    ZipFile, ZipInfo, ZipExtFile, BadZipFile,
    structFileHeader, stringFileHeader, sizeFileHeader, MAX_EXTRACT_VERSION,
    _FH_SIGNATURE, _FH_GENERAL_PURPOSE_FLAG_BITS, _FH_FILENAME_LENGTH, _FH_EXTRA_FIELD_LENGTH
)


class StreamingZipFile(ZipFile):
    """A version of ZipFile that can read file entries in a streaming fashion.
    
    Normally, using ZipFile to read files in a .zip archive requires random access
    to the file, because ZipFile reads the central directory at the end of the archive first
    to retrieve the metadata of each file in the archive to create the list of ZipInfo objects.

    Instead, StreamingZipFile can be iterated over in order to read each file one by one,
    using each file header to construct ZipInfo objects for each file. Sample usage:

    zf = StreamingZipFile(fileobj)
    for zinfo in zf:
        print(zinfo)
        print(zf.open(zinfo).read())
    """

    # Stores the position of the next file header to be read.
    _next_header_pos = 0

    # Whether all entries have been loaded.
    _loaded = False

    def _RealGetContents(self, *args, **kwargs):
        """Internal method of ZipFile that normally reads the central directory of the archive
        and is called upon initialization. Here, we override this method so that nothing happens.
        """
        pass

    def open(self, name, mode="r", pwd=None, *, force_zip64=False):
        """Open a file within the .zip archive. Normally, this method in ZipFile seeks to the
        specified file and skips its header. We override this method so that we just
        continue reading from the current location of the ZipInfo's underlying file object (self.fp).
        """
        if isinstance(name, ZipInfo):
            # 'name' is already an info object
            zinfo = name
        else:
            raise OSError("must specify a zipinfo object when reading")
        # Open for reading:
        return ZipExtFile(self.fp, mode, zinfo, pwd, True)

    def next(self):
        """Return the next member of the archive as a ZipInfo object. Returns
        None if there is no more available. This method is analogous to
        TarFile.next().

        We construct a ZipInfo object using the information stored in the next file header.
        The logic here is based on the implementation of ZipFile._RealGetContents(), which
        constructs a ZipInfo object from information in a central directory file header, but
        modified to work with the file-header-specific struct.
        """
        fp = self.fp

        # Advance to the next header, if needed.
        fp.read(self._next_header_pos - fp.tell())

        # Read the next header.
        zipinfo = None
        try:
            fheader = fp.read(sizeFileHeader)
            if len(fheader) != sizeFileHeader:
                raise BadZipFile("Truncated file header")
            fheader = struct.unpack(structFileHeader, fheader)
            if fheader[_FH_SIGNATURE] != stringFileHeader:
                raise BadZipFile("Bad magic number for file header")
            filename = fp.read(fheader[_FH_FILENAME_LENGTH])
            flags = fheader[_FH_GENERAL_PURPOSE_FLAG_BITS]
            if flags & 0x800:
                # UTF-8 file names extension
                filename = filename.decode('utf-8')
            else:
                # Historical ZIP filename encoding
                filename = filename.decode('cp437')
            # Create ZipInfo instance to store file information
            x = ZipInfo(filename)
            x.extra = fp.read(fheader[_FH_EXTRA_FIELD_LENGTH])
            x.header_offset = self._next_header_pos

            # The file header stores nearly all the same information needed for ZipInfo as what the
            # central directory file header stores, except for a couple of missing fields.
            # We just set them to 0 here.
            x.comment = 0
            x.create_version, x.create_system = 0, 0
            x.volume, x.internal_attr, x.external_attr = 0, 0, 0

            (x.extract_version, x.reserved, x.flag_bits, x.compress_type, t, d,
            x.CRC, x.compress_size, x.file_size) = fheader[1:10]
            if x.extract_version > MAX_EXTRACT_VERSION:
                raise NotImplementedError("zip file version %.1f" %
                                        (x.extract_version / 10))

            # Convert date/time code to (year, month, day, hour, min, sec)
            x._raw_time = t
            x.date_time = ( (d>>9)+1980, (d>>5)&0xF, d&0x1F,
                            t>>11, (t>>5)&0x3F, (t&0x1F) * 2 )

            x._decodeExtra()
            self.filelist.append(x)
            self.NameToInfo[x.filename] = x
            self._next_header_pos = (
                fp.tell() + x.compress_size
            )  # Beginning of the next file's header.
            return x

        except BadZipFile:
            # We've reached the end of all the file entries.
            self._loaded = True
            return None

    def __iter__(self):
        """Provide an iterator object that yields members of the archive.
        This method is analogous to TarFile.__iter__().
        """
        if self._loaded:
            yield from self.infolist
            return

        # Yield items using ZipFile's next() method. When all
        # members have been read, set ZipFile as _loaded.
        while True:
            zipinfo = self.next()
            if not zipinfo:
                self._loaded = True
                return
            yield zipinfo
