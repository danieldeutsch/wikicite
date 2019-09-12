import bz2
import json
import os
from glob import glob
from threading import Thread


class IndexedBz2FileWriter(object):
    def __init__(self, file_path: str, index_file_path: str) -> None:
        self.file_path = file_path
        self.index_file_path = index_file_path
        self.offset = 0

    def __enter__(self) -> 'IndexedBz2FileWriter':
        self.out = bz2.open(self.file_path, 'wb')
        self.index_out = bz2.open(self.index_file_path, 'wb')
        return self

    def __exit__(self, *args) -> None:
        self.out.close()
        self.index_out.close()

    def write(self, key: str, data: str) -> None:
        def _write():
            entry = {'key': key}
            if data is not None:
                length = self.out.write(data.encode())
                entry['success'] = True
                entry['filename'] = self.file_path
                entry['offset'] = self.offset
                entry['length'] = length
                self.offset += length
            else:
                entry['success'] = False
            self.index_out.write(json.dumps(entry).encode() + b'\n')

        # Hack to prevent keyboard interrupt from killing the process in
        # the middle of writing data, which will corrupt the file
        thread = Thread(target=_write)
        thread.start()
        thread.join()


class IndexedBz2FilesReader(object):
    def __init__(self, index_file_glob: str) -> None:
        self.locations = {}
        for index_file_path in glob(index_file_glob):
            with bz2.open(index_file_path, 'rb') as f:
                for line in f:
                    data = json.loads(line.decode())
                    key = data['key']
                    self.locations[key] = data

    def read(self, key: str) -> str:
        if key not in self.locations:
            return None

        location = self.locations[key]
        if not location['success']:
            return None

        filename = location['filename']
        offset = location['offset']
        length = location['length']
        with bz2.open(filename, 'rb') as f:
            f.seek(offset, 0)
            return f.read(length).decode()
