#!/usr/bin/env python

import errno
import fuse
import os
import stat
import tempfile

from fuse import Fuse
from s3 import S3Helper
from threading import Lock



fuse.fuse_python_api = (0, 2)
# fuse.feature_assert('stateful_files', 'has_init')

class HMFSStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class HMFS(Fuse):

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)

        self.bucket = None
        self.s3 = S3Helper()
        self.objects = []

    def getattr(self, path):
        print("attr:", path)
        st = HMFSStat()
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        elif path[1:] in self.objects:
            st.st_mode = stat.S_IFREG | 0o644
            st.st_nlink = 1
            st.st_size = self.objects[path[1:]]["Size"]
        else:
            print(f"Path does not exists {path}")
            return -errno.ENOENT
        return st

    def readdir(self, path, offset):
        print("readdir", path)
        self.objects = self.s3.get_objects(self.bucket)

        for obj in self.objects.keys():
            yield fuse.Direntry(obj)

    def open(self, path, flags):
        if path[1:] not in self.objects:
            return -errno.ENOENT

        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        print("read", path, size, offset)
        buffer = self.s3.get_object(self.bucket, path[1:])
        return buffer

    def main(self, *a, **kw):
        options = self.cmdline[0]
        self.bucket = options.bucket
        return Fuse.main(self, *a, **kw)


def main():
    pid = os.getpid()
    with open("./pid", "w") as f:
        f.write(f"{pid}")

    usage = """
Hackmeeting File System

""" + Fuse.fusage

    server = HMFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="bucket", metavar="BUCKET", default="__none__",
                                help="S3 bucket name")

    server.parse(values=server, errex=1)
    server.main()


if __name__ == '__main__':
    main()
