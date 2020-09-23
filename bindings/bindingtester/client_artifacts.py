#!/usr/bin/env python3

from argparse import ArgumentParser, Action
from argparse import FileType
import fdb
import zlib
import math
import hashlib


fdb.api_version(620)
baseDir = ('bindingtester', 'artifacts')
current_schema_version = 1
BLOB_TRANSACTION_LIMIT = 1024


def progressBar(iterable, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    total = len(iterable)
    # Progress Bar Printing Function
    def printProgressBar (iteration):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Initial Call
    printProgressBar(0)
    # Update Progress Bar
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    # Print New Line on Complete
    print()

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

class Artifact:
    def __init__(self, version, api_version=None, has_mv_client=None, upload_done=False):
        self.version = version
        self.dirName = baseDir + (version,)
        self.api_version = api_version
        self.has_mv_client = has_mv_client
        self.upload_done = upload_done

    @fdb.transactional
    def read_transactional(self, tr):
        self.read(tr)

    def read(self, tr):
        d = fdb.directory.open(tr, baseDir + (self.version,))
        schema_version = int.from_bytes(tr[d.pack(('schema_version',))].wait(), 'big')
        # for now we never changed the schema - later this needs to change
        assert schema_version == current_schema_version
        (self.api_version, self.has_mv_client, self.upload_done) = fdb.tuple.unpack(tr[d.pack(('metadata',))])

    @fdb.transactional
    def write_transactional(self, tr, replace=False):
        return self.write(tr, replace)

    def write(self, tr, replace=False):
        d = None
        if not self.upload_done or (not replace and fdb.directory.exists(tr, self.dirName)):
            print('An artifact with version {} already exists'.format(self.version))
            return False
        else:
            d = fdb.directory.create_or_open(tr, self.dirName)
        tr[d.pack(('schema_version',))] = current_schema_version.to_bytes(4, 'big')
        tr[d.pack(('metadata',))] = fdb.tuple.pack((self.api_version, self.has_mv_client, self.upload_done))
        return True

    def to_string(self):
        return "{}: api_version={}, has_mv_client={}, upload_done={}".format(self.version, self.api_version, self.has_mv_client, self.upload_done)

    @fdb.transactional
    def list_files(self, tr):
        res = []
        d = fdb.directory.create_or_open(tr, self.dirName + ('files',))
        for name in d.list(tr):
            fDir = d.open(tr, name)
            length = int.from_bytes(tr[fDir.pack(('size',))].wait(), 'big')
            res.append((name, length))
        return res

    @fdb.transactional
    def write_blob_part(self, tr, base, begin, blob):
        assert len(blob) <= BLOB_TRANSACTION_LIMIT
        tr[base.pack((begin,))] = blob

    @fdb.transactional
    def write_file_metadata(self, tr, name, size, checksum):
        d = fdb.directory.create(tr, self.dirName + ('files', name))
        assert d is not None
        tr[d.pack(('size',))] = size.to_bytes(4, byteorder='big')
        tr[d.pack(('part_size',))] = BLOB_TRANSACTION_LIMIT.to_bytes(4, 'big')
        tr[d.pack(('checksum',))] = checksum
        return d

    def upload_file(self, db, name, blob):
        m = hashlib.sha256()
        m.update(blob)
        d = self.write_file_metadata(db, name, len(blob), m.hexdigest().encode('utf-8'))
        for b in progressBar(range(int(math.ceil(len(blob) / BLOB_TRANSACTION_LIMIT))), prefix="Uploading {}".format(name)):
            begin = BLOB_TRANSACTION_LIMIT * b;
            end = min(begin + BLOB_TRANSACTION_LIMIT, len(blob))
            self.write_blob_part(db, d, begin, blob[begin:end])

    @fdb.transactional
    def read_file_metadata(self, tr, name):
        d = fdb.directory.open(tr, self.dirName + ('files', name))
        size = int.from_bytes(tr[d.pack(('size',))].wait(), 'big')
        part_size = int.from_bytes(tr[d.pack(('part_size',))].wait(), 'big')
        checksum = tr[d.pack(('checksum',))].wait().decode('utf-8')
        return (d, size, part_size, checksum)

    @fdb.transactional
    def download_blob_part(self, tr, base, begin):
        return tr[base.pack((begin,))].wait()

    def download_file(self, db, name):
        (d, size, part_size, checksum) = self.read_file_metadata(db, name)
        res = bytearray(size)
        offset = 0
        for b in progressBar(range(int(math.ceil(len(blob) / part_size))), prefix="Downloading {}".format(name)):
            blob = self.download_blob_part(db, d, offset)
            try:
                res[offset:offset+len(b)] = bytearray(blob)
            except:
                import pdb; pdb.set_trace()
            offset += len(b)
        m = hashlib.sha256()
        res = bytes(res)
        m.update(res)
        assert m.hexdigest() == checksum
        return res


@fdb.transactional
def get_artifacts(tr):
    res = []
    if not fdb.directory.exists(tr, baseDir):
        return res
    d = fdb.directory.open(tr, baseDir)
    for version in d.list(tr):
        artifact = Artifact(version)
        artifact.read(tr)
        res.append(artifact)
    return res

def list_artifacts(db, args):
    artifacts = get_artifacts(db)
    if len(artifacts) == 0:
        print('No artifacts have been uploaded yet')
        return
    for artifact in artifacts:
        if args.api_version is not None and args.api_version != artifact.api_version:
            continue
        files = artifact.list_files(db)
        print("{}".format(artifact.to_string()))
        print("Binaries:")
        for file in files:
            print("\t{} (size={})".format(file[0], sizeof_fmt(file[1])))

def add_artifact(db, args):
    has_mv_client = False if args.has_no_mv_client else True
    artifact = Artifact(args.version, api_version=args.api_version, has_mv_client=has_mv_client)
    if not artifact.write_transactional(db):
        return
    fdbcli = zlib.compress(args.fdbcli.read())
    fdbserver = zlib.compress(args.fdbserver.read())
    fdbc = zlib.compress(args.fdbc.read())
    artifact.upload_file(db, 'fdbcli', fdbcli)
    artifact.upload_file(db, 'fdbc', fdbc)
    artifact.upload_file(db, 'fdbserver', fdbserver)
    artifact.upload_done = True
    artifact.write_transactional(db, replace=True)


@fdb.transactional
def remove_artifacts(tr, args):
    if not args.all and not args.version:
        print("Either --version or --all needs to be passed to remove")
        return
    if args.all:
        fdb.directory.remove_if_exists(tr, baseDir)
        return
    d = baseDir + (args.version,)
    if not fdb.directory.exists(tr, d):
        print('No artifact with version {} found'.format(args.version))
        return
    fdb.directory.remove(tr, d)

def download_artifact(db, args):
    artifact = Artifact(args.version)
    artifact.read_transactional(db)
    if args.fdbcli:
        blob = artifact.download_file(db, 'fdbcli')
        args.fdbcli.write(zlib.decompress(blob))


class ParserBuilder:
    def list(self, subparser):
        parser = subparser.add_parser('list', help="List existing artifacts")
        parser.add_argument('--api_version', help="Filter by API version")
        parser.add_argument('--raw', action='store_true', help="List the raw keys")
        parser.set_defaults(func=list_artifacts)

    def add(self, subparser):
        parser = subparser.add_parser('add', help="Add a new artifact")
        parser.add_argument('--fdbcli', type=FileType('rb'), help="The fdbcli executable of that version", required=True)
        parser.add_argument('--fdbserver', type=FileType('rb'), help="The fdbserver executable of that version", required=True)
        parser.add_argument('--fdbc', type=FileType('rb'), help="The fdb_c shared object of that version", required=True)
        parser.add_argument('--has_no_mv_client', action='store_true', help="Has to be passed if this client doesn't implement the multiversion client")
        parser.add_argument('--api_version', type=int, help="The max API version this client/server pair supports", required=True)
        parser.add_argument('version', type=str, help="The version of this artifact")
        parser.set_defaults(func=add_artifact)

    def download(self, subparser):
        parser = subparser.add_parser('download', help="Download an artifact")
        parser.add_argument('--fdbcli', type=FileType('wb'), help="The fdbcli executable of that version")
        parser.add_argument('--fdbserver', type=FileType('wb'), help="The fdbserver executable of that version")
        parser.add_argument('--fdbc', type=FileType('wb'), help="The fdb_c shared object of that version")
        parser.add_argument('version', type=str, help="The version of this artifact")
        parser.set_defaults(func=download_artifact)

    def remove(self, subparser):
        parser = subparser.add_parser('remove', help="Delete an artifact from FDB")
        parser.add_argument('--all', action='store_true', help="Delete ALL artifacts")
        parser.add_argument('--version', type=str, help="Delete artifact with version")
        parser.set_defaults(func=remove_artifacts)


def main():
    parser = ArgumentParser()
    command_parser = parser.add_subparsers(help="Commands", required=True)
    builder = ParserBuilder()
    builder.list(command_parser)
    builder.add(command_parser)
    builder.download(command_parser)
    builder.remove(command_parser)
    args = parser.parse_args()
    db = fdb.open()
    args.func(db, args)
    # client = Client(args)
    # getattr(client, args.command)()

if __name__ == '__main__':
    main()
