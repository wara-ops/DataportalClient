from importlib.metadata import PackageNotFoundError, version

from .chunkedlist import ChunkedList
from .dataportalclient import DataportalClient

# The version is read from the installed distribution metadata, which pdm-backend
# fills in from the git tag at build time. It is deliberately not a generated file
# or part of versions.py: a file has to be kept in step with the tag by hand or by
# a CI rewrite, and that is exactly what used to let a build carry the wrong label.
#
# NOTE this is the PEP 440 normalised form, '1.2.3', WITHOUT the leading 'v' that the
# git tag (v1.2.3) has. Anything comparing __version__ to a tag must strip the 'v'
# from the tag first. Builds past a tag look like '1.2.4.dev4+g<sha>'.
try:
    __version__ = version("dataportalclient")
except PackageNotFoundError:
    # A source tree that was never installed (PYTHONPATH=src, or a bare checkout) has
    # no metadata to read. Importing the package must still work, so report an
    # obviously-not-a-release value instead of raising; it cannot be mistaken for a
    # real version, and 'pip install .' or 'pdm install' gives the real one.
    __version__ = "0.0.0.dev0"
