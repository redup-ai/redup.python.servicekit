import sys

if sys.version_info >= (3, 8):
    from importlib.metadata import version
else:
    from importlib_metadata import version

try:
    __version__ = version("redup-servicekit")
except Exception:
    __version__ = "0.0.0"


__all__ = ['__version__']