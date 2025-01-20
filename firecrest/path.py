"""A pathlib.Path-like object for accessing the file system via the Firecrest API."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO
import os
from pathlib import PurePosixPath
import stat
import tempfile
from typing import Callable, Iterator, List

from firecrest import ClientCredentialsAuth
from firecrest.v1 import Firecrest
from firecrest.v1.BasicClient import logger as FcLogger
from firecrest.FirecrestException import HeaderException


try:
    # available in python 3.11
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self


@contextmanager
def diable_fc_logging() -> Iterator[None]:
    """Temporarily disable Firecrest logging.

    This is useful when calling methods that are expected to fail,
    such as `exists` or `is_dir`, as it avoids polluting the log with errors.
    """
    level = FcLogger.level
    FcLogger.setLevel(60)
    try:
        yield
    finally:
        FcLogger.setLevel(level)


class MachineDoesNotExist(OSError):
    """The machine does not exist."""

    def __init__(self, path: FcPath) -> None:
        super().__init__(f"Machine does not exist: {path!r}")


class ApiTimeoutError(OSError):
    """The API call timed out."""

    def __init__(self, path: FcPath) -> None:
        super().__init__(f"API call timed out: {path!r}")


_COMMON_HEADER_EXC: dict[str, Callable[[FcPath], Exception] | None] = {
    "X-Timeout": ApiTimeoutError,
    "X-Machine-Does-Not-Exist": MachineDoesNotExist,
    "X-Machine-Not-Available": PermissionError,
    "X-Permission-Denied": PermissionError,
    "X-Not-Found": FileNotFoundError,
    "X-Not-A-Directory": NotADirectoryError,
    "X-Exists": FileExistsError,
    "X-Invalid-Path": FileNotFoundError,
    "X-A-Directory": IsADirectoryError,
}


@dataclass
class _Cache:
    """A cache of path statistics."""

    st_mode: int | None = None
    """The st_mode of the path, following symlinks."""
    lst_mode: int | None = None
    """The st_mode of the path, not following symlinks."""
    link_target: str | None = None
    """The target path of a symlink."""

    def reset(self) -> None:
        """Reset the cache."""
        self.st_mode = None
        self.lst_mode = None
        self.link_target = None


class FcPath(os.PathLike):
    """A pathlib.Path-like object for accessing the file system via the Firecrest API."""

    __slots__ = ("_client", "_machine", "_path", "_cache_enabled", "_cache")

    def __init__(
        self,
        client: Firecrest,
        machine: str,
        path: str | PurePosixPath,
        *,
        cache_enabled: bool = False,
        _cache: None | _Cache = None,
    ) -> None:
        """Construct a new FcPath instance.

        :param client: A Firecrest client object
        :param machine: The machine name
        :param path: The absolute path to the file or directory
        :param cache_enabled: Enable caching of path statistics
            This enables caching of path statistics, like mode,
            which can be useful if you are using multiple methods on the same path,
            as it avoids making multiple calls to the API.
            You should only use this if you are sure that the file system is not being modified.

        """
        self._client = client
        self._machine = machine
        self._path = PurePosixPath(path)
        if not self._path.is_absolute():
            raise ValueError(f"Path must be absolute: {str(self._path)!r}")
        self._cache_enabled = cache_enabled
        self._cache = _cache or _Cache()

    @classmethod
    def from_env_variables(
        cls, machine: str, path: str | PurePosixPath, *, cache_enabled: bool = False
    ) -> Self:
        """Convenience method, to construct a new FcPath using environmental variables.

        The following environment variables are required:
        - FIRECREST_URL
        - FIRECREST_CLIENT_ID
        - FIRECREST_CLIENT_SECRET
        - AUTH_TOKEN_URL
        """
        auth_obj = ClientCredentialsAuth(
            os.environ["FIRECREST_CLIENT_ID"],
            os.environ["FIRECREST_CLIENT_SECRET"],
            os.environ["AUTH_TOKEN_URL"],
        )
        client = Firecrest(os.environ["FIRECREST_URL"], authorization=auth_obj)
        return cls(client, machine, path, cache_enabled=cache_enabled)

    @property
    def client(self) -> Firecrest:
        """The Firecrest client object."""
        return self._client

    @property
    def machine(self) -> str:
        """The machine name."""
        return self._machine

    @property
    def path(self) -> str:
        """Return the string representation of the path on the machine."""
        return str(self._path)

    @property
    def pure_path(self) -> PurePosixPath:
        """Return the pathlib representation of the path on the machine."""
        return self._path

    @property
    def cache_enabled(self) -> bool:
        """Enable caching of path statistics.

        This enables caching of path statistics, like mode,
        which can be useful if you are using multiple methods on the same path,
        as it avoids making multiple calls to the API.

        You should only use this if you are sure that the file system is not being modified.
        """
        return self._cache_enabled

    @cache_enabled.setter
    def cache_enabled(self, value: bool) -> None:
        self._cache_enabled = value

    def clear_cache(self) -> None:
        """Clear the cache of path statistics."""
        self._cache = _Cache()

    def _new_path(self, path: PurePosixPath, *, _cache: None | _Cache = None) -> Self:
        """Construct a new FcPath object from a PurePosixPath object."""
        return self.__class__(
            self._client,
            self._machine,
            path,
            cache_enabled=self._cache_enabled,
            _cache=_cache,
        )

    def __fspath__(self) -> str:
        return str(self._path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        variables = [
            repr(self._client._firecrest_url),
            repr(self._machine),
            repr(self.path),
        ]
        if self._cache_enabled:
            variables.append("CACHED")
        return f"{self.__class__.__name__}({', '.join(variables)})"

    def as_posix(self) -> str:
        """Return the string representation of the path."""
        return self._path.as_posix()

    @property
    def name(self) -> str:
        """The final path component, if any."""
        return self._path.name

    @property
    def suffix(self) -> str:
        """
        The final component's last suffix, if any.

        This includes the leading period. For example: '.txt'
        """
        return self._path.suffix

    @property
    def suffixes(self):
        """
        A list of the final component's suffixes, if any.

        These include the leading periods. For example: ['.tar', '.gz']
        """
        return self._path.suffixes

    @property
    def stem(self) -> str:
        """
        The final path component, minus its last suffix.

        If the final path component has no suffix, this is the same as name.
        """
        return self._path.stem

    def with_name(self, name: str) -> Self:
        """Return a new path with the file name changed."""
        return self._new_path(self._path.with_name(name))

    def with_suffix(self, suffix: str) -> Self:
        """Return a new path with the file suffix changed."""
        return self._new_path(self._path.with_suffix(suffix))

    @property
    def parts(self) -> tuple[str, ...]:
        """The components of the path."""
        return self._path.parts

    @property
    def parent(self) -> Self:
        """The path’s parent directory."""
        return self._new_path(self._path.parent)

    def is_absolute(self) -> bool:
        """Return True if the path is absolute."""
        return self._path.is_absolute()

    def __truediv__(self, other: str) -> Self:
        return self._new_path(self._path / other)

    def joinpath(self, *other: str) -> Self:
        """Combine this path with one or several arguments, and return a
        new path representing either a subpath (if all arguments are relative
        paths) or a totally different path (if one of the arguments is
        anchored).
        """
        return self._new_path(self._path.joinpath(*other))

    @contextmanager
    def convert_header_exceptions(
        self, convert: None | dict[str, Callable[[Self], Exception] | None] = None
    ) -> Iterator[None]:
        """Catch HeaderException and re-raise as an alternative."""
        converters: dict[str, Callable[[Self], Exception] | None] = {
            **_COMMON_HEADER_EXC,
            **(convert or {}),
        }
        with diable_fc_logging():
            try:
                yield
            except HeaderException as exc:
                for header in exc.responses[-1].headers:
                    c = converters.get(header, None)
                    if c is not None:
                        raise c(self) from exc
                raise

    def checksum(self) -> str:
        """Return the SHA256 (256-bit) checksum of the file."""
        # this is not part of the pathlib.Path API, but is useful
        with self.convert_header_exceptions():
            return self._client.checksum(self._machine, self.path)

    # methods that utilise stat calls

    def _lstat_mode(self) -> int:
        """Return the st_mode of the path, not following symlinks."""
        if self._cache_enabled and self._cache.lst_mode is not None:
            return self._cache.lst_mode
        # TODO This is currently a workaround for a bug in the Firecrest API: https://github.com/eth-cscs/firecrest/issues/171
        # Until fixed, the only way to get the full mode is to access it via `ls`` on the parent directory.
        for item in self.parent.iterdir(hidden=self.name.startswith(".")):
            if item.name == self.name:
                self._cache.lst_mode = item._cache.lst_mode
                return item._cache.lst_mode
        raise FileNotFoundError(self)

    def resolve(self) -> Self:
        """Resolve a path, removing '..' and '.' components."""
        parts: List[str] = []
        for part in self.parts:
            if part == '..':
                if parts:
                    parts.pop()
            elif part != '.':
                parts.append(part)

        return self._new_path(PurePosixPath(*parts))

    def _stat_mode(self) -> int:
        """Return the st_mode of the path, following symlinks."""
        if self._cache_enabled and self._cache.st_mode is not None:
            return self._cache.st_mode
        if (
            self._cache_enabled
            and self._cache.lst_mode is not None
            and not stat.S_ISLNK(self._cache.lst_mode)
        ):
            self._cache.st_mode = self._cache.lst_mode
            return self._cache.st_mode
        # TODO This is currently a workaround for a bug in the Firecrest API: https://github.com/eth-cscs/firecrest/issues/171
        # Until fixed, the only way to get the full mode is to access it via `ls`` on the parent directory.
        # this gives us the mode of the symlink, not the target, so then we need to follow the symlink
        path = self
        followed_links = 0
        while True:
            if followed_links > 10:
                raise FileNotFoundError(f"Followed more than 10 symlinks: {self}")
            for item in path.parent.iterdir(hidden=path.name.startswith(".")):
                if item.name != path.name:
                    continue
                mode = item._cache.lst_mode
                if stat.S_ISLNK(mode):
                    if not item._cache.link_target:
                        raise FileNotFoundError(f"Symlink has no target path: {self}")
                    pureposixpath = PurePosixPath(item._cache.link_target)
                    if not pureposixpath.is_absolute():
                        path = path.parent.joinpath(pureposixpath).resolve()
                    else:
                        path = self._new_path(pureposixpath)
                    followed_links += 1
                    break
                else:
                    self._cache.st_mode = mode
                    return mode
            else:
                raise FileNotFoundError(self)

    def stat(self) -> os.stat_result:
        """Return stat info for this path.

        If the path is a symbolic link,
        stat will examine the file the link points to.
        """
        with self.convert_header_exceptions():
            stats = self._client.stat(self._machine, self.path, dereference=True)
        return os.stat_result(
            (
                self._stat_mode(),
                stats["ino"],
                stats["dev"],
                stats["nlink"],
                stats["uid"],
                stats["gid"],
                stats["size"],
                stats["atime"],
                stats["mtime"],
                stats["ctime"],
            )
        )

    def lstat(self) -> os.stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        with self.convert_header_exceptions():
            stats = self._client.stat(self._machine, self.path, dereference=False)
        return os.stat_result(
            (
                self._lstat_mode(),
                stats["ino"],
                stats["dev"],
                stats["nlink"],
                stats["uid"],
                stats["gid"],
                stats["size"],
                stats["atime"],
                stats["mtime"],
                stats["ctime"],
            )
        )

    def exists(self) -> bool:
        """Whether this path exists (follows symlinks)."""
        try:
            self.stat()
        except FileNotFoundError:
            return False
        return True

    def is_dir(self) -> bool:
        """Whether this path is a directory (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISDIR(st_mode)

    def is_file(self) -> bool:
        """Whether this path is a regular file (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISREG(st_mode)

    def is_symlink(self) -> bool:
        """Whether this path is a symbolic link."""
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISLNK(st_mode)

    def is_block_device(self) -> bool:
        """Whether this path is a block device (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISBLK(st_mode)

    def is_char_device(self) -> bool:
        """Whether this path is a character device (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISCHR(st_mode)

    def is_fifo(self) -> bool:
        """Whether this path is a FIFO (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISFIFO(st_mode)

    def is_socket(self) -> bool:
        """Whether this path is a socket (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISSOCK(st_mode)

    def iterdir(self, hidden=True, recursive=False) -> Iterator[Self]:
        """Iterate over the directory entries."""
        with self.convert_header_exceptions():
            results = self._client.list_files(
                self._machine, self.path, show_hidden=hidden, recursive=recursive
            )
        for entry in results:
            yield self._new_path(
                self._path / entry["name"],
                _cache=_Cache(
                    lst_mode=_ls_to_st_mode(entry["type"], entry["permissions"]),
                    link_target=entry["link_target"],
                ),
            )

    def relpath(self, start: str | PurePosixPath) -> str:
        """Return a relative version of this path."""
        return self._path.relative_to(start).as_posix()

    # operations that modify a file

    def chmod(self, mode: int) -> None:
        """Change the mode of the path to the numeric mode.

        Note, if the path points to a symlink,
        the symlink target's permissions are changed.
        """
        # note: according to https://www.gnu.org/software/coreutils/manual/html_node/chmod-invocation.html#chmod-invocation
        # chmod never changes the permissions of symbolic links,
        # i.e. this is chmod, not lchmod
        if not isinstance(mode, int):
            raise TypeError("mode must be an integer")
        with self.convert_header_exceptions(
            {"X-Invalid-Mode": lambda p: ValueError(f"invalid mode: {mode}")}
        ):
            self._client.chmod(self._machine, self.path, str(mode))
            self._cache.reset()

    def rename(self, target: str) -> Self:
        """Rename this path to the (absolute) target path.

        Returns the new Path instance pointing to the target path.
        """
        target_path = self._new_path(PurePosixPath(target))
        with self.convert_header_exceptions():
            self._client.mv(self._machine, self.path, target_path.path)
        return target_path

    def symlink_to(self, target: str) -> None:
        """Make this path a symlink pointing to the target path."""
        target_path = PurePosixPath(target)
        if not target_path.is_absolute():
            raise ValueError("target must be an absolute path")
        with self.convert_header_exceptions():
            self._client.symlink(self._machine, str(target_path), self.path)

    def mkdir(
        self, mode: None = None, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a new directory at this given path."""
        if mode is not None:
            raise NotImplementedError("mode is not supported yet")
        with self.convert_header_exceptions():
            # Note see: https://github.com/eth-cscs/firecrest/issues/172
            # Also see: https://github.com/eth-cscs/firecrest/issues/202
            # firecrest does not support `exist_ok`, it's somehow blended into `parents`
            self._client.mkdir(self._machine, self.path, p=parents if not exist_ok else True)

    def touch(self, mode: None = None, exist_ok: bool = True) -> None:
        """Create a file at this given path.

        :param mode: ignored
        :param exist_ok: if True, do not raise an exception if the path already exists
        """
        if mode is not None:
            raise NotImplementedError("mode is not supported yet")
        if self.exists():
            if exist_ok:
                return
            raise FileExistsError(self)
        try:
            _, source_path = tempfile.mkstemp()
            with self.convert_header_exceptions():
                self._client.simple_upload(
                    self._machine, source_path, self.parent.path, self.name
                )
        finally:
            os.remove(source_path)

    def read_bytes(self) -> bytes:
        """Read the contents of the file as bytes."""
        # TODO capture 413 status_code response, for content too large error?
        io = BytesIO()
        with self.convert_header_exceptions():
            self._client.simple_download(self._machine, self.path, io)
        return io.getvalue()

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Read the contents of the file as text."""
        return self.read_bytes().decode(encoding, errors)

    def write_bytes(self, data: bytes) -> None:
        """Write bytes to the file."""
        # TODO capture 413 status_code response, for content too large error?
        buffer = BytesIO(data)
        with self.convert_header_exceptions():
            self._client.simple_upload(
                self._machine, buffer, self.parent.path, self.name
            )

    def write_text(
        self, data: str, encoding: str = "utf-8", errors: str = "strict"
    ) -> None:
        """Write text to the file."""
        self.write_bytes(data.encode(encoding, errors))

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove this file."""
        # note /utilities/rm uses `rm -rf`,
        # so we have to be careful to check first what we are deleting
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            if not missing_ok:
                raise FileNotFoundError(self)
            return
        if stat.S_ISDIR(st_mode):
            raise IsADirectoryError(self)
        with self.convert_header_exceptions():
            self._client.simple_delete(self._machine, self.path)
            self._cache.reset()

    def rmtree(self) -> None:
        """Recursively delete a directory tree."""
        # note /utilities/rm uses `rm -rf`,
        # so we have to be careful to check first what we are deleting
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            raise FileNotFoundError(self)
        if not stat.S_ISDIR(st_mode):
            raise NotADirectoryError(self)
        with self.convert_header_exceptions():
            self._client.simple_delete(self._machine, self.path)
            self._cache.reset()


@lru_cache(maxsize=256)
def _ls_to_st_mode(ftype: str, permissions: str) -> int:
    """Use the return information from `utilities/ls` to create an st_mode value.

    :param ftype: The file type, e.g. "-" for regular file, "d" for directory.
    :param permissions: The file permissions, e.g. "rwxr-xr-x".
    """
    ftypes = {
        "b": "0060",  # block device
        "c": "0020",  # character device
        "d": "0040",  # directory
        "l": "0120",  # Symbolic link
        "s": "0140",  # Socket.
        "p": "0010",  # FIFO
        "-": "0100",  # Regular file
    }
    if ftype not in ftypes:
        raise ValueError(f"invalid file type: {ftype}")
    p = permissions
    r = lambda x: 4 if x == "r" else 0  # noqa: E731
    w = lambda x: 2 if x == "w" else 0  # noqa: E731
    x = lambda x: 1 if x == "x" else 0  # noqa: E731
    st_mode = (
        ((r(p[0]) + w(p[1]) + x(p[2])) * 100)
        + ((r(p[3]) + w(p[4]) + x(p[5])) * 10)
        + ((r(p[6]) + w(p[7]) + x(p[8])) * 1)
    )
    return int(ftypes[ftype] + str(st_mode), 8)
