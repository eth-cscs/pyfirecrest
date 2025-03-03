#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import json
import logging
import re
import typer
import yaml

import firecrest as fc

from firecrest import __app_name__, __version__
from typing import List, Optional
from enum import Enum

from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.theme import Theme


app = typer.Typer(
    # Disable printing locals to avoid printing the value of local
    # variables in order to hide secrets/password etc
    pretty_exceptions_show_locals=False,
)

custom_theme = {
    "repr.attrib_name": "none",
    "repr.attrib_value": "none",
    "repr.number": "none",
}
console = Console(theme=Theme(custom_theme))
client: fc.v2.Firecrest = None  # type: ignore
logger = logging.getLogger(__name__)


def examine_exeption(e: Exception) -> None:
    msg = f"{__app_name__}: Operation failed"
    if isinstance(e, fc.ClientsCredentialsException):
        msg += ": could not fetch token"
    elif isinstance(e, fc.FirecrestException):
        msg += ": a Firecrest client error has occurred"
    else:
        # in case of FirecrestException and ClientsCredentialsException
        # we don't need to log again the exception
        logger.critical(e)

    console.print(f"[red]{msg}[/red]")


def version_callback(value: bool):
    if value:
        console.print(f"FirecREST CLI Version: {__version__}")
        raise typer.Exit()


def config_parent_load_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    ctx.default_map = ctx.parent.default_map  # type: ignore


def config_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    if value:
        try:
            with open(value, 'r') as f:
                config = yaml.safe_load(f)

            ctx.default_map = ctx.default_map or {}
            ctx.default_map.update(config)
        except Exception as ex:
            raise typer.BadParameter(str(ex))

    return value


@app.command(rich_help_panel="Status commands")
def systems(
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="Get information for only one system."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
    pager: Optional[bool] = typer.Option(
        True, help="Display the output in a pager application."
    ),
):
    """Provides information for the available systems in FirecREST"""
    try:
        if name:
            raise NotImplementedError("Rich table not implemented yet")
            result = [client.system(name)]
            title = f"Status of FirecREST system `{name}`"
        else:
            result = client.systems()
            # title = "Status of FirecREST systems"

        if raw:
            if pager:
                with console.pager():
                    console.print(json.dumps(result, indent=4))

            console.print(json.dumps(result, indent=4))
            return
        else:
            raise NotImplementedError("Rich table not implemented yet")

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_nodes(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    nodes: Optional[List[str]] = typer.Argument(
        None, help="List of specific compute nodes to query."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
    pager: Optional[bool] = typer.Option(
        True, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the compute nodes of the scheduler.
    """
    try:
        results = client.nodes(system)
        # TODO filter by nodes (?)
        if nodes:
            raise NotImplementedError("Node filtering not implemented yet")

        if raw:
            if pager:
                with console.pager():
                    console.print(json.dumps(results, indent=4))

            console.print(json.dumps(results, indent=4))
        else:
            raise NotImplementedError("Rich table not implemented yet")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_reservations(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    reservations: Optional[List[str]] = typer.Argument(
        None, help="List of specific reservations to query."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
    pager: Optional[bool] = typer.Option(
        True, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the scheduler reservations.
    """
    try:
        results = client.reservations(system)
        # TODO filter by reservations (?)
        if reservations:
            raise NotImplementedError("Reservations filtering not implemented yet")

        if raw:
            if pager:
                with console.pager():
                    console.print(json.dumps(results, indent=4))

            console.print(json.dumps(results, indent=4))
        else:
            raise NotImplementedError("Rich table not implemented yet")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_partitions(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    partitions: Optional[List[str]] = typer.Argument(
        None, help="List of specific partitions to query."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
    pager: Optional[bool] = typer.Option(
        True, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the scheduler partitions.
    """
    try:
        results = client.partitions(system)
        # TODO filter by partitions (?)
        if partitions:
            raise NotImplementedError("Partitions filtering not implemented yet")

        if raw:
            if pager:
                with console.pager():
                    console.print(json.dumps(results, indent=4))

            console.print(json.dumps(results, indent=4))
        else:
            raise NotImplementedError("Rich table not implemented yet")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def id(
    system: str = typer.Option(
        None,
        "-s",
        "--system",
        help="The name of the system where the `id` command will run.",
        envvar="FIRECREST_SYSTEM",
    ),
    raw: bool = typer.Option(False, "--json", "--raw", help="Print the output in json format."),
):
    """Return the identity of the user in the remote system.
    """
    try:
        result = client.userinfo(system)
        if raw:
            console.print(json.dumps(result, indent=4))
        else:
            user = f"{result['user']['id']}({result['user']['name']})"
            group = f"{result['group']['id']}({result['group']['name']})"
            all_groups = ",".join([f"{g['id']}({g['name']})" for g in result["groups"]])
            console.print(f"uid={user} gid={group} groups={all_groups}")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def ls(
    config_from_parent: str = typer.Option(
        None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    show_hidden: bool = typer.Option(
        False,
        "-a",
        "--show-hidden",
        help="Include directory entries whose names begin with a dot (‘.’).",
    ),
    recursive: bool = typer.Option(
        False,
        "-R",
        "--recursive",
        help="Recursively list directories encountered.",
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
):
    """List directory contents"""
    try:
        result = client.list_files(system, path, show_hidden, recursive)
        if raw:
            console.print(json.dumps(result, indent=4))
        else:
            table = create_table(
                f"Files in system `{system}` and path `{path}`",
                result,
                ("Filename", "name"),
                ("Type", "type"),
                ("Group", "group"),
                ("Permissions", "permissions"),
                ("Size", "size"),
                ("User", "user"),
                ("Last modified", "last_modified"),
                ("Link target", "link_target"),
            )

            console.print(table)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def mkdir(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    p: bool = typer.Option(
        False,
        "-p",
        help="Create intermediate directories as required, equivalent to `-p` of the unix command.",
    ),
):
    """Create new directories"""
    try:
        client.mkdir(system, path, p)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def mv(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
):
    """Rename/move files, directory, or symlink at the `source_path` to the `target_path` on `system`'s filesystem"""
    try:
        client.mv(system, source, destination)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chmod(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    mode: str = typer.Argument(..., help="Same as numeric mode of linux chmod tool."),
):
    """Change the file mod bits of a given file according to the specified mode"""
    try:
        client.chmod(system, path, mode)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chown(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    owner: Optional[str] = typer.Option(None, help="Owner ID for target."),
    group: Optional[str] = typer.Option(None, help="Group ID for target."),
):
    """Change the user and/or group ownership of a given file.

    If only owner or group information is passed, only that information will be updated.
    """
    try:
        client.chown(system, path, owner, group)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def cp(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
):
    """Copy files"""
    try:
        client.copy(system, source, destination)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def compress(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
):
    """Compress files using gzip compression.
    You can name the output file as you like, but typically these files have a .tar.gz extension.
    """
    try:
        client.compress(system, source, destination, fail_on_timeout=False)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def extract(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    extension: str = typer.Argument("auto", help="Extension of file. Possible values are `auto`, `.zip`, `.tar`, `.tgz`, `.gz` and `.bz2`."),
):
    """Extract files.
    If you don't select the extension, FirecREST will try to guess the right command based on the extension of the sourcePath.
    Supported extensions are `.zip`, `.tar`, `.tgz`, `.gz` and `.bz2`.
    """
    try:
        client.extract(system, source, destination, extension, fail_on_timeout=False)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def file(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
):
    """Determine file type"""
    try:
        console.print(client.file_type(system, path))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def stat(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    deref: bool = typer.Option(False, "-L", "--dereference", help="Follow links."),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
):
    """Use the stat linux application to determine the status of a file on the system's filesystem"""
    try:
        result = client.stat(system, path, deref)
        if raw:
            console.print(json.dumps(result, indent=4))
        else:
            title = f"Status of file {path}"
            if deref:
                title += " (dereferenced)"

            data = [
                {
                    "Attribute": "mode",
                    "Value": result["mode"],
                    "Description": "access rights in octal",
                },
                {
                    "Attribute": "ino",
                    "Value": result["ino"],
                    "Description": "inode number",
                },
                {
                    "Attribute": "dev",
                    "Value": result["dev"],
                    "Description": "device number in decimal",
                },
                {
                    "Attribute": "nlink",
                    "Value": result["nlink"],
                    "Description": "number of hard links",
                },
                {
                    "Attribute": "uid",
                    "Value": result["uid"],
                    "Description": "user ID of owner",
                },
                {
                    "Attribute": "gid",
                    "Value": result["gid"],
                    "Description": "group ID of owner",
                },
                {
                    "Attribute": "size",
                    "Value": result["size"],
                    "Description": "total size, in bytes",
                },
                {
                    "Attribute": "atime",
                    "Value": result["atime"],
                    "Description": "time of last access, seconds since Epoch",
                },
                {
                    "Attribute": "mtime",
                    "Value": result["mtime"],
                    "Description": "time of last data modification, seconds since Epoch",
                },
                {
                    "Attribute": "ctime",
                    "Value": result["ctime"],
                    "Description": "time of last status change, seconds since Epoch",
                },
            ]
            table = create_table(
                title,
                data,
                ("Attribute", "Attribute"),
                ("Value", "Value"),
                ("Description", "Description"),
            )

            console.print(table)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def symlink(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    target: str = typer.Argument(..., help="The path of the original file."),
    link_name: str = typer.Argument(..., help="The name of the link to the TARGET."),
):
    """Create a symbolic link"""
    try:
        client.symlink(system, target, link_name)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def rm(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    force: bool = typer.Option(
        ...,
        prompt="Are you sure you want to delete this entry?",
        help="Attempt to remove the files without prompting for confirmation, regardless of the file's permissions.",
    ),
    # TODO (?) add option to not display error to emulate `-f` from the rm command
):
    """Remove directory entries"""
    try:
        if force:
            client.simple_delete(system, path)
        else:
            console.print("Operation cancelled")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def checksum(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
):
    """Calculate the SHA256 (256-bit) checksum"""
    try:
        console.print(client.checksum(system, path))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def head(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    lines: str = typer.Option(
        None,
        "-n",
        "--lines",
        help="Print NUM lines of each of the specified files; with a leading '-', print all but the last NUM lines of each file",
        metavar="[-]NUM",
    ),
    bytes: str = typer.Option(
        None,
        "-c",
        "--bytes",
        help="Print NUM bytes of each of the specified files; with a leading '-', print all but the last NUM bytes of each file",
        metavar="[-]NUM",
    ),
):
    """Display the beginning of a specified file.
    By default the first 10 lines will be returned.
    Bytes and lines cannot be specified simultaneously.

    You view only files smaller than UTILITIES_MAX_FILE_SIZE bytes.
    This variable is available in the parameters command.
    """
    if lines and bytes:
        console.print(
            f"[red]{__app_name__} head: cannot specify both 'bytes' and 'lines'[/red]"
        )
        raise typer.Exit(code=1)

    try:
        lines_arg = lines
        bytes_arg = bytes
        skip_ending = False
        if lines and lines.startswith("-"):
            lines_arg = lines[1:]
            skip_ending = True
        elif bytes and bytes.startswith("-"):
            bytes_arg = bytes[1:]
            skip_ending = True

        console.print(client.head(system, path, bytes_arg, lines_arg, skip_ending))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def tail(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    lines: str = typer.Option(
        None,
        "-n",
        "--lines",
        help="Output the last NUM lines; or use +NUM to output starting with line NUM",
        metavar="[+]NUM",
    ),
    bytes: str = typer.Option(
        None,
        "-c",
        "--bytes",
        help="Output the last NUM bytes; or use +NUM to output starting with byte NUM",
        metavar="[+]NUM",
    ),
):
    """Display the end of a specified file.
    By default the last 10 lines will be returned.
    Bytes and lines cannot be specified simultaneously.

    You view only files smaller than UTILITIES_MAX_FILE_SIZE bytes.
    This variable is available in the parameters command.
    """
    if lines and bytes:
        console.print(
            f"[red]{__app_name__} tail: cannot specify both 'bytes' and 'lines'[/red]"
        )
        raise typer.Exit(code=1)

    try:
        lines_arg = lines
        bytes_arg = bytes
        skip_beginning = False
        if lines and lines.startswith("+"):
            lines_arg = lines[1:]
            skip_beginning = True
        elif bytes and bytes.startswith("+"):
            bytes_arg = bytes[1:]
            skip_beginning = True

        console.print(client.tail(system, path, bytes_arg, lines_arg, skip_beginning))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def whoami(
    config_from_parent: str = typer.Option(None,
        callback=config_parent_load_callback,
        is_eager=True,
        hidden=True
    ),
    system: str = typer.Option(
        None,
        "-s",
        "--system",
        help="The name of the system where the `whoami` command will run.",
        envvar="FIRECREST_SYSTEM",
    ),
):
    """Return the username that FirecREST will be using to perform the other calls.
    If the name of the system is not passed the username will be deduced from the token.
    """
    try:
        console.print(client.whoami(system))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


# @app.command(rich_help_panel="Storage commands")
# def download(
#     config_from_parent: str = typer.Option(None,
#         callback=config_parent_load_callback,
#         is_eager=True,
#         hidden=True
#     ),
#     system: str = typer.Option(
#         ...,
#         "-s",
#         "--system",
#         help="The name of the system where the source filesystem belongs to.",
#         envvar="FIRECREST_SYSTEM",
#     ),
#     source: str = typer.Argument(..., help="The absolute source path."),
#     destination: Optional[str] = typer.Argument(
#         None,
#         help="The destination path (can be relative). It is required only when the download is `direct`.",
#     ),
#     transfer_type: TransferType = typer.Option(
#         TransferType.direct,
#         "--type",
#         case_sensitive=False,
#         help=f"Select type of transfer.",
#     ),
# ):
#     """Download a file

#     Direct download will download the file to the DESTINATION but it will work only for small files.
#     You can find the maximum size in UTILITIES_MAX_FILE_SIZE by running the `parameters` command.

#     External download will return with a link in case of success.
#     The file can be downloaded locally from there without any authentication.
#     """
#     try:
#         if transfer_type == TransferType.direct:
#             if destination:
#                 client.simple_download(system, source, destination)
#             else:
#                 console.print("`destination` is required when the ")
#                 raise typer.Exit(code=1)
#         elif transfer_type == TransferType.external:
#             down_obj = client.external_download(system, source)
#             console.print(
#                 f"Follow the status of the transfer asynchronously with that task ID: [green]{down_obj.task_id}[/green]"
#             )
#             with console.status(
#                 "Moving file to the staging area... It is safe to "
#                 "cancel the command and follow up through the task."
#             ):
#                 console.out(f"Download the file from:\n{down_obj.object_storage_link}")
#     except Exception as e:
#         examine_exeption(e)
#         raise typer.Exit(code=1)


# @app.command(rich_help_panel="Storage commands")
# def upload(
#     config_from_parent: str = typer.Option(None,
#         callback=config_parent_load_callback,
#         is_eager=True,
#         hidden=True
#     ),
#     system: str = typer.Option(
#         ...,
#         "-s",
#         "--system",
#         help="The name of the system where the source filesystem belongs to.",
#         envvar="FIRECREST_SYSTEM",
#     ),
#     source: str = typer.Argument(..., help="The source path (can be relative)."),
#     destination_directory: str = typer.Argument(
#         ..., help="The absolute destination path."
#     ),
#     filename: Optional[str] = typer.Argument(
#         None,
#         help="The name of the file in the system (by default it will be same as the local file). It works only for a direct upload.",
#     ),
#     transfer_type: TransferType = typer.Option(
#         TransferType.direct,
#         "--type",
#         case_sensitive=False,
#         help=f"Select type of transfer.",
#     ),
# ):
#     """Upload a file

#     Direct upload will upload the file to the DESTINATION directory but it will work only for small files.
#     You can find the maximum size in UTILITIES_MAX_FILE_SIZE by running the `parameters` command.

#     External download will return with a command that will need to be run in case of success.
#     The file can be uploaded to a stage area without any authentication and FirecREST will move the file to the cluster's filesystem as soon as it finished.
#     """
#     try:
#         if transfer_type == TransferType.direct:
#             client.simple_upload(system, source, destination_directory, filename)
#         elif transfer_type == TransferType.external:
#             up_obj = client.external_upload(system, source, destination_directory)
#             console.print(
#                 f"Follow the status of the transfer asynchronously with that task ID: [green]{up_obj.task_id}[/green]"
#             )
#             with console.status(
#                 "Waiting for Presigned URL to upload file to staging "
#                 "area... It is safe to cancel the command and follow "
#                 "up through the task."
#             ):
#                 data = up_obj.object_storage_data
#                 if "command" in data:
#                     console.print(
#                         "\nNecessary information to upload the file in the staging area:"
#                     )
#                     console.print(f"[yellow]{json.dumps(data['parameters'], indent=4)}[/yellow]")
#                     console.print(
#                         "\nOr simply run the following command to finish the upload:"
#                     )
#                     console.print(f"[green]{data['command']}[/green]")
#                 else:
#                     console.print(json.dumps(data, indent=4))
#     except Exception as e:
#         examine_exeption(e)
#         raise typer.Exit(code=1)


def validate_env_var_format(value: List[str]):
    pattern = re.compile(r'\S+=\S+')
    for item in value:
        if not pattern.match(item):
            raise typer.BadParameter(f"Please use the format `VAR=VALUE`.")

    return value


@app.command(rich_help_panel="Compute commands")
def submit(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    working_dir: str = typer.Option(
        ..., help="working directory of the job."
    ),
    job_script: str = typer.Argument(
        ...,
        help="The path of the local script",
    ),
    env_vars: List[str] = typer.Option(
        [], "-e", "--env-var",
        metavar="VAR=VALUE",
        help="Environment variable to be exported in the environment where the job script will be submitted (format `VAR=VALUE`).",
        callback=validate_env_var_format
    ),
):
    """Submit a batch script to the workload manager of the target system"""
    envvars = {}
    for var_value_pair in env_vars:
        var, val = var_value_pair.split('=', 1)
        envvars[var] = val

    try:
        result = client.submit(
            system_name=system,
            script_local_path=job_script,
            working_dir=working_dir,
            env_vars=envvars
        )
        console.print(json.dumps(result, indent=4))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def job_info(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    jobs: Optional[List[str]] = typer.Argument(
        None, help="List of job IDs to display."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
):
    """Retrieve information about submitted jobs.
    """
    try:
        result = client.job_info(system)
        if jobs:
            raise NotImplementedError("Job filtering not implemented yet")

        if raw:
            console.print(json.dumps(result, indent=4))
        else:
            raise NotImplementedError("Rich table not implemented yet")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def job_metadata(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    job: str = typer.Argument(
        ..., help="Job id."
    ),
    raw: bool = typer.Option(True, "--json", "--raw", help="Print the output in json format."),
):
    """Retrieve metadata for a current job.
    """
    try:
        result = client.metadat(system, jobid)
        if raw:
            console.print(json.dumps(result, indent=4))
        else:
            raise NotImplementedError("Rich table not implemented yet")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def cancel(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    job: str = typer.Argument(..., help="The ID of the job that will be cancelled."),
):
    """Cancel job"""
    try:
        client.cancel_job(system, job)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def attach_to_job(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    job: str = typer.Argument(..., help="The ID of the job that will be cancelled."),
    command: str = typer.Argument(..., help="The command that will be executed in the context of the job."),
):
    """Attach a process to a job."""
    try:
        client.attach_to_job(system, job, command)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.callback()
def main(
    config: Optional[str] = typer.Option(
        None,
        envvar="FIRECREST_CONFIG",
        callback=config_callback,
        is_eager=True,
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
    firecrest_url: str = typer.Option(
        ..., help="FirecREST URL.", envvar="FIRECREST_URL"
    ),
    client_id: str = typer.Option(
        ..., help="Registered client ID.", envvar="FIRECREST_CLIENT_ID"
    ),
    client_secret: str = typer.Option(
        ..., help="Secret for the client.", envvar="FIRECREST_CLIENT_SECRET"
    ),
    token_url: str = typer.Option(
        ...,
        help="URL of the token request in the authorization server (e.g. https://auth.com/auth/.../openid-connect/token).",
        envvar="AUTH_TOKEN_URL",
    ),
    api_version: str = typer.Option(
        None,
        help="Set the version of the api of firecrest. By default it will be assumed that you are using version 1.13.1 or "
        "compatible. The version is parsed by the `packaging` library.",
        envvar="FIRECREST_API_VERSION",
    ),
    verbose: Optional[bool] = typer.Option(
        None, "-v", "--verbose", help="Enable verbose mode."
    ),
    timeout: Optional[float] = typer.Option(
        None,
        help="How many seconds to wait for the FirecREST server to send data before giving up.",
    ),
    auth_timeout: Optional[float] = typer.Option(
        None,
        help="How many seconds to wait for the authorization server to send data before giving up.",
    ),
    debug: Optional[bool] = typer.Option(None, help="Enable debug mode."),
):
    """
    CLI for FirecREST

    Before running you need to setup the following variables or pass them as required options:
    - FIRECREST_URL: FirecREST URL
    - FIRECREST_CLIENT_ID: registered client ID
    - FIRECREST_CLIENT_SECRET: secret for the client
    - AUTH_TOKEN_URL: URL for the token request in the authorization server (e.g. https://auth.your-server.com/auth/.../openid-connect/token)
    """
    global client
    auth_obj = fc.ClientCredentialsAuth(client_id, client_secret, token_url)
    auth_obj.timeout = auth_timeout
    client = fc.v2.Firecrest(firecrest_url=firecrest_url, authorization=auth_obj)
    client.timeout = timeout
    if api_version:
        client.set_api_version(api_version)

    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(message)s",
            handlers=[RichHandler(console=console)],
        )
    elif verbose:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            handlers=[RichHandler(console=console)],
        )
    else:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(message)s",
            handlers=[RichHandler(console=console)],
        )


typer_click_object = typer.main.get_command(app)
