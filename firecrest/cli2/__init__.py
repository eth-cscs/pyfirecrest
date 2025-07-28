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
    # else:
        # in case of FirecrestException and ClientsCredentialsException
        # we don't need to log again the exception
    msg += f": {e}"

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
def server_version():
    """Provides the exact version of the FirecREST server, when available."""
    try:
        result = client.server_version()
        if result is None:
            console.print("[yellow]Server version not available[/yellow]")
        else:
            console.print(result)

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def systems(
    pager: Optional[bool] = typer.Option(
        False, help="Display the output in a pager application."
    ),
):
    """Provides information for the available systems in FirecREST"""
    try:
        result = client.systems()
        if pager:
            with console.pager():
                console.print(json.dumps(result, indent=4))
        else:
            console.print(json.dumps(result, indent=4))

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_nodes(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    # nodes: Optional[List[str]] = typer.Argument(
    #     None, help="List of specific compute nodes to query."
    # ),
    pager: Optional[bool] = typer.Option(
        False, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the compute nodes of the scheduler.
    """
    try:
        results = client.nodes(system)
        # TODO filter by nodes (?)

        if pager:
            with console.pager():
                console.print(json.dumps(results, indent=4))

        console.print(json.dumps(results, indent=4))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_reservations(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    # reservations: Optional[List[str]] = typer.Argument(
    #     None, help="List of specific reservations to query."
    # ),
    pager: Optional[bool] = typer.Option(
        False, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the scheduler reservations.
    """
    try:
        results = client.reservations(system)
        # TODO filter by reservations (?)

        if pager:
            with console.pager():
                console.print(json.dumps(results, indent=4))

        else:
            console.print(json.dumps(results, indent=4))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def get_partitions(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    # partitions: Optional[List[str]] = typer.Argument(
    #     None, help="List of specific partitions to query."
    # ),
    pager: Optional[bool] = typer.Option(
        False, help="Display the output in a pager application."
    ),
):
    """Retrieves information about the scheduler partitions.
    """
    try:
        results = client.partitions(system)
        # TODO filter by partitions (?)

        if pager:
            with console.pager():
                console.print(json.dumps(results, indent=4))
        else:
            console.print(json.dumps(results, indent=4))

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def id(
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the `id` command will run.",
        envvar="FIRECREST_SYSTEM",
    ),
    raw: bool = typer.Option(False, "--json", help="Print the output in json format."),
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
    numeric_uid_gid: bool = typer.Option(
        False,
        "-n",
        "--numeric-uid-gid",
        help="Print numeric UID and GID instead of names.",
    ),
    dereference: bool = typer.Option(
        False,
        "-L",
        help="When showing file information for a symbolic link, show information for the file the link references rather than for the link itself.",
    ),
):
    """List directory contents"""
    try:
        result = client.list_files(
            system,
            path,
            show_hidden,
            recursive,
            numeric_uid_gid,
            dereference
        )
        console.print(json.dumps(result, indent=4))

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def mkdir(
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
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    account: Optional[str] = typer.Option(None, help="The account to use for the operation."),
):
    """Rename/move files, directory, or symlink at the `source_path` to the `target_path` on `system`'s filesystem"""
    try:
        client.mv(system, source, destination, account)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chmod(
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
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    owner: str = typer.Option(None, help="Owner ID for target."),
    group: str = typer.Option(None, help="Group ID for target."),
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
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    dereference: bool = typer.Option(
        False,
        "-L",
        "--dereference",
        help="When copying a symbolic link, copy the file or directory the link points to rather than the link itself.",
    ),
    account: Optional[str] = typer.Option(None, help="The account to use for the operation."),
):
    """Copy files"""
    try:
        client.cp(system, source, destination, dereference=dereference, account=account)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def compress(
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    dereference: bool = typer.Option(
        False,
        help="When compressing a symbolic link, compress the file or directory the link points to rather than the link itself.",
    ),
):
    """Compress files using gzip compression.
    """
    try:
        client.compress(
            system,
            source,
            destination,
            dereference=dereference,
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def extract(
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
    """Extract files.
    """
    try:
        client.extract(system, source, destination)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def file(
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
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    deref: bool = typer.Option(False, "-L", "--dereference", help="Follow links."),
):
    """Use the stat linux application to determine the status of a file on the system's filesystem"""
    try:
        result = client.stat(system, path, deref)
        console.print(json.dumps(result, indent=4))

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def symlink(
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
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    account: Optional[str] = typer.Option(None, help="The account to use for the operation."),
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
            client.rm(system, path, account)
        else:
            console.print("Operation cancelled")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def checksum(
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
        result = client.checksum(system, path)
        console.print(json.dumps(
            result,
            indent=4
        ))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def head(
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
    raw: bool = typer.Option(False, "--json", help="Print the output in json format."),
):
    """Display the beginning of a specified file.
    By default the first 10 lines will be returned.
    Bytes and lines cannot be specified simultaneously.
    """
    # TODO: add info about the max size of the file
    if lines and bytes:
        console.print(
            f"[red]{__app_name__} head: cannot specify both 'bytes' and 'lines'[/red]"
        )
        raise typer.Exit(code=1)

    try:
        lines_arg = int(lines) if lines is not None else None
        bytes_arg = int(bytes) if bytes is not None else None
        skip_ending = False
        if lines and lines.startswith("-"):
            lines_arg = abs(lines_arg)
            skip_ending = True
        elif bytes and bytes.startswith("-"):
            bytes_arg = abs(bytes_arg)
            skip_ending = True

        result = client.head(
            system,
            path,
            bytes_arg,
            lines_arg,
            skip_ending
        )
        if raw:
            console.print(
                json.dumps(
                    result,
                    indent=4
                )
            )
        else:
            console.print(result["content"])
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def tail(
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
    raw: bool = typer.Option(False, "--json", help="Print the output in json format."),
):
    """Display the end of a specified file.
    By default the last 10 lines will be returned.
    Bytes and lines cannot be specified simultaneously.
    """
    # TODO: add info about the max size of the file
    if lines and bytes:
        console.print(
            f"[red]{__app_name__} tail: cannot specify both 'bytes' and 'lines'[/red]"
        )
        raise typer.Exit(code=1)

    try:
        lines_arg = int(lines) if lines is not None else None
        bytes_arg = int(bytes) if bytes is not None else None
        skip_beginning = False
        if lines and lines.startswith("+"):
            lines_arg = abs(lines_arg)
            skip_beginning = True
        elif bytes and bytes.startswith("+"):
            bytes_arg = abs(bytes_arg)
            skip_beginning = True

        result = client.tail(
            system,
            path,
            bytes_arg,
            lines_arg,
            skip_beginning
        )

        if raw:
            console.print(
                json.dumps(
                    result,
                    indent=4
                )
            )
        else:
            console.print(
                result["content"]
            )

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Storage commands")
def download(
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the source filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(
        None,
        help="The destination path (can be relative).",
    ),
    account: Optional[str] = typer.Option(None, help="The account to use for the operation."),
):
    """Download a file
    """
    try:
        client.download(
            system,
            source,
            destination,
            account=account,
            blocking=True
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Storage commands")
def upload(
    system: str = typer.Option(
        ...,
        "-s",
        "--system",
        help="The name of the system where the source filesystem belongs to.",
        envvar="FIRECREST_SYSTEM",
    ),
    source: str = typer.Argument(..., help="The source path (can be relative)."),
    destination_directory: str = typer.Argument(
        ..., help="The absolute destination path."
    ),
    filename: str = typer.Argument(
        None,
        help="The name of the file in the system.",
    ),
    account: Optional[str] = typer.Option(None, help="The account to use for the operation."),
):
    """Upload a file
    """
    try:
        client.upload(
            system,
            source,
            destination_directory,
            filename,
            account=account,
            blocking=True
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


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
        ..., help="Working directory of the job."
    ),
    job_script: str = typer.Argument(
        ...,
        help="The path of the job script. Treated as remote if it starts with 'remote://'.",
    ),
    env_vars: List[str] = typer.Option(
        [], "-e", "--env-var",
        metavar="VAR=VALUE",
        help="Environment variable to be exported in the environment where the job script will be submitted (format `VAR=VALUE`).",
        callback=validate_env_var_format
    ),
    account: Optional[str] = typer.Option(
        None,
        help="The account to use for the operation. If not specified, the default account will be used.",
    )
):
    """Submit a batch script to the workload manager of the target system."""

    def is_remote_script(p: str) -> bool:
        return p.startswith("remote://")

    envvars = {var.split("=", 1)[0]: var.split("=", 1)[1] for var in env_vars}

    try:
        if is_remote_script(job_script):
            p = job_script.split("://", 1)[1]
            result = client.submit(
                system_name=system,
                script_remote_path=p,
                working_dir=working_dir,
                env_vars=envvars,
                account=account,
            )
        else:
            result = client.submit(
                system_name=system,
                script_local_path=job_script,
                working_dir=working_dir,
                env_vars=envvars,
                account=account,
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
    # jobs: Optional[List[str]] = typer.Argument(
    #     None, help="List of job IDs to display."
    # ),
):
    """Retrieve information about submitted jobs.
    """
    try:
        result = client.job_info(system)
        # if jobs:
        #     raise NotImplementedError("Job filtering not implemented yet")

        console.print(json.dumps(result, indent=4))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def wait_for_job(
    system: str = typer.Option(
        ..., "-s", "--system", help="The name of the system.", envvar="FIRECREST_SYSTEM"
    ),
    jobid: str = typer.Argument(
        ..., help="Job id of the job to wait for."
    ),
):
    """Wait for a job to complete. It will return the job information when the job is completed.
    """
    try:
        result = client.wait_for_job(system, jobid)
        console.print(json.dumps(result, indent=4))
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
):
    """Retrieve metadata for a current job.
    """
    try:
        result = client.job_metadata(system, job)
        console.print(json.dumps(result, indent=4))
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
