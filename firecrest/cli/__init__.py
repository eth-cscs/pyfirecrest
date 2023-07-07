#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import logging
import typer

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
    rich_markup_mode="rich",
    # Disable printing locals to avoid printing the value of local
    # variables in order to hide secrets/password etc
    pretty_exceptions_show_locals=False,
)
submit_template_app = typer.Typer(
    rich_markup_mode="rich",
    # Disable printing locals to avoid printing the value of local
    # variables in order to hide secrets/password etc
    pretty_exceptions_show_locals=False,
)
app.add_typer(
    submit_template_app,
    name="submit-template",
    rich_help_panel="Compute commands",
    help="""
    Create and submit a job for internal transfers

    Possible to stage-out jobs providing the SLURM ID of a production job.
    More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/
    """,
)
reservation_app = typer.Typer(
    rich_markup_mode="rich",
    # Disable printing locals to avoid printing the value of local
    # variables in order to hide secrets/password etc
    pretty_exceptions_show_locals=False,
)
app.add_typer(
    reservation_app,
    name="reservation",
    rich_help_panel="Compute commands",
    help="Create, list, update and delete reservations",
)

custom_theme = {
    "repr.attrib_name": "none",
    "repr.attrib_value": "none",
    "repr.number": "none",
}
console = Console(theme=Theme(custom_theme))
client: fc.Firecrest = None  # type: ignore
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


def create_table(table_title, data, *mappings):
    table = Table(title=table_title, box=box.ASCII)
    for (title, _) in mappings:
        table.add_column(title, overflow="fold")

    for i in data:
        table.add_row(*(str(i[key]) for (_, key) in mappings))

    return table


def version_callback(value: bool):
    if value:
        console.print(f"FirecREST CLI Version: {__version__}")
        raise typer.Exit()


@app.command(rich_help_panel="Status commands")
def services(
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="Get information for only one service."
    )
):
    """Provides information for the services of FirecREST
    """
    try:
        if name:
            result = [client.service(name)]
            title = f"Status of FirecREST service `{name}`"
        else:
            result = client.all_services()
            title = "Status of FirecREST services"

        table = create_table(
            title,
            result,
            ("Service", "service"),
            ("Status", "status"),
            ("Description", "description"),
        )
        console.print(table, overflow="fold")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def systems(
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="Get information for only one system."
    )
):
    """Provides information for the available systems in FirecREST
    """
    try:
        if name:
            result = [client.system(name)]
            title = f"Status of FirecREST system `{name}`"
        else:
            result = client.all_systems()
            title = "Status of FirecREST systems"

        table = create_table(
            title,
            result,
            ("System", "system"),
            ("Status", "status"),
            ("Description", "description"),
        )
        console.print(table, overflow="fold")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def parameters():
    """Configurable parameters of FirecREST
    """
    try:
        result = client.parameters()
        storage_table = create_table(
            "Storage parameters",
            result["storage"],
            ("Name", "name"),
            ("Value", "value"),
            ("Unit", "unit"),
        )

        utilities_table = create_table(
            "Utilities parameters",
            result["utilities"],
            ("Name", "name"),
            ("Value", "value"),
            ("Unit", "unit"),
        )

        console.print(storage_table, utilities_table, overflow="fold")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def tasks(
    taskids: Optional[List[str]] = typer.Argument(
        None, help="List of task IDs to display."
    ),
    pager: Optional[bool] = typer.Option(
        True, help="Display the output in a pager application."
    ),
):
    """Retrieve information about the FirecREST tasks of the users
    """
    try:
        result = client._tasks(taskids)
        num_results = len(result.values())
        table = create_table(
            f"Task information: {num_results} result{'s' if num_results > 1 else ''}",
            result.values(),
            ("Task ID", "hash_id"),
            ("Status", "status"),
            ("Description", "description"),
            ("Created at", "created_at"),
            ("Updated at", "updated_at"),
            ("User", "user"),
            ("Service", "service"),
            ("Data", "data"),
        )
        if pager:
            with console.pager():
                console.print(table)
        else:
            console.print(table)

    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def ls(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    show_hidden: bool = typer.Option(
        False,
        "-a",
        "--show-hidden",
        help="Include directory entries whose names begin with a dot (‘.’).",
    ),
    raw: bool = typer.Option(False, "--raw", help="Print unformatted."),
):
    """List directory contents
    """
    try:
        result = client.list_files(machine, path, show_hidden)
        if raw:
            console.print(result)
        else:
            table = create_table(
                f"Files in machine `{machine}` and path `{path}`",
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
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    p: bool = typer.Option(
        False,
        "-p",
        help="Create intermediate directories as required, equivalent to `-p` of the unix command.",
    ),
):
    """Create new directories
    """
    try:
        client.mkdir(machine, path, p)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def mv(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
):
    """Rename/move files, directory, or symlink at the `source_path` to the `target_path` on `machine`'s filesystem
    """
    try:
        client.mv(machine, source, destination)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chmod(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    mode: str = typer.Argument(..., help="Same as numeric mode of linux chmod tool."),
):
    """Change the file mod bits of a given file according to the specified mode
    """
    try:
        client.chmod(machine, path, mode)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chown(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    owner: Optional[str] = typer.Option(None, help="Owner ID for target."),
    group: Optional[str] = typer.Option(None, help="Group ID for target."),
):
    """Change the user and/or group ownership of a given file.

    If only owner or group information is passed, only that information will be updated.
    """
    try:
        client.chown(machine, path, owner, group)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def cp(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
):
    """Copy files
    """
    try:
        client.copy(machine, source, destination)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def file(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
):
    """Determine file type
    """
    try:
        console.print(client.file_type(machine, path))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def stat(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    deref: bool = typer.Option(False, "-L", "--dereference", help="Follow links."),
    raw: bool = typer.Option(False, "--raw", help="Print unformatted."),
):
    """Use the stat linux application to determine the status of a file on the machine's filesystem
    """
    try:
        result = client.stat(machine, path, deref)
        if raw:
            console.print(result)
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
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    target: str = typer.Argument(..., help="The path of the original file."),
    link_name: str = typer.Argument(..., help="The name of the link to the TARGET."),
):
    """Create a symbolic link
    """
    try:
        client.symlink(machine, target, link_name)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def rm(
    machine: str,
    path: str,
    force: bool = typer.Option(
        ...,
        prompt="Are you sure you want to delete this entry?",
        help="Attempt to remove the files without prompting for confirmation, regardless of the file's permissions.",
    ),
    # TODO (?) add option to not display error to emulate `-f` from the rm command
):
    """Remove directory entries
    """
    try:
        if force:
            client.simple_delete(machine, path)
        else:
            console.print("Operation cancelled")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def checksum(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
):
    """Calculate the SHA256 (256-bit) checksum
    """
    try:
        console.print(client.checksum(machine, path))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def head(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    lines: int = typer.Option(
        None, "-n", "--lines", help="Print count lines of each of the specified files."
    ),
    bytes: int = typer.Option(
        None, "-c", "--bytes", help="Print bytes of each of the specified files."
    ),
    reverse: bool = typer.Option(
        False, help="Print bytes of each of the specified files."
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
        console.print(client.head(machine, path, bytes, lines, reverse))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def tail(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    lines: int = typer.Option(
        None, "-n", "--lines", help="Print count lines of each of the specified files."
    ),
    bytes: int = typer.Option(
        None, "-c", "--bytes", help="Print bytes of each of the specified files."
    ),
    reverse: bool = typer.Option(
        False, help="Print bytes of each of the specified files."
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
        console.print(client.tail(machine, path, bytes, lines, reverse))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def whoami():
    """Return the username that FirecREST will be using to perform the other calls
    """
    try:
        console.print(client.whoami())
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


class TransferType(str, Enum):
    direct = "direct"
    external = "external"


@app.command(rich_help_panel="Storage commands")
def download(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: Optional[str] = typer.Argument(
        None,
        help="The destination path (can be relative). It is required only when the download is `direct`.",
    ),
    transfer_type: TransferType = typer.Option(
        TransferType.direct,
        "--type",
        case_sensitive=False,
        help=f"Select type of transfer.",
    ),
):
    """Download a file

    Direct download will download the file to the DESTINATION but it will work only for small files.
    You can find the maximum size in UTILITIES_MAX_FILE_SIZE by running the `parameters` command.

    External download will return with a link in case of success.
    The file can be downloaded locally from there without any authentication.
    """
    try:
        if transfer_type == TransferType.direct:
            if destination:
                client.simple_download(machine, source, destination)
            else:
                console.print("`destination` is required when the ")
                raise typer.Exit(code=1)
        elif transfer_type == TransferType.external:
            down_obj = client.external_download(machine, source)
            console.print(
                f"Follow the status of the transfer asynchronously with that task ID: [green]{down_obj.task_id}[/green]"
            )
            with console.status(
                "Moving file to the staging area... It is safe to "
                "cancel the command and follow up through the task."
            ):
                console.out(f"Download the file from:\n{down_obj.object_storage_link}")
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Storage commands")
def upload(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The source path (can be relative)."),
    destination_directory: str = typer.Argument(
        ..., help="The absolute destination path."
    ),
    filename: Optional[str] = typer.Argument(
        None,
        help="The name of the file in the machine (by default it will be same as the local file). It works only for a direct upload.",
    ),
    transfer_type: TransferType = typer.Option(
        TransferType.direct,
        "--type",
        case_sensitive=False,
        help=f"Select type of transfer.",
    ),
):
    """Upload a file

    Direct upload will upload the file to the DESTINATION directory but it will work only for small files.
    You can find the maximum size in UTILITIES_MAX_FILE_SIZE by running the `parameters` command.

    External download will return with a command that will need to be run in case of success.
    The file can be uploaded to a stage area without any authentication and FirecREST will move the file to the cluster's filesystem as soon as it finished.
    """
    try:
        if transfer_type == TransferType.direct:
            client.simple_upload(machine, source, destination_directory, filename)
        elif transfer_type == TransferType.external:
            up_obj = client.external_upload(machine, source, destination_directory)
            console.print(
                f"Follow the status of the transfer asynchronously with that task ID: [green]{up_obj.task_id}[/green]"
            )
            with console.status(
                "Waiting for Presigned URL to upload file to staging "
                "area... It is safe to cancel the command and follow "
                "up through the task."
            ):
                data = up_obj.object_storage_data
                if "command" in data:
                    console.print(
                        "\nNecessary information to upload the file in the staging area:"
                    )
                    console.print(f"[yellow]{data['parameters']}[/yellow]")
                    console.print(
                        "\nOr simply run the following command to finish the upload:"
                    )
                    console.print(f"[green]{data['command']}[/green]")
                else:
                    console.print(data)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def submit(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    job_script: str = typer.Argument(
        ...,
        help="The path of the script (if it's local it can be relative path, if it is on the machine it has to be the absolute path)",
    ),
    account: Optional[str] = typer.Option(
        None,
        "--account",
        help="Charge resources used by this job to specified account.",
    ),
    local: Optional[bool] = typer.Option(
        True,
        help="The batch file can be local (default) or on the machine's filesystem.",
    ),
):
    """Submit a batch script to the workload manger of the target system
    """
    try:
        console.print(client.submit(machine, job_script, local, account=account))
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@submit_template_app.command("mv")
def submit_mv(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    job_name: Optional[str] = typer.Option(None, help="Job name in the script."),
    time: Optional[str] = typer.Option(
        None,
        help="""
        Limit on the total run time of the job.

        Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        """,
    ),
    jobid: Optional[str] = typer.Option(
        None, help="Transfer data after job with ID JOBID is completed."
    ),
    account: Optional[str] = typer.Option(
        None,
        help="""
        Name of the project account to be used in SLURM script.
        If not set, system default is taken.
        """,
    ),
):
    """Move/rename file
    """
    try:
        console.print(
            client.submit_move_job(
                machine, source, destination, job_name, time, jobid, account
            )
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@submit_template_app.command("cp")
def submit_cp(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    job_name: Optional[str] = typer.Option(None, help="Job name in the script."),
    time: Optional[str] = typer.Option(
        None,
        help="""
        Limit on the total run time of the job.

        Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        """,
    ),
    jobid: Optional[str] = typer.Option(
        None, help="Transfer data after job with ID JOBID is completed."
    ),
    account: Optional[str] = typer.Option(
        None,
        help="""
        Name of the project account to be used in SLURM script.
        If not set, system default is taken.
        """,
    ),
):
    """Copy file
    """
    try:
        console.print(
            client.submit_copy_job(
                machine, source, destination, job_name, time, jobid, account
            )
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@submit_template_app.command("rsync")
def submit_rsync(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    source: str = typer.Argument(..., help="The absolute source path."),
    destination: str = typer.Argument(..., help="The absolute destination path."),
    job_name: Optional[str] = typer.Option(None, help="Job name in the script."),
    time: Optional[str] = typer.Option(
        None,
        help="""
        Limit on the total run time of the job.

        Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        """,
    ),
    jobid: Optional[str] = typer.Option(
        None, help="Transfer data after job with ID JOBID is completed."
    ),
    account: Optional[str] = typer.Option(
        None,
        help="""
        Name of the project account to be used in SLURM script.
        If not set, system default is taken.
        """,
    ),
):
    """Transfer/synchronize files or directories efficiently between filesystems
    """
    try:
        console.print(
            client.submit_rsync_job(
                machine, source, destination, job_name, time, jobid, account
            )
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@submit_template_app.command("rm")
def submit_rm(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    job_name: Optional[str] = typer.Option(None, help="Job name in the script."),
    time: Optional[str] = typer.Option(
        None,
        help="""
        Limit on the total run time of the job.

        Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        """,
    ),
    jobid: Optional[str] = typer.Option(
        None, help="Transfer data after job with ID JOBID is completed."
    ),
    account: Optional[str] = typer.Option(
        None,
        help="""
        Name of the project account to be used in SLURM script.
        If not set, system default is taken.
        """,
    ),
):
    """Remove files
    """
    try:
        console.print(
            client.submit_delete_job(machine, path, job_name, time, jobid, account)
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def poll(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    jobs: Optional[List[str]] = typer.Argument(
        None, help="List of job IDs to display."
    ),
    start_time: Optional[str] = typer.Option(
        None,
        help="Start time (and/or date) of job's query. Allowed formats are `HH:MM\[:SS] \[AM|PM]` or `MMDD\[YY]` or `MM/DD\[/YY]` or `MM.DD\[.YY]` or `MM/DD\[/YY]-HH:MM\[:SS]` or `YYYY-MM-DD\[THH:MM\[:SS]]`.",
    ),
    end_time: Optional[str] = typer.Option(
        None,
        help="End time (and/or date) of job's query. Allowed formats are `HH:MM\[:SS] \[AM|PM]` or `MMDD\[YY]` or `MM/DD\[/YY]` or `MM.DD\[.YY]` or `MM/DD\[/YY]-HH:MM\[:SS]` or `YYYY-MM-DD\[THH:MM\[:SS]]`.",
    ),
    raw: bool = typer.Option(False, "--raw", help="Print unformatted."),
):
    """Retrieve information about submitted jobs.
    This call uses the `sacct` command
    """
    try:
        result = client.poll(machine, jobs, start_time, end_time)
        if raw:
            console.print(result)
        else:
            table = create_table(
                "Accounting data for jobs",
                result,
                ("Job ID", "jobid"),
                ("Name", "name"),
                ("Nodelist", "nodelist"),
                ("Nodes", "nodes"),
                ("Partition", "partition"),
                ("Start time", "start_time"),
                ("State", "state"),
                ("Time", "time"),
                ("Time left", "time_left"),
                ("User", "user"),
            )
            console.print(table)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def poll_active(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    jobs: Optional[List[str]] = typer.Argument(
        None, help="List of job IDs to display."
    ),
    raw: bool = typer.Option(False, "--raw", help="Print unformatted."),
):
    """Retrieves information about active jobs.
    This call uses the `squeue -u <username>` command
    """
    try:
        result = client.poll_active(machine, jobs)
        if raw:
            console.print(result)
        else:
            table = create_table(
                "Information about jobs in the queue",
                result,
                ("Job ID", "jobid"),
                ("Name", "name"),
                ("Nodelist", "nodelist"),
                ("Nodes", "nodes"),
                ("Partition", "partition"),
                ("Start time", "start_time"),
                ("State", "state"),
                ("Time", "time"),
                ("Time left", "time_left"),
                ("User", "user"),
            )
            console.print(table)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def cancel(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    job: str = typer.Argument(..., help="The ID of the job that will be cancelled."),
):
    """Cancel job
    """
    try:
        client.cancel(machine, job)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@reservation_app.command(rich_help_panel="Reservation commands")
def list(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    )
):
    """List all active reservations and their status
    """
    try:
        res = client.all_reservations(machine)
        console.print(res)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@reservation_app.command(rich_help_panel="Reservation commands")
def create(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    name: str = typer.Argument(..., help="The reservation name."),
    account: str = typer.Argument(
        ..., help="The account in SLURM to which the reservation is made for."
    ),
    num_nodes: str = typer.Argument(
        ..., help="The number of nodes needed for the reservation."
    ),
    node_type: str = typer.Argument(..., help="The node type."),
    start_time: str = typer.Argument(
        ..., help="The start time for reservation (YYYY-MM-DDTHH:MM:SS)."
    ),
    end_time: str = typer.Argument(
        ..., help="The end time for reservation (YYYY-MM-DDTHH:MM:SS)."
    ),
):
    """Create a reservation
    """
    try:
        client.create_reservation(
            machine, name, account, num_nodes, node_type, start_time, end_time
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@reservation_app.command(rich_help_panel="Reservation commands")
def update(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    name: str = typer.Argument(..., help="The reservation name."),
    account: str = typer.Argument(
        ..., help="The account in SLURM to which the reservation is made for."
    ),
    num_nodes: str = typer.Argument(
        ..., help="The number of nodes needed for the reservation."
    ),
    node_type: str = typer.Argument(..., help="The node type."),
    start_time: str = typer.Argument(
        ..., help="The start time for reservation (YYYY-MM-DDTHH:MM:SS)."
    ),
    end_time: str = typer.Argument(
        ..., help="The end time for reservation (YYYY-MM-DDTHH:MM:SS)."
    ),
):
    """Update a reservation
    """
    try:
        client.update_reservation(
            machine, name, account, num_nodes, node_type, start_time, end_time
        )
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@reservation_app.command(rich_help_panel="Reservation commands")
def delete(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    name: str = typer.Argument(..., help="The reservation name."),
):
    """Delete a reservation
    """
    try:
        client.delete_reservation(machine, name)
    except Exception as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.callback()
def main(
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
        help="Set the version of the api of firecrest. By default it will be assumed that you are using version 1.13.0 or "
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
    client = fc.Firecrest(firecrest_url=firecrest_url, authorization=auth_obj)
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
