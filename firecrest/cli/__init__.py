#
#  Copyright (c) 2019-2022, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import typer

import firecrest as fc

from firecrest import __app_name__, __version__
from typing import List, Optional
from enum import Enum

from rich.console import Console
from rich.table import Table
from rich import print

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

console = Console()
client = None


def examine_exeption(e):
    if isinstance(e, fc.ClientsCredentialsException):
        console.print(
            f"[red]{__app_name__}: Operation failed: could not fetch token[/red]"
        )

    console.print(e)


def version_callback(value: bool):
    if value:
        print(f"FirecREST CLI Version: {__version__}")
        raise typer.Exit()


@app.command(rich_help_panel="Setup commands")
def login():
    """Does nothing but maybe it can setup the env vars(?)
    """
    pass


@app.command(rich_help_panel="Setup commands")
def logout():
    """Does nothing but maybe it can unsetup the env vars(?)
    """
    pass


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

        table = Table(title=title)
        table.add_column("Service")
        table.add_column("Status")
        table.add_column("Description")
        for i in result:
            table.add_row(i["service"], i["status"], i["description"])

        console.print(table)
    except fc.FirecrestException as e:
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

        table = Table(title=title)
        table.add_column("System")
        table.add_column("Status")
        table.add_column("Description")
        for i in result:
            table.add_row(i["system"], i["status"], i["description"])

        console.print(table)
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Status commands")
def parameters():
    """Configurable parameters of FirecREST
    """
    try:
        all_results = client.parameters()
        title = "Storage parameters"
        table1 = Table(title=title)
        table1.add_column("Name")
        table1.add_column("Value")
        table1.add_column("Unit")
        for i in all_results["storage"]:
            table1.add_row(i["name"], str(i["value"]), i["unit"])

        title = "Utilities parameters"
        table2 = Table(title=title)
        table2.add_column("Name")
        table2.add_column("Value")
        table2.add_column("Unit")
        for i in all_results["utilities"]:
            table2.add_row(i["name"], str(i["value"]), i["unit"])

        console.print(table1, table2)
    except fc.FirecrestException as e:
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
            table = Table(title=f"Files in machine `{machine}` and path `{path}`")
            table.add_column("filename")
            table.add_column("type")
            table.add_column("group")
            table.add_column("permissions")
            table.add_column("size")
            table.add_column("user")
            table.add_column("last_modified")
            table.add_column("link_target")
            for i in result:
                table.add_row(
                    i["name"],
                    i["type"],
                    i["group"],
                    i["permissions"],
                    i["size"],
                    i["user"],
                    i["last_modified"],
                    i["link_target"],
                )

            console.print(table)
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def chown(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
    owner: Optional[str] = typer.Argument(None, help="Owner ID for target."),
    group: Optional[str] = typer.Argument(None, help="Group ID for target."),
):
    """Change the user and/or group ownership of a given file.

    If only owner or group information is passed, only that information will be updated.
    """
    try:
        client.chown(machine, path, owner, group)
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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

            table = Table(title=title)
            table.add_column("Attribute")
            table.add_column("Value")
            table.add_column("Description")

            table.add_row("mode", str(result["mode"]), "access rights in octal")
            table.add_row("ino", str(result["ino"]), "inode number")
            table.add_row("dev", str(result["dev"]), "device number in decimal")
            table.add_row("nlink", str(result["nlink"]), "number of hard links")
            table.add_row("uid", str(result["uid"]), "user ID of owner")
            table.add_row("gid", str(result["gid"]), "group ID of owner")
            table.add_row("size", str(result["size"]), "total size, in bytes")
            table.add_row(
                "atime",
                str(result["atime"]),
                "time of last access, seconds since Epoch",
            )
            table.add_row(
                "mtime",
                str(result["mtime"]),
                "time of last data modification, seconds since Epoch",
            )
            table.add_row(
                "ctime",
                str(result["ctime"]),
                "time of last status change, seconds since Epoch",
            )

            console.print(table)
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def head(
    machine: str = typer.Argument(
        ..., help="The machine name where the filesystem belongs to."
    ),
    path: str = typer.Argument(..., help="The absolute target path."),
):
    """View the content of a specified file

    You view only files smaller than UTILITIES_MAX_FILE_SIZE bytes.
    This variable is available in the parameters command.
    """
    try:
        console.print(client.view(machine, path))
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Utilities commands")
def whoami():
    """Return the username that FirecREST will be using to perform the other calls
    """
    try:
        console.print(client.whoami())
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


class TransferType(str, Enum):
    direct = "direct"
    external = "external"
    smart = "smart"


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
        help=f"Select type of transfer (run `{__app_name__} download --help` for details).",
    ),
):
    """Download a file

    Direct download will download the file to the DESTINATION but it will work only for small files. You can find the maximum size by running the `parameters` command.

    External download will return with a link in case of success. The file can be downloaded locally from there without any authentication.
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
                console.print(f"Download the file from: {down_obj.object_storage_data}")
        else:
            console.print("ERROR: smart option is not implemented yet.")
            raise typer.Exit(code=1)
    except fc.FirecrestException as e:
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
        help="The name of the file in the machine (by default it will be same as the local file).",
    ),
    transfer_type: TransferType = typer.Option(
        TransferType.direct,
        "--type",
        case_sensitive=False,
        help=f"Select type of transfer (run `{__app_name__} upload --help` for details).",
    ),
):
    """Upload a file
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
                        "\nRun the following the following command to finish the upload:"
                    )
                    console.print(f"[green]{data['command']}[/green]")
                    console.print(
                        "\nYou can also use a different software to upload the file:"
                    )
                    console.print(f"[yellow]{data['parameters']}[/yellow]")
                else:
                    console.print(data)
        else:
            console.print("ERROR: smart option is not implemented yet.")
            raise typer.Exit(code=1)
    except fc.FirecrestException as e:
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
    local: Optional[bool] = typer.Option(
        True,
        help="The batch file can be local (default) or on the machine's filesystem.",
    ),
):
    """Submit a batch script to the workload manger of the target system
    """
    try:
        console.print(client.submit(machine, job_script, local))
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def poll(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    jobs: Optional[List[str]] = typer.Argument(
        None,
        help="The path of the script (if it's local it can be relative path, if it is on the machine it has to be the absolute path)",
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
            title = "Accounting data for jobs"
            table = Table(title=title)
            table.add_column("Job ID")
            table.add_column("Name")
            table.add_column("Nonelist")
            table.add_column("Nodes")
            table.add_column("Partition")
            table.add_column("Start time")
            table.add_column("State")
            table.add_column("Time")
            table.add_column("Time left")
            table.add_column("User")
            for i in result:
                table.add_row(
                    i["jobid"],
                    i["name"],
                    i["nodelist"],
                    i["nodes"],
                    i["partition"],
                    i["start_time"],
                    i["state"],
                    i["time"],
                    i["time_left"],
                    i["user"],
                )

            console.print(table)
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.command(rich_help_panel="Compute commands")
def poll_active(
    machine: str = typer.Argument(
        ..., help="The machine name where the source filesystem belongs to."
    ),
    jobs: Optional[List[str]] = typer.Argument(
        None,
        help="The path of the script (if it's local it can be relative path, if it is on the machine it has to be the absolute path)",
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
            title = "Information about jobs in the queue"
            table = Table(title=title)
            table.add_column("Job ID")
            table.add_column("Name")
            table.add_column("Nonelist")
            table.add_column("Nodes")
            table.add_column("Partition")
            table.add_column("Start time")
            table.add_column("State")
            table.add_column("Time")
            table.add_column("Time left")
            table.add_column("User")
            for i in result:
                table.add_row(
                    i["jobid"],
                    i["name"],
                    i["nodelist"],
                    i["nodes"],
                    i["partition"],
                    i["start_time"],
                    i["state"],
                    i["time"],
                    i["time_left"],
                    i["user"],
                )

            console.print(table)
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
        client.all_reservations(machine)
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
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
    except fc.FirecrestException as e:
        examine_exeption(e)
        raise typer.Exit(code=1)


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
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
    client = fc.Firecrest(firecrest_url=firecrest_url, authorization=auth_obj)
