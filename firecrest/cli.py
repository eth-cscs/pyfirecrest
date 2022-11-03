#
#  Copyright (c) 2019-2022, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import os
import typer

import firecrest as fc

from firecrest import __app_name__, __version__
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich import print

app = typer.Typer(
    rich_markup_mode="rich",
    # Disable printing locals to avoid printing the value of local
    # variables in order to hide secrets/password etc
    pretty_exceptions_show_locals=False,
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
    js: bool = typer.Option(False, "--json", help="Print in JSON format."),
):
    """List directory contents
    """
    try:
        result = client.list_files(machine, path, show_hidden)
        if js:
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
    js: bool = typer.Option(False, "--json", help="Print in JSON format."),
):
    """Use the stat linux application to determine the status of a file on the machine's filesystem
    """
    try:
        result = client.stat(machine, path, deref)
        if js:
            console.print(result)
        else:
            title = f"Status of file {path}"
            if deref:
                title += ' (dereferenced)'
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
