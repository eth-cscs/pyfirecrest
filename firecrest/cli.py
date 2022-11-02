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
        None, "-n", "--name", help="Get information for only one service"
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
        None, "-n", "--name", help="Get information for only one system"
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
        ..., help="The machine name where the filesystem belongs to"
    ),
    path: str = typer.Argument(..., help="The absolute target path"),
    show_hidden: bool = typer.Option(
        False,
        "-a",
        "--show-hidden",
        help="Include directory entries whose names begin with a dot (‘.’).",
    ),
):
    """List directory contents
    """
    try:
        result = client.list_files(machine, path, show_hidden)
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


if __name__ == "__main__":
    app()
