"""A simple CLI for pyfirecrest."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import time

import click
import yaml

import firecrest as fc


class LazyConfig:
    """A configuration object, to lazy load the client."""

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self._params = None
        self._client = None
        self._default_machine = None

    @property
    def params(self) -> dict:
        if self._params is None:
            with self.file_path.open("rb") as f:
                config = yaml.safe_load(f)
            if not isinstance(config, dict):
                raise TypeError("Config file must be a dictionary")
            required = {
                "client_id",
                "client_secret",
                "token_uri",
                "client_url",
                "default_machine",
            }
            missing = required - set(config.keys())
            if missing:
                raise ValueError(f"Config file missing keys: {missing}")
            self._params = config
        return self._params

    @property
    def client(self) -> fc.Firecrest:
        if self._client is None:
            config = self.params
            auth = fc.ClientCredentialsAuth(
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                token_uri=config["token_uri"],
            )
            self._client = fc.Firecrest(
                firecrest_url=config["client_url"], authorization=auth
            )
            self._default_machine = config["default_machine"]

        return self._client

    @property
    def default_machine(self) -> str:
        return self.params["default_machine"]


pass_config = click.make_pass_decorator(LazyConfig, ensure=True)


MACHINE_OPTION = click.option(
    "-m", "--machine", type=str, default=None, help="Machine name"
)


def echo_success(message: str = "Success!"):
    click.secho(message, fg="green")


@click.group()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    default=".fclient.yaml",
    help="Path to the configuration file",
    show_default=True,
)
@click.pass_context
def main(ctx: click.Context, config):
    """A simple CLI for pyfirecrest."""
    ctx.ensure_object(dict)
    ctx.obj = LazyConfig(config)


@main.group()
def status():
    """`/status/` endpoints."""


@status.command()
@pass_config
def parameters(config: LazyConfig):
    """GET `/status/parameters`"""
    print(yaml.dump(config.client.parameters()))


@status.command()
@click.argument("name", required=False, type=str, default=None)
@pass_config
def services(config: LazyConfig, name: str | None):
    """GET `/status/services`"""
    if name:
        print(yaml.dump(config.client.service(name)))
    else:
        print(yaml.dump(config.client.all_services()))


@status.command()
@click.argument("name", required=False, type=str, default=None)
@pass_config
def systems(config: LazyConfig, name: str | None):
    """GET `/status/systems`"""
    if name:
        print(yaml.dump(config.client.system(name)))
    else:
        print(yaml.dump(config.client.all_systems()))


@main.group()
def utils():
    """`/utilities/` endpoints."""


@utils.command
@pass_config
def whoami(config: LazyConfig):
    """Print the username."""
    print(config.client.whoami())


@utils.command
@pass_config
def pwd(config: LazyConfig):
    """Print the users's home path."""
    # TODO: how can you get pwd? Needed for all storage endpoints!
    user_name = config.client.whoami()
    print(f"/home/{user_name}")


@utils.command()
@click.argument("path", type=str, required=False, default=".")
@click.option("-a", "--hidden", is_flag=True, help="Show hidden files")
@click.option("-l", "--long", is_flag=True, help="Long format")
@click.option("-R", "--recursive", is_flag=True, help="Recurse directories")
@click.option("--delimiter", type=str, default="/", help="Delimiter recursive joining")
@click.option(
    "--max-calls",
    type=int,
    default=100,
    help="Maximum API calls allowed during recursion",
)
@MACHINE_OPTION
@pass_config
def ls(
    config: LazyConfig,
    machine: str | None,
    path: str,
    hidden: bool,
    long: bool,
    delimiter: str,
    recursive: bool,
    max_calls: int,
):
    """GET `/utilities/ls`"""
    machine = machine or config.default_machine
    max_depth = None if recursive else 1
    for result in config.client.ls_recurse(
        machine,
        path,
        show_hidden=hidden,
        delimiter=delimiter,
        max_calls=max_calls,
        max_depth=max_depth,
    ):
        if long:
            print(f"- {json.dumps(result)}")
        else:
            print(
                "  " * (result.get("depth", 1) - 1)
                + result["name"]
                + ("/" if result.get("type") == "d" else "")
            )


@utils.command()
@click.argument("path", type=str)
@click.option("-p", "--parents", is_flag=True, help="Create parent directories")
@MACHINE_OPTION
@pass_config
def mkdir(config: LazyConfig, machine: str | None, path: str, parents: bool):
    """POST `/utilities/mkdir`"""
    config.client.mkdir(machine or config.default_machine, path, parents)
    echo_success()


@utils.command()
@click.argument("source_path", type=str)
@click.argument("target_path", type=str)
@MACHINE_OPTION
@pass_config
def rename(config: LazyConfig, machine: str | None, source_path: str, target_path: str):
    """PUT `/utilities/rename`"""
    config.client.mv(machine or config.default_machine, source_path, target_path)
    echo_success()


@utils.command()
@click.argument("target_path", type=str)
@click.argument("mode", type=str)
@MACHINE_OPTION
@pass_config
def chmod(config: LazyConfig, machine: str | None, target_path: str, mode: str):
    """PUT `/utilities/chmod`"""
    config.client.chmod(machine or config.default_machine, target_path, mode)
    echo_success()


@utils.command()
@click.argument("target_path", type=str)
@click.option("-o", "--owner", type=str, help="Owner username")
@click.option("-g", "--group", type=str, help="Group username")
@MACHINE_OPTION
@pass_config
def chown(
    config: LazyConfig,
    machine: str | None,
    target_path: str,
    owner: str | None,
    group: str | None,
):
    """PUT `/utilities/chown`"""
    config.client.chown(machine or config.default_machine, target_path, owner, group)
    echo_success()


@utils.command()
@click.argument("source_path", type=str)
@click.argument("target_path", type=str)
@MACHINE_OPTION
@pass_config
def copy(config: LazyConfig, machine: str | None, source_path: str, target_path: str):
    """POST `/utilities/copy`"""
    config.client.copy(machine or config.default_machine, source_path, target_path)
    echo_success()


@utils.command("file")
@click.argument("path", type=str)
@MACHINE_OPTION
@pass_config
def file_type(config: LazyConfig, machine: str | None, path: str):
    """GET `/utilities/file`"""
    print(config.client.file_type(machine or config.default_machine, path))


@utils.command()
@click.argument("path", type=str)
@click.option("-d", "--dereference", is_flag=True, help="Dereference symlinks")
@MACHINE_OPTION
@pass_config
def stat(config: LazyConfig, machine: str | None, path: str, dereference: bool):
    """GET `/utilities/stat`"""
    print(config.client.stat(machine or config.default_machine, path, dereference))


@utils.command()
@click.argument("target_path", type=str)
@click.argument("link_path", type=str)
@MACHINE_OPTION
@pass_config
def symlink(config: LazyConfig, machine: str | None, target_path: str, link_path: str):
    """POST `/utilities/symlink`"""
    config.client.symlink(machine or config.default_machine, target_path, link_path)
    echo_success()


@utils.command()
@click.argument("path", type=str)
@MACHINE_OPTION
@pass_config
def download(config: LazyConfig, machine: str | None, path: str):
    """GET `/utilities/download`"""
    config.client.simple_download(machine or config.default_machine, path)
    echo_success()


@utils.command()
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
@click.argument("target_dir", type=str, default=".", required=False)
@click.argument("target_name", type=str, required=False)
@MACHINE_OPTION
@pass_config
def upload(
    config: LazyConfig,
    machine: str | None,
    source: str,
    target_dir: str,
    target_name: str | None,
):
    """POST `/utilities/upload`"""
    config.client.simple_upload(
        machine or config.default_machine, source, target_dir, target_name
    )
    echo_success()


@utils.command()
@click.argument("path", type=str)
@MACHINE_OPTION
@pass_config
def rm(config: LazyConfig, machine: str | None, path: str):
    """DELETE `/utilities/rm`"""
    config.client.simple_delete(machine or config.default_machine, path)
    echo_success()


@utils.command()
@click.argument("path", type=str)
@MACHINE_OPTION
@pass_config
def checksum(config: LazyConfig, machine: str | None, path: str):
    """GET `/utilities/checksum`"""
    print(config.client.checksum(machine or config.default_machine, path))


@utils.command()
@click.argument("path", type=str)
@MACHINE_OPTION
@pass_config
def view(config: LazyConfig, machine: str | None, path: str):
    """GET `/utilities/view`"""
    print(config.client.view(machine or config.default_machine, path))


@main.group()
def compute():
    """`/compute/` endpoints."""


@compute.command()
@click.option("-j", "--jobs", type=str, help="Comma delimited list of job IDs")
@click.option("-s", "--start", type=str, help="Start date/time")
@click.option("-e", "--end", type=str, help="End date/time")
@MACHINE_OPTION
@pass_config
def acct(
    config: LazyConfig,
    machine: str | None,
    jobs: str | None,
    start: datetime | None,
    end: datetime | None,
):
    """GET `/compute/acct`"""
    jobs = jobs.split(",") if jobs else None
    print(config.client.poll(machine or config.default_machine, jobs, start, end))


@compute.command()
@click.option("-j", "--jobs", type=str, help="Comma delimited list of job IDs")
@MACHINE_OPTION
@pass_config
def jobs(config: LazyConfig, machine: str | None, jobs: str | None):
    """GET `/compute/jobs`"""
    jobs = jobs.split(",") if jobs else None
    print(config.client.poll_active(machine or config.default_machine, jobs))


@compute.command()
@click.argument("job", type=str)
@MACHINE_OPTION
@pass_config
def cancel(config: LazyConfig, machine: str | None, job: str):
    """DELETE `/compute/job/{jobid}`"""
    print(config.client.cancel(machine or config.default_machine, job))


@compute.command()
@click.argument("script_path", type=str)
@click.option("-l", "--local", is_flag=True, help="Whether the path is local")
@MACHINE_OPTION
@pass_config
def submit(config: LazyConfig, machine: str | None, script_path: str, local: bool):
    """POST `/compute/jobs/upload` or POST `/compute/jobs/path`"""
    print(config.client.submit(machine or config.default_machine, script_path, local))


@main.group("store")
def storage():
    """`/storage/` endpoints."""


@storage.command("rsync")
@click.argument("source_path", type=str)
@click.argument("target_path", type=str)
@click.option("-j", "--job-name", type=str, help="Name the job")
@click.option("-t", "--time", type=str, help="Time limit")
@click.option(
    "-s",
    "--stage-out-job-id",
    type=str,
    help="Move data after job with id {stageOutJobId} is completed",
)
@click.option(
    "-a",
    "--account",
    type=str,
    help="Name of scheduler account, otherwise use system default.",
)
@MACHINE_OPTION
@pass_config
def storage_rsync(
    config: LazyConfig,
    machine: str | None,
    source_path: str,
    target_path: str,
    job_name: str | None,
    time: str | None,
    stage_out_job_id: str | None,
    account: str | None,
):
    """POST `/storage/xfer-internal/rsync`"""
    config.client.submit_rsync_job(
        machine or config.default_machine,
        source_path,
        target_path,
        job_name=job_name,
        time=time,
        stage_out_job_id=stage_out_job_id,
        account=account,
    )
    echo_success()


@storage.command("mv")
@click.argument("source_path", type=str)
@click.argument("target_path", type=str)
@click.option("-j", "--job-name", type=str, help="Name the job")
@click.option("-t", "--time", type=str, help="Time limit")
@click.option(
    "-s",
    "--stage-out-job-id",
    type=str,
    help="Move data after job with id {stageOutJobId} is completed",
)
@click.option(
    "-a",
    "--account",
    type=str,
    help="Name of scheduler account, otherwise use system default.",
)
@MACHINE_OPTION
@pass_config
def storage_mv(
    config: LazyConfig,
    machine: str | None,
    source_path: str,
    target_path: str,
    job_name: str | None,
    time: str | None,
    stage_out_job_id: str | None,
    account: str | None,
):
    """POST `/storage/xfer-internal/mv`"""
    config.client.submit_move_job(
        machine or config.default_machine,
        source_path,
        target_path,
        job_name=job_name,
        time=time,
        stage_out_job_id=stage_out_job_id,
        account=account,
    )
    echo_success()


@storage.command("cp")
@click.argument("source_path", type=str)
@click.argument("target_path", type=str)
@click.option("-j", "--job-name", type=str, help="Name the job")
@click.option("-t", "--time", type=str, help="Time limit")
@click.option(
    "-s",
    "--stage-out-job-id",
    type=str,
    help="Move data after job with id {stageOutJobId} is completed",
)
@click.option(
    "-a",
    "--account",
    type=str,
    help="Name of scheduler account, otherwise use system default.",
)
@MACHINE_OPTION
@pass_config
def storage_cp(
    config: LazyConfig,
    machine: str | None,
    source_path: str,
    target_path: str,
    job_name: str | None,
    time: str | None,
    stage_out_job_id: str | None,
    account: str | None,
):
    """POST `/storage/xfer-internal/cp`"""
    config.client.submit_copy_job(
        machine or config.default_machine,
        source_path,
        target_path,
        job_name=job_name,
        time=time,
        stage_out_job_id=stage_out_job_id,
        account=account,
    )
    echo_success()


@storage.command("rm")
@click.argument("path", type=str)
@click.option("-j", "--job-name", type=str, help="Name the job")
@click.option("-t", "--time", type=str, help="Time limit")
@click.option(
    "-s",
    "--stage-out-job-id",
    type=str,
    help="Move data after job with id {stageOutJobId} is completed",
)
@click.option(
    "-a",
    "--account",
    type=str,
    help="Name of scheduler account, otherwise use system default.",
)
@MACHINE_OPTION
@pass_config
def storage_rm(
    config: LazyConfig,
    machine: str | None,
    path: str,
    job_name: str | None,
    time: str | None,
    stage_out_job_id: str | None,
    account: str | None,
):
    """POST `/storage/xfer-internal/rm`"""
    config.client.submit_delete_job(
        machine or config.default_machine,
        path,
        job_name=job_name,
        time=time,
        stage_out_job_id=stage_out_job_id,
        account=account,
    )
    echo_success()


@storage.command("upload")
@click.argument("path", type=str)
@click.argument("target_dir", type=str, default=".", required=False)
@click.option("-w", "--wait", is_flag=True, help="Wait for upload to finish")
@MACHINE_OPTION
@pass_config
def storage_upload(
    config: LazyConfig, machine: str | None, path: str, target_dir: str, wait: bool
):
    """POST `/storage/xfer-external/upload`"""
    up_obj = config.client.external_upload(
        machine or config.default_machine, path, target_dir
    )
    print("Uploading to object storage ...")
    up_obj.finish_upload()
    if not wait:
        echo_success()
        return
    print("Moving fom object storage ...")
    while up_obj.in_progress:
        time.sleep(1)
    echo_success()


@storage.command("download")
@click.argument("path", type=str)
@click.argument("target_dir", type=str, default=".", required=False)
@MACHINE_OPTION
@pass_config
def storage_download(
    config: LazyConfig, machine: str | None, path: str, target_dir: str
):
    """POST `/storage/xfer-external/download`"""
    down_obj = config.client.external_download(machine or config.default_machine, path)
    print("Moving to object storage...")
    down_obj.object_storage_data
    print("Downloading from object storage ...")
    down_obj.finish_download(target_dir + "/" + path.rsplit("/", 1)[-1])
    echo_success()


@storage.command()
@click.argument("task_id", type=str)
@pass_config
def invalidate(config: LazyConfig, task_id: str):
    """POST `/storage/xfer-external/invalidate`"""
    config.client._invalidate(task_id)
    echo_success()


# TODO how to remove files from object storage?


@main.group()
def tasks():
    """`/tasks` endpoints."""


@tasks.command()
@click.argument("tasks", type=str, required=False)
@pass_config
def tasks_get(config: LazyConfig, tasks: str | None):
    """GET `/tasks`"""
    print(yaml.safe_dump(config.client._tasks(tasks.split(",") if tasks else None)))


@main.group()
def reserve():
    """`/reservations` endpoints."""


@reserve.command("get")
@MACHINE_OPTION
@pass_config
def reserve_get(config: LazyConfig, machine: str | None):
    """GET `/reservations`"""
    print(
        yaml.safe_dump(
            config.client.all_reservations(machine or config.default_machine)
        )
    )


@reserve.command("create")
@click.argument("reservation", type=str)
@click.argument("account", type=str)
@click.argument("number_of_nodes", type=str)
@click.argument("node_type", type=str)
@click.argument("start_time", type=str)
@click.argument("end_time", type=str)
@MACHINE_OPTION
@pass_config
def reserve_create(
    config: LazyConfig,
    machine: str | None,
    reservation: str,
    account: str,
    number_of_nodes: str,
    node_type: str,
    start_time: str,
    end_time: str,
):
    """POST `/reservations/{reservation}`"""
    config.client.create_reservation(
        machine or config.default_machine,
        reservation,
        account,
        number_of_nodes,
        node_type,
        start_time,
        end_time,
    )


@reserve.command("update")
@click.argument("reservation", type=str)
@click.argument("account", type=str)
@click.argument("number_of_nodes", type=str)
@click.argument("node_type", type=str)
@click.argument("start_time", type=str)
@click.argument("end_time", type=str)
@MACHINE_OPTION
@pass_config
def reserve_update(
    config: LazyConfig,
    machine: str | None,
    reservation: str,
    account: str,
    number_of_nodes: str,
    node_type: str,
    start_time: str,
    end_time: str,
):
    """PUT `/reservations/{reservation}`"""
    config.client.update_reservation(
        machine or config.default_machine,
        reservation,
        account,
        number_of_nodes,
        node_type,
        start_time,
        end_time,
    )


@reserve.command("delete")
@click.argument("reservation", type=str)
@MACHINE_OPTION
@pass_config
def reserve_delete(config: LazyConfig, machine: str | None, reservation: str):
    """DELETE `/reservations/{reservation}`"""
    config.client.delete_reservation(machine or config.default_machine, reservation)


if __name__ == "__main__":
    main(help_option_names=["-h", "--help"])
