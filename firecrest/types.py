"""Types returned by the API.

See also: https://firecrest-api.cscs.ch
"""
from __future__ import annotations

import sys
from typing import Any

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


class Parameter(TypedDict):
    """A parameter record, from `status/parameters/{name}`"""

    name: str
    unit: str
    value: Any


class Parameters(TypedDict):
    """A parameters record, from `status/parameters`"""

    storage: list[Parameter]
    utilities: list[Parameter]


class Service(TypedDict):
    """A service record, from `status/services/{name}`"""

    description: str
    service: str
    status: str
    status_code: int


class System(TypedDict):
    """A system record, from `status/systems/{name}`"""

    description: str
    status: str
    system: str


class Task(TypedDict):
    """A task record, from `/tasks`"""

    created_at: str
    data: Any
    description: str
    hash_id: str
    last_modify: str
    service: str
    status: str
    task_id: str
    updated_at: str
    user: str


class LsFile(TypedDict):
    """A file listing record, from `utilities/ls`"""

    group: str
    last_modified: str
    link_target: str
    name: str
    permissions: str
    size: str
    type: str
    user: str


class StatFile(TypedDict):
    """A file stat record, from `utilities/stat`

    Command is `stat {deref} -c '%a %i %d %h %u %g %s %X %Y %Z`

    See also https://docs.python.org/3/library/os.html#os.stat_result
    """

    atime: int
    ctime: int
    dev: int  # device
    gid: int  # group id of owner
    ino: int  # inode number
    mode: int  # protection bits
    mtime: int
    nlink: int  # number of hard links
    size: int  # size of file, in bytes
    uid: int  # user id of owner


class JobAcct(TypedDict):
    """A job accounting record, from `compute/acct`"""

    jobid: str
    name: str
    nodelist: str
    nodes: str
    partition: str
    start_time: str
    state: str
    time: str
    time_left: str
    user: str


class JobQueue(TypedDict):
    """A job queue record, from `compute/jobs`"""

    job_data_err: str
    job_data_out: str
    job_file: str
    job_file_err: str
    job_file_out: str
    jobid: str
    name: str
    nodelist: str
    nodes: str
    partition: str
    start_time: str
    state: str
    time: str
    time_left: str
    user: str


class JobSubmit(TypedDict):
    """A job submit record, from `compute/jobs`"""

    firecrest_taskid: str
    job_data_err: str
    job_data_out: str
    job_file: str
    job_file_err: str
    job_file_out: str
    jobid: int
    result: str
