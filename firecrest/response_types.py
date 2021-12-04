"""Module containing the response types for the FireCrest endpoints."""
from typing import List
try:
    from typing import TypedDict
except ImportError:
    # not available in python<3.8
    from typing_extensions import TypedDict


class Utilities_File(TypedDict):
    """
    TypedDict for the utilities/file response.
    """
    description: str
    output: str  # output of https://en.wikipedia.org/wiki/File_(command)


class Utilities_Ls_Item(TypedDict):
    """
    TypedDict for a single item of utilities/ls response.
    """
    group: str
    last_modified: str
    link_target: str
    name: str
    permissions: str
    size: str
    type: str
    user: str


class Utilities_Ls(TypedDict):
    """
    TypedDict for the utilities/ls response.
    """
    description: str
    output: List[Utilities_Ls_Item]
