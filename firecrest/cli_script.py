#!/usr/bin/env python3
#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from firecrest import cli, __app_name__
from firecrest import cli2
import os


def main() -> None:
    # TODO: This is a temporary solution to support both API versions
    # in the same CLI script. We can have better support from the URL path or
    # the API response headers of v2.
    ver = os.environ.get("FIRECREST_API_VERSION")
    if ver and ver.startswith("1"):
        cli.app(prog_name=__app_name__)
    else:
        cli2.app(prog_name=__app_name__)


if __name__ == "__main__":
    main()
