#!/usr/bin/env python3
#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from firecrest import cli, __app_name__


def main() -> None:
    cli.app(prog_name=__app_name__)


if __name__ == "__main__":
    main()
