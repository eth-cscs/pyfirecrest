#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import sys


__version__ = "2.6.0"
__app_name__ = "firecrest"
MIN_PYTHON_VERSION = (3, 7, 0)

# Check python version
if sys.version_info[:3] < MIN_PYTHON_VERSION:
    sys.stderr.write(
        "Unsupported Python version: "
        "Python >= %d.%d.%d is required\n" % MIN_PYTHON_VERSION
    )
    sys.exit(1)

from firecrest.BasicClient import Firecrest
from firecrest.AsyncClient import AsyncFirecrest
from firecrest.ExternalStorage import ExternalDownload, ExternalUpload, ExternalStorage
from firecrest.AsyncExternalStorage import (
    AsyncExternalDownload,
    AsyncExternalUpload,
    AsyncExternalStorage,
)
from firecrest.Authorization import ClientCredentialsAuth
from firecrest.FirecrestException import (
    ClientsCredentialsException,
    FirecrestException,
    UnauthorizedException,
    HeaderException,
    UnexpectedStatusException,
    StorageDownloadException,
    StorageUploadException,
)
