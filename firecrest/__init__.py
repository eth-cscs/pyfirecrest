#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#

import sys


MIN_PYTHON_VERSION = (3, 6, 0)

# Check python version
if sys.version_info[:3] < MIN_PYTHON_VERSION:
    sys.stderr.write(
        "Unsupported Python version: "
        "Python >= %d.%d.%d is required\n" % MIN_PYTHON_VERSION
    )
    sys.exit(1)

from firecrest.BasicClient import (
    Firecrest,
    ExternalDownload,
    ExternalUpload,
    ExternalStorage,
)
from firecrest.Keycloak import ClientCredentialsAuthorization
from firecrest.FirecrestException import (
    FirecrestException, UnauthorizedException, HeaderException,
    UnexpectedStatusException, StorageDownloadException, StorageUploadException
)
