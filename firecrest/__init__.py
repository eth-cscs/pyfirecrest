#
#  Copyright (c) 2019-2022, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import sys


__version__ = "1.2.0"

from firecrest.BasicClient import (
    Firecrest,
    ExternalDownload,
    ExternalUpload,
    ExternalStorage,
)
from firecrest.Authorization import ClientCredentialsAuth
from firecrest.FirecrestException import (
    FirecrestException,
    UnauthorizedException,
    HeaderException,
    UnexpectedStatusException,
    StorageDownloadException,
    StorageUploadException,
)
