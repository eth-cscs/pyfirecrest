#
#  Copyright (c) 2026, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar

#: Context variable holding the correlation ID that is sent as the
#: `X-Correlation-ID` header in all the requests to FirecREST made in the
#: current context. Use the :func:`correlation_id` context manager to set it.
current_correlation_id: ContextVar[str | None] = ContextVar(
    "firecrest_correlation_id", default=None
)


@contextmanager
def correlation_id(cid: str | None = None):
    """Attach a correlation ID to all the requests to FirecREST that are
    made inside this context. The ID is sent in the `X-Correlation-ID`
    header of every request, so all of them can be traced in the server
    logs as parts of the same logical operation.

    .. code-block:: python

        with firecrest.correlation_id("my-workflow-1234"):
            client.mkdir("cluster", "/home/user/my-workflow")
            client.submit("cluster", script_local_path="script.sh")

    Without this context manager, the clients generate a new correlation ID
    for every method call.

    :param cid: the correlation ID to use. When `None`, a random UUID4 is
                generated. The ID in use is yielded, so it can be captured
                with `as` and logged by the caller.
    """
    token = current_correlation_id.set(cid or str(uuid.uuid4()))
    try:
        yield current_correlation_id.get()
    finally:
        current_correlation_id.reset(token)


@contextmanager
def ensure_correlation_id(default: str | None = None):
    """Set the correlation ID only when none is set in the current context.

    Used internally by the clients so that every public method call gets a
    correlation ID, without overriding one that was set by the user through
    :func:`correlation_id`.

    :param default: the ID to fall back to. When `None`, a random UUID4 is
                    generated.
    """
    if current_correlation_id.get() is not None:
        yield current_correlation_id.get()
        return

    with correlation_id(default) as cid:
        yield cid
