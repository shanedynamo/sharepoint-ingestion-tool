"""Graph API rate limit handler with exponential backoff."""

import functools
import logging
import time
from typing import Callable

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BASE_DELAY = 1.0


def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
) -> Callable:
    """Decorator: retries on 429 / 503 with Retry-After parsing + exponential backoff.

    - Parses the ``Retry-After`` header when present (seconds).
    - Falls back to exponential backoff: base_delay * 2^attempt.
    - Raises the last exception after *max_retries* consecutive failures.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as exc:
                    last_exception = exc
                    status = exc.response.status_code if exc.response is not None else 0

                    if status not in (429, 503):
                        raise

                    if attempt == max_retries:
                        break

                    # Prefer Retry-After header; fall back to exponential
                    retry_after = exc.response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = base_delay * (2 ** attempt)
                    else:
                        delay = base_delay * (2 ** attempt)

                    logger.warning(
                        "Throttled (HTTP %d) on %s – retry %d/%d in %.1fs",
                        status,
                        func.__qualname__,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    time.sleep(delay)

            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator
