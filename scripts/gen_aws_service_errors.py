#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Refresh the S3 and STS error entries in utils/s3/aws_error_definitions.{hh,cc}
# from the c2j models published by aws/aws-sdk-cpp.
#
# Those files are ordinary sources, compiled as they are committed. Only the
# region between the @SCYLLA_AWS_ERRORS_BEGIN@ and @SCYLLA_AWS_ERRORS_END@
# markers is rewritten; the core error entries around it are maintained by
# hand. Run this when AWS adds an error worth carrying, and commit the result.
#
# Ports the relevant bits of the aws-sdk-cpp Java generator:
#   * ErrorFormatter.formatErrorConstName()  -> _format_error_const_name()
#   * C2jModelToGeneratorModelTransformer.convertError()  -> error extraction
#   * ServiceErrorsSource.vm GetErrorForName() body  -> mapping snippet
#
# Usage:
#   scripts/gen_aws_service_errors.py            # rewrite the two files
#   scripts/gen_aws_service_errors.py --dry-run  # print the blocks instead

from __future__ import annotations

import argparse
import http.client
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import NamedTuple

# The files whose marked region this script owns.
REPO_ROOT = Path(__file__).resolve().parent.parent
HEADER = REPO_ROOT / "utils" / "s3" / "aws_error_definitions.hh"
SOURCE = REPO_ROOT / "utils" / "s3" / "aws_error_definitions.cc"
BEGIN_MARKER = "@SCYLLA_AWS_ERRORS_BEGIN@"
END_MARKER = "@SCYLLA_AWS_ERRORS_END@"

# --- constants copied verbatim from the upstream Java generator ---------------
# Keep these in sync with:
#   ErrorFormatter.java                         (CORE_ERROR_CONSTANTS)
#   C2jModelToGeneratorModelTransformer.java    (THROTTLE_ERRORS,
#                                                RETRYABLE_ERRORS,
#                                                RESPONSE_CODES_TO_RETRY)

# Names already provided by the core error mapper in aws_error.cc; skip when
# emitting service-specific enums.
CORE_ERROR_CONSTANTS: set[str] = {
    "INCOMPLETE_SIGNATURE", "INTERNAL_FAILURE", "INVALID_ACTION",
    "INVALID_CLIENT_TOKEN_ID", "INVALID_PARAMETER_COMBINATION",
    "INVALID_QUERY_PARAMETER", "INVALID_PARAMETER_VALUE",
    "MISSING_ACTION", "MISSING_AUTHENTICATION_TOKEN", "MISSING_PARAMETER",
    "OPT_IN_REQUIRED", "REQUEST_EXPIRED", "SERVICE_UNAVAILABLE",
    "THROTTLING", "VALIDATION", "ACCESS_DENIED", "RESOURCE_NOT_FOUND",
    "UNRECOGNIZED_CLIENT", "MALFORMED_QUERY_STRING", "SLOW_DOWN",
    "REQUEST_TIME_TOO_SKEWED", "INVALID_SIGNATURE", "SIGNATURE_DOES_NOT_MATCH",
    "INVALID_ACCESS_KEY_ID", "REQUEST_TIMEOUT", "NETWORK_CONNECTION",
}
THROTTLE_ERRORS: set[str] = {
    "Throttling", "ThrottlingException", "ThrottledException",
    "RequestThrottledException", "TooManyRequestsException",
    "ProvisionedThroughputExceededException", "TransactionInProgressException",
    "RequestLimitExceeded", "BandwidthLimitExceeded", "LimitExceededException",
    "RequestThrottled", "SlowDown", "PriorRequestNotComplete",
    "EC2ThrottledException",
}
RETRYABLE_ERRORS: set[str] = {
    "RequestTimeout", "InternalError", "RequestTimeoutException",
    "IDPCommunicationError",
}
RESPONSE_CODES_TO_RETRY: set[int] = {500, 502, 503, 504}

# The two services we care about, with their c2j model filenames on
# aws-sdk-cpp main.
SERVICES: dict[str, str] = {
    "s3":  "s3-2006-03-01.normal.json",
    "sts": "sts-2011-06-15.normal.json",
}
MODEL_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/aws/aws-sdk-cpp/main/"
    "tools/code-generation/api-descriptions/{filename}"
)

# Bounds for the model fetch. Without an explicit timeout urlopen() inherits
# socket.getdefaulttimeout(), which is None, so a black-holed connection
# blocks forever and hangs whoever runs this.
FETCH_TIMEOUT_SECONDS = 10
FETCH_ATTEMPTS = 3
FETCH_BACKOFF_SECONDS = 2
# HTTP statuses worth another attempt. Anything else (404 for a renamed
# model, 403) will not change on a retry, so fail immediately.
FETCH_RETRY_STATUSES: set[int] = {408, 425, 429, 500, 502, 503, 504}
# Cap on an honoured Retry-After, so a server asking for a long wait cannot
# stall the run either.
FETCH_MAX_RETRY_AFTER_SECONDS = 30

# --- data types ----------------------------------------------------------------

class ServiceError(NamedTuple):
    enum_name: str      # e.g. NO_SUCH_BUCKET
    wire_code: str      # what appears in <Code> of the XML response
    retryable: bool

# --- ports of the Java logic ---------------------------------------------------

def _format_error_const_name(error_name: str) -> str:
    """Port of ErrorFormatter.formatErrorConstName().

    UPPER_CAMEL → UPPER_UNDERSCORE via Guava's CaseFormat, plus:
      * '.' replaced with '_' first
      * trailing '_ERROR' stripped
      * trailing '_EXCEPTION' stripped

    Guava inserts '_' before every uppercase letter after the first character,
    so runs of uppercase letters like 'IDP' become 'I_D_P' — which matches
    upstream's generated I_D_P_COMMUNICATION_ERROR in STSErrors.h.
    """
    s = error_name.replace('.', '_')
    out = []
    for i, ch in enumerate(s):
        if i > 0 and ch.isupper():
            out.append('_')
        out.append(ch.upper())
    upper = ''.join(out)
    if upper.endswith('_ERROR'):
        upper = upper[:-len('_ERROR')]
    if upper.endswith('_EXCEPTION'):
        upper = upper[:-len('_EXCEPTION')]
    return upper


def _is_retryable(shape: dict, wire_code: str) -> bool:
    """Reproduces the retryability rules from convertError() plus the post-pass
    that applies THROTTLE_ERRORS / RETRYABLE_ERRORS by wire code."""
    if shape.get("retryable") is not None:
        return True
    err = shape.get("error", {})
    if err.get("httpStatusCode", 0) in RESPONSE_CODES_TO_RETRY and not err.get("senderFault", False):
        return True
    return wire_code in THROTTLE_ERRORS or wire_code in RETRYABLE_ERRORS


def _extract_service_errors(model: dict) -> list[ServiceError]:
    """Collect every error shape referenced by any operation, dedupe by enum
    name, skip core error names."""
    shapes = model.get("shapes", {})
    error_shape_names: set[str] = set()
    for op in model.get("operations", {}).values():
        for err in op.get("errors", []):
            error_shape_names.add(err["shape"])

    seen: set[str] = set()
    errors: list[ServiceError] = []
    for shape_name in sorted(error_shape_names):
        shape = shapes.get(shape_name, {})
        wire_code = shape.get("error", {}).get("code") or shape_name

        # Shape names in c2j are already UpperCamel, so no transformation
        # needed before feeding to _format_error_const_name.
        enum_name = _format_error_const_name(shape_name)
        if enum_name in CORE_ERROR_CONSTANTS or enum_name in seen:
            continue
        seen.add(enum_name)
        errors.append(ServiceError(enum_name, wire_code, _is_retryable(shape, wire_code)))

    errors.sort(key=lambda e: e.enum_name)
    return errors

# --- renderers -----------------------------------------------------------------

def _render_enum_lines(errors: list[ServiceError], indent: str) -> str:
    return "\n".join(f"{indent}{e.enum_name}," for e in errors)


def _render_mapping_lines(errors: list[ServiceError], indent: str) -> str:
    lines = []
    for e in errors:
        r = "yes" if e.retryable else "no"
        lines.append(
            f'{indent}{{"{e.wire_code}", aws_error(aws_error_type::{e.enum_name}, retryable::{r})}},'
        )
    return "\n".join(lines)

# --- in-place patcher ----------------------------------------------------------


def _replace_region(text: str, block: str, context: str) -> str:
    """Replace whatever sits between the two markers with `block`.

    The marker lines themselves are kept, so the file can be rewritten again."""
    begin = text.find(BEGIN_MARKER)
    end = text.find(END_MARKER)
    if begin < 0 or end < 0:
        raise RuntimeError(f"{context}: markers not found")
    if end < begin:
        raise RuntimeError(f"{context}: end marker precedes begin marker")
    begin_eol = text.index("\n", begin) + 1
    end_bol = text.rindex("\n", 0, end) + 1
    return text[:begin_eol] + block + text[end_bol:]


def _write_if_changed(path: Path, text: str) -> bool:
    if path.exists() and path.read_text() == text:
        print(f"{path}: no changes", file=sys.stderr)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"wrote {path}", file=sys.stderr)
    return True

def _retry_after_seconds(value: str) -> float | None:
    """How long Retry-After asks us to wait, or None if it cannot be read.

    The header carries either delay-seconds or an HTTP-date. Only a
    non-negative integer counts as the former, so that "inf" or "nan" cannot
    reach time.sleep()."""
    text = value.strip()
    if text.isdigit():
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        # The obsolete asctime form carries no zone, and HTTP dates are GMT.
        # Without this the subtraction below raises TypeError.
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _retry_delay(error: Exception, attempt: int) -> float | None:
    """How long to wait before retrying, or None if the error is permanent.

    Anything that carries no HTTP status is a transport failure (DNS, TCP,
    TLS, a timeout, a truncated response) and is always worth retrying. An
    HTTPError is retried only for the statuses a server uses to say "later",
    honouring Retry-After when it sends one."""
    backoff = FETCH_BACKOFF_SECONDS * (2 ** (attempt - 1))
    if not isinstance(error, urllib.error.HTTPError):
        return backoff
    if error.code not in FETCH_RETRY_STATUSES:
        return None
    retry_after = error.headers.get("Retry-After") if error.headers else None
    if retry_after:
        seconds = _retry_after_seconds(retry_after)
        if seconds is not None:
            return min(seconds, FETCH_MAX_RETRY_AFTER_SECONDS)
    return backoff


def _fetch_raw(filename: str) -> bytes:
    """Fetch one c2j model, retrying transient failures. Raises RuntimeError
    once the attempts are exhausted or the failure is permanent.

    Every attempt is bounded by FETCH_TIMEOUT_SECONDS, so an unresponsive
    endpoint ends the run with an error instead of hanging it."""
    url = MODEL_URL_TEMPLATE.format(filename=filename)
    print(f"# fetching {url}", file=sys.stderr)
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as resp:
                return resp.read()
        except (OSError, http.client.HTTPException) as e:
            # URLError (and its HTTPError subclass) derive from OSError, so
            # this catches non-2xx responses (rate-limiting, model moved or
            # renamed) alongside transport failures. urllib only wraps
            # socket errors in URLError while connecting -- a timeout or a
            # reset while reading the response body surfaces bare, as
            # TimeoutError or ConnectionResetError, and a truncated body
            # surfaces as http.client.IncompleteRead.
            delay = _retry_delay(e, attempt)
            if delay is None or attempt == FETCH_ATTEMPTS:
                raise RuntimeError(f"failed to fetch {url}: {e}") from e
            print(f"# attempt {attempt}/{FETCH_ATTEMPTS} failed ({e}), "
                  f"retrying in {delay:g}s", file=sys.stderr)
            time.sleep(delay)
    raise AssertionError("unreachable")

# --- main ----------------------------------------------------------------------


def _render_block(per_service: dict[str, list[ServiceError]], render, indent: str) -> str:
    """One block covering every service, each labelled so the committed file
    stays readable."""
    parts = []
    for service, errors in per_service.items():
        parts.append(f"{indent}// {service.upper()}\n")
        parts.append(render(errors, indent) + "\n")
    return "".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="print the generated blocks instead of rewriting "
                             "the files")
    args = parser.parse_args()

    per_service = {
        service: _extract_service_errors(json.loads(_fetch_raw(filename)))
        for service, filename in SERVICES.items()
    }

    enum_block = _render_block(per_service, _render_enum_lines, "    ")
    map_block = _render_block(per_service, _render_mapping_lines, "        ")

    if args.dry_run:
        print(enum_block, end="")
        print(map_block, end="")
        return 0

    _write_if_changed(HEADER, _replace_region(HEADER.read_text(), enum_block,
                                              HEADER.name))
    _write_if_changed(SOURCE, _replace_region(SOURCE.read_text(), map_block,
                                              SOURCE.name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
