#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Generate ScyllaDB `aws_error_type` enum entries and wire-name → enum
# mapping entries for S3 and STS by pulling the c2j models from
# aws/aws-sdk-cpp main on GitHub.
#
# Ports the relevant bits of the aws-sdk-cpp Java generator:
#   * ErrorFormatter.formatErrorConstName()  → _format_error_const_name()
#   * C2jModelToGeneratorModelTransformer.convertError()  → error extraction
#   * ServiceErrorsSource.vm GetErrorForName() body  → mapping snippet
#
# Usage:
#   python3 utils/s3/gen_aws_service_errors.py           # writes generated files next to templates
#   python3 utils/s3/gen_aws_service_errors.py --output-dir DIR
#                                                        # build-system mode
#   python3 utils/s3/gen_aws_service_errors.py --dry-run # prints to stdout instead

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import NamedTuple

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

HERE = Path(__file__).resolve().parent
HEADER_TEMPLATE = HERE / "aws_error_definitions.hh.in"
SOURCE_TEMPLATE = HERE / "aws_error_definitions.cc.in"
# Names the build system consumes when the script is invoked with
# --output-dir DIR: the generated files land under DIR/utils/s3/.
GENERATED_HEADER_NAME = "aws_error_definitions_generated.hh"
GENERATED_SOURCE_NAME = "aws_error_definitions_generated.cc"
HASHES_NAME = "aws_error_definitions.hashes.json"


def _substitute_tags(text: str, per_service: dict[str, str], context: str) -> str:
    """Replace `// @SCYLLA_AWS_ERRORS_<SERVICE>@` marker lines with the
    corresponding generated block. Preserves the indentation of the marker
    line so the emitted block matches the surrounding code."""
    for service, generated in per_service.items():
        tag = f"@SCYLLA_AWS_ERRORS_{service.upper()}@"
        marker_line = None
        for line in text.splitlines(keepends=False):
            if tag in line:
                marker_line = line
                break
        if marker_line is None:
            raise RuntimeError(f"{context}: marker {tag} not found")
        text = text.replace(marker_line, generated, 1)
    return text


def _write_if_changed(path: Path, text: str) -> bool:
    if path.exists() and path.read_text() == text:
        print(f"{path}: no changes", file=sys.stderr)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"wrote {path}", file=sys.stderr)
    return True

# --- main ----------------------------------------------------------------------


def _fetch_raw(filename: str) -> bytes:
    """Fetch one c2j model. Raises RuntimeError on any network/HTTP error;
    the caller decides how to react (e.g. fall back to cached outputs)."""
    url = MODEL_URL_TEMPLATE.format(filename=filename)
    print(f"# fetching {url}", file=sys.stderr)
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read()
    except urllib.error.URLError as e:
        # HTTPError is a subclass of URLError, so this catches both
        # transient network failures (DNS, TCP, TLS) and non-2xx
        # HTTP responses (rate-limiting, model moved/renamed, etc.).
        raise RuntimeError(f"failed to fetch {url}: {e}") from e


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _load_hashes(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            pass
    return {}


def _save_hashes(path: Path, hashes: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(hashes, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="print generated blocks to stdout instead of "
                             "writing any files")
    parser.add_argument("--force", action="store_true",
                        help="regenerate even if the cached sidefile shows "
                             "all model/template/output hashes still match")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="build-system mode: read the source templates "
                             "utils/s3/aws_error_definitions.{hh,cc}.in and "
                             "write the fully-expanded copies (plus the "
                             "hashes sidefile) to <output-dir>/utils/s3/. "
                             "When omitted, the generated files land next to "
                             "the templates.")
    args = parser.parse_args()

    # Where do generated files (and the sidefile) live? In `--output-dir`
    # mode: under the build tree. Otherwise: alongside the source templates
    # (developer in-place workflow).
    if args.output_dir is not None:
        out_dir = args.output_dir / "utils" / "s3"
    else:
        out_dir = HERE
    header_out = out_dir / GENERATED_HEADER_NAME
    source_out = out_dir / GENERATED_SOURCE_NAME
    hashes_file = out_dir / HASHES_NAME

    # Compute fresh hashes for the inputs we can hash without hitting the
    # network first (templates).
    fresh: dict = {
        "templates": {
            HEADER_TEMPLATE.name: _sha256_file(HEADER_TEMPLATE),
            SOURCE_TEMPLATE.name: _sha256_file(SOURCE_TEMPLATE),
        },
    }

    # Fetch models and hash them. If the network is unreachable but we
    # already have generated outputs whose hashes match the sidefile, fall
    # back to those (offline / air-gapped builds must still succeed). If we
    # have no usable cache, the build must fail loudly.
    raw_models: dict[str, bytes] = {}
    fresh["models"] = {}
    fetch_error: str | None = None
    try:
        for service, filename in SERVICES.items():
            raw = _fetch_raw(filename)
            raw_models[service] = raw
            fresh["models"][service] = _sha256_bytes(raw)
    except RuntimeError as e:
        fetch_error = str(e)

    cached = _load_hashes(hashes_file)
    cached_gen = cached.get("generated", {})

    if fetch_error is not None:
        # No network. If cached outputs are present at all we let the build
        # proceed with them — leaving a developer stranded without a
        # buildable tree is worse than shipping a possibly-stale registry.
        # If the recorded hashes don't match (someone hand-edited the
        # generated files, or the sidefile is stale) we surface a second
        # warning so it's not silent.
        outputs_present = (
            not args.dry_run
            and header_out.exists() and source_out.exists()
        )
        if outputs_present and not args.force:
            print(f"warning: {fetch_error}", file=sys.stderr)
            hashes_match = (
                cached_gen.get(header_out.name) == _sha256_file(header_out)
                and cached_gen.get(source_out.name) == _sha256_file(source_out)
            )
            if not hashes_match:
                print(f"warning: cached generated files at {out_dir} do not "
                      f"match the sidefile hashes (manual edits or stale "
                      f"sidefile); using them anyway to keep the build "
                      f"working", file=sys.stderr)
            else:
                print(f"warning: keeping existing generated files at "
                      f"{out_dir} (offline build)", file=sys.stderr)
            return 0
        # No cached outputs to fall back to, or --force was requested.
        sys.exit(f"error: {fetch_error} and no cached outputs at "
                 f"{out_dir}; cannot regenerate offline")

    # Short-circuit only if every input matches AND the outputs on disk
    # still match what we last wrote (guards against manual edits).
    outputs_pristine = (
        header_out.exists()
        and source_out.exists()
        and cached_gen.get(header_out.name) == _sha256_file(header_out)
        and cached_gen.get(source_out.name) == _sha256_file(source_out)
    ) if not args.dry_run else False

    if (not args.force and not args.dry_run
            and cached.get("models") == fresh["models"]
            and cached.get("templates") == fresh["templates"]
            and outputs_pristine):
        print("models, templates and generated outputs all match cached "
              "sidefile; nothing to do", file=sys.stderr)
        return 0

    per_service = {
        s: _extract_service_errors(json.loads(raw_models[s]))
        for s in SERVICES
    }

    if args.dry_run:
        for service, errors in per_service.items():
            print(f"\n=== {service}: enum entries ===")
            print(_render_enum_lines(errors, "    "))
            print(f"\n=== {service}: mapping entries ===")
            print(_render_mapping_lines(errors, "        "))
        return 0

    header_text = _substitute_tags(
        HEADER_TEMPLATE.read_text(),
        {s: _render_enum_lines(e, "    ") for s, e in per_service.items()},
        context=HEADER_TEMPLATE.name)
    source_text = _substitute_tags(
        SOURCE_TEMPLATE.read_text(),
        {s: _render_mapping_lines(e, "        ") for s, e in per_service.items()},
        context=SOURCE_TEMPLATE.name)

    _write_if_changed(header_out, header_text)
    _write_if_changed(source_out, source_text)

    fresh["generated"] = {
        header_out.name: _sha256_bytes(header_text.encode()),
        source_out.name: _sha256_bytes(source_text.encode()),
    }
    _save_hashes(hashes_file, fresh)
    print(f"updated {hashes_file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

