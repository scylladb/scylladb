# Pre-generated AWS error definitions

`aws_error_definitions_generated.{hh,cc}` here are the expanded output of
`gen_aws_service_errors.py`, committed so that a build which cannot reach
GitHub has something to compile. A build uses them only when configured with
`--allow-stale-aws-models` (`Scylla_ALLOW_STALE_AWS_MODELS=ON` under CMake),
and only after the model fetch has failed; it copies them and does not
regenerate. See `docs/dev/building.md`.

They are derived from AWS's `c2j` models and are dual-licensed. See
`licenses/README.md`.

## Do not move these into `utils/s3/`

The build generates the same filenames under `<build-dir>/gen/utils/s3/`, and
the include is `#include "utils/s3/aws_error_definitions_generated.hh"`. The
source root precedes the generated directory on the include path, so a copy
named `utils/s3/aws_error_definitions_generated.hh` would shadow the file the
build just produced, and the compile would use one revision of the table while
the link used another.

## Refreshing them

```console
./utils/s3/gen_aws_service_errors.py --update-pregenerated
```

This fetches the current models, regenerates, and rewrites both files. Run it
when AWS adds an error worth carrying, and commit the result. The build never
writes here.
