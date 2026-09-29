# Third-party code vendored into this repository

Each directory below is a copy of an upstream crate, kept under its own licence
(the `LICENSE` file in each directory is the upstream text, verbatim) and built
as a path-only, `publish = false` workspace member. The only local edits are the
ones that detach the crate from its upstream workspace, each marked in place
with a `dfe-fetcher:` comment; re-sync by diffing against the recorded commit.

| directory | upstream | path | commit | licence |
|---|---|---|---|---|
| `vector-file-source/` | <https://github.com/vectordotdev/vector> | `lib/file-source` | `0141894cb40218aff7fcb308dd32847dd875bf74` | MIT |
| `vector-file-source-common/` | <https://github.com/vectordotdev/vector> | `lib/file-source-common` | `0141894cb40218aff7fcb308dd32847dd875bf74` | MIT |
