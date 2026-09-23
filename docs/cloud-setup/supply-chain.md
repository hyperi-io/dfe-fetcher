<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/supply-chain.md     -->
<!-- Purpose:   Supply-chain registry sources setup guide -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Supply-Chain Registry Setup for dfe-fetcher

What an operator needs to configure so dfe-fetcher can watch public
package registries (PyPI, crates.io, Go module proxy) for supply-chain
takeover of the packages your organisation owns.

## Overview

These three sources poll public, unauthenticated registry APIs. On each
tick the fetcher pulls the full metadata document for every configured
package and emits it; downstream tooling computes deltas across ticks to
detect unexpected publications, yanks, ownership changes, or commit-hash
drift - the classic indicators of a hijacked package.

| Source | Endpoint(s) | Record unit | Auth |
|--------|-------------|-------------|--------|
| `pypi` | `https://pypi.org/pypi/<package>/json` | one record per package | no credentials |
| `crates_io` | `https://crates.io/api/v1/crates/<name>` | one record per crate | no credentials |
| `go_modules` | `https://proxy.golang.org/<module>/@v/list` then `.../@v/<version>.info` | one record per module (version list + per-version info) | no credentials |

There is NO authentication for any of these sources. There are no API
keys, OAuth flows, or service accounts to provision. The only "setup" is
deciding which packages you own and listing them. Each source also
accepts an `api_url_override` so you can point at an internal mirror or a
test server.

The three sources are the shipped `pypi`, `crates_io` and `go_modules`
profiles (`crates/fetcher/profiles/`); each typed block below maps onto an
instance of its profile at load. Missing packages are tolerated: a 404 for a
configured name is an empty answer, not an error, and the other names still
land. A 5xx or 429 on one name is retried with backoff; a name the registry
keeps refusing fails the tick, and the names after it wait for the next tick
(these are state documents, so nothing is lost). The Go source fetches at most
100 versions' `.info` per module per tick, in list order, and a retracted
version (a 404 on its `.info`) is left out of the row.

## Prerequisites

- Network egress from the fetcher host to the public registry endpoints
  above (or to your internal mirror, if you set `api_url_override`).
- An inventory of the packages your organisation publishes (or
  namespaces you want to monitor for typosquats). This is the real
  prerequisite - the monitoring is only as good as the list.
- Nothing else. No accounts, no credentials, no tenant-side enablement.

## Required Permissions

None. All three registries serve public read-only metadata over HTTPS
with no authentication. There is no IAM, no token, and no role to grant.

The registries do, however, ask consumers to be polite:

| Registry | Operator expectation |
|----------|----------------------|
| PyPI | Set a descriptive User-Agent; respect `ETag` / `X-Cache` so repeated requests hit the CDN. No hard edge rate limit today, but PyPI may throttle or ban irresponsible consumers. |
| crates.io | A descriptive User-Agent is REQUIRED (a bare client UA like `reqwest/x` invites blocking). Limit to a maximum of 1 request per second. dfe-fetcher sends `dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)`. |
| Go module proxy | Public, no auth. The proxy only ever receives module paths and versions. Use `GOPRIVATE` upstream for truly private modules so they are never queried against the public mirror. |

## Source-Side Setup

There is nothing to provision on the registry side. You cannot (and need
not) create an account or grant any access. "Source-side setup" here is
purely the inventory step:

1. List every package, crate, and module your organisation publishes to
   each public registry. Include packages that are deprecated but not
   deleted - those are prime takeover targets.

2. Optionally add high-value typosquat candidates (names one edit away
   from yours) so you are alerted if someone publishes them.

3. Use the exact registry name for each entry:
   - PyPI: the normalised project name, e.g. `scalo`.
   - crates.io: the crate name, e.g. `dfe-fetcher`.
   - Go: the full module path, e.g.
     `github.com/hyperi-io/some-go-tool`.

4. For private Go modules, ensure they are NOT in the monitored list and
   are covered by `GOPRIVATE` in your build environment - the public
   proxy would only fail to find them, but there is no reason to send
   their paths to a public mirror.

## dfe-fetcher Configuration

### Config File

Each source takes a list of names to monitor plus a `topic`. The lists
are the only required fields.

```yaml
sources:
  pypi:
    enabled: true
    packages:
      - "scalo"
      - "hyperi-ci"
    # api_url_override: "https://pypi.org"   # default
    topic: "pypi"
    # filter: 'info.yanked != false'         # hot-reloaded

  crates_io:
    enabled: true
    crates:
      - "scalo"
      - "dfe-loader"
      - "dfe-fetcher"
    # api_url_override: "https://crates.io"   # default
    topic: "crates_io"
    # filter: 'versions.size() > 0'           # hot-reloaded

  go_modules:
    enabled: true
    modules:
      - "github.com/hyperi-io/some-go-tool"
    # api_url_override: "https://proxy.golang.org"  # default
    topic: "go_modules"
    # filter: 'versions.size() > 0'           # hot-reloaded
```

Field reference (identical shape across all three):

- `enabled` - turn the source on.
- `packages` (pypi) / `crates` (crates_io) / `modules` (go_modules) -
  the list of names to monitor. An empty list means the source does
  nothing.
- `api_url_override` - optional base URL; point at an internal mirror or
  a test server. Defaults: `https://pypi.org`, `https://crates.io`,
  `https://proxy.golang.org`.
- `topic` - output topic.
- `filter` - optional CEL filter, hot-reloaded.

### Environment Variables

Scalar fields follow the
`DFE_FETCHER_SOURCES__<SOURCE>__<FIELD>` pattern (double underscores):

```bash
DFE_FETCHER_SOURCES__PYPI__ENABLED="true"
DFE_FETCHER_SOURCES__PYPI__TOPIC="pypi"

DFE_FETCHER_SOURCES__CRATES_IO__ENABLED="true"
DFE_FETCHER_SOURCES__CRATES_IO__TOPIC="crates_io"

DFE_FETCHER_SOURCES__GO_MODULES__ENABLED="true"
DFE_FETCHER_SOURCES__GO_MODULES__TOPIC="go_modules"
```

The name lists (`packages`, `crates`, `modules`) are sequences and are
cleanest to define in the config file. If you must set them via the
environment, follow the loader's indexed-sequence convention, e.g.
`DFE_FETCHER_SOURCES__PYPI__PACKAGES__0="scalo"`. Prefer the
config file for anything more than a couple of names.

### Secrets Manager

Not applicable. These sources have no credentials, so there is no secret
to store. There is no `credential_secret` field on any of the three
registry sources.

## Verification

1. Confirm reachability with a manual request (substitute one of your own
   package names):

   ```bash
   curl -s -H "User-Agent: dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)" \
     https://pypi.org/pypi/scalo/json | head -c 200

   curl -s -H "User-Agent: dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)" \
     https://crates.io/api/v1/crates/dfe-fetcher | head -c 200

   curl -s https://proxy.golang.org/github.com/hyperi-io/some-go-tool/@v/list
   ```

2. Start dfe-fetcher with the sources enabled and watch the logs. Each
   source logs `unit tick complete` with its `rows` count on every tick.

3. Each profile's probe is a lightweight reachability request (PyPI root,
   crates.io `/api/v1/summary`, Go proxy root) and is healthy when the
   endpoint answers 2xx.

4. Emitted records carry a name field so downstream tooling can route
   them: `_dfe_fetcher_package` (PyPI), `_dfe_fetcher_crate`
   (crates.io), `_dfe_fetcher_module` (Go). A 404 for a configured name is
   skipped, not fatal, and is not counted as an API error.

## Cost

dfe-fetcher only reads. All three registries are public, serve metadata
without credentials, and are not expected to carry an additional charge.

Be a good citizen to avoid being throttled or blocked: send a descriptive
User-Agent (dfe-fetcher does this for crates.io automatically), respect each
registry's published usage policy, and choose a fetch interval that matches
how quickly you need to detect a takeover rather than polling aggressively.
For very large watch lists, consider an internal mirror via
`api_url_override`.

## References

- PyPI API docs - rate limits, caching, ETag, User-Agent guidance:
  https://docs.pypi.org/api/
- crates.io data access policy - required User-Agent, 1 req/sec, dumps:
  https://crates.io/data-access
- crates.io policies:
  https://crates.io/policies
- Go module mirror and proxy protocol (`@v/list`, `@v/<version>.info`):
  https://proxy.golang.org/
- Go module services privacy (what the proxy receives, GOPRIVATE):
  https://proxy.golang.org/privacy
- Go module mirror launch / protocol overview:
  https://go.dev/blog/module-mirror-launch
