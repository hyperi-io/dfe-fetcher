<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/s4hana.md           -->
<!-- Purpose:   SAP S/4HANA OData V2 setup guide     -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                              -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# SAP S/4HANA Setup for dfe-fetcher

Status: beta. This profile has not been run against a live S/4HANA system. Its filter, paging and error handling are tested against synthetic OData V2 pages (`crates/fetcher/tests/fixtures/s4hana_odata_v2_page1.json` and `_page2.json`) shaped from the OData V2 JSON format, so the first tick on your system is the proof. The fetcher warns at startup for an enabled instance below stable.

What an SAP administrator configures so dfe-fetcher can read an OData V2 entity set from S/4HANA (read-only, pull-mode).

## Overview

One unit reads one OData V2 entity set as an event feed. Each tick asks for the rows whose timestamp property falls in the window, oldest first, and follows the server's own next link until there is none.

SAP publishes no S/4HANA OData V2 entity set for the security audit log or for change documents. The SAP Business Accelerator Hub catalog (5702 entries read from `catalog.svc` on 2026-11-21) has no entry for it. A search of names, titles and short texts for audit, change document, change log and application log finds a deprecated OData V4 "Security Audit Log services" entry, and audit APIs for SAP BTP, IBP, SuccessFactors and Convergent Charging, which are other products. So the entity set is yours to pick: a released A2X API with a change timestamp, or a service your SAP team builds over the audit or change-document tables. The profile reads any of them the same way.

The source is the shipped `s4hana` profile (`crates/fetcher/profiles/s4hana.yaml`), configured as an instance under `sources.rest`. A second entity set is a second unit instantiated from the profile's `entity_set` endpoint.

## Prerequisites and Permissions

- An S/4HANA system (on-premise, private cloud or public cloud) whose OData service is active, and its API host. The Hub shows the path as `https://{host}:{port}/sap/opu/odata/sap/<SERVICE>`.
- The service name, the entity set and a timestamp property on it that the service can filter and sort on.
- Network egress from the fetcher host to the API host.

Read access to the entity set is all it needs. dfe-fetcher sends GET and nothing else.

| Path | Credential | Where it is created |
|------|------------|---------------------|
| Basic | A communication user's name and password | Public cloud: Maintain Communication Users, then a communication system and arrangement ([SAP's guide](https://help.sap.com/doc/6ce62b6bdda340ffbeae3f138c3cb71b/SHIP/en-US/Set_Up_Authentication_for_SAP_S4HANA_Cloud_Extensions.pdf), section 3) |
| OAuth2 client credentials | A client id and secret | SAP's pages for this flow did not load for us, so the token path below is unconfirmed. Community posts show `/sap/bc/sec/oauth2/token` on private cloud and on-premise (client registered in `SOAUTH2`), and `/sap/public/bc/sec/oauth2/token` on a public cloud `-api` host |

Set `vars.token_path` to whatever your system's OAuth details page shows. The exchange posts `grant_type`, `client_id`, `client_secret` and an optional `scope` as a form. Whether your system also wants the client credentials in a Basic header is not confirmed.

## Configuration

```yaml
sources:
  rest:
    s4_changes:
      enabled: true
      topic: "s4hana"
      interval_secs: 900
      auth:
        mode: basic
        username: "SVC_DFE_FETCHER"
        password: "vault:kv/data/s4hana/prod:password"
      vars:
        base_url: "https://s4.example.internal:44300"   # required, no default
        service: "ZAUDIT_SRV"                           # your service
        entity_set: "ChangeLogEntries"                  # your entity set
        time_field: "ChangedAt"                         # the timestamp property
        time_type: datetimeoffset                       # or datetime
        key_field: "EntryId"                            # required, unique per row
        sap_client: "100"                               # sent as a header, unset for the user's default client
        # extra_filter: "ObjectClass eq 'ORDER'"        # ANDed onto the window filter
      profile: s4hana
```

For a second entity set, switch the template off and name each set as its own unit:

```yaml
      units:
        entity_set: { enabled: false }
        change_docs: { endpoint: entity_set, vars: { entity_set: "ChangeDocs", time_field: "ChangedAt" } }
        logons:      { endpoint: entity_set, vars: { service: "ZLOGON_SRV", entity_set: "Logons", time_field: "LogonAt" } }
```

Rows land on `<topic>` tagged `<connection>.<unit>`.

## Reading a Window

- The filter is `time_field ge <start> and time_field lt <end>`, both bounds cut to the second, so a row falls in exactly one window. `time_type: datetimeoffset` sends `datetimeoffset'2026-11-20T01:00:00Z'` and `datetime` sends `datetime'2026-11-20T01:00:00'`. Both literal forms are from the [OData V2 overview](https://www.odata.org/documentation/odata-version-2-0/overview/).
- A window ends 5 minutes before now, so a row whose transaction commits just after its timestamp is still read. The 5 minutes is a guess, not measured against SAP. A row that becomes visible later is never read, because a closed window is not read again. To allow longer, copy the profile inline under the instance with a larger `window.delay`.
- Rows come back ordered by `time_field` then `key_field`. `key_field` has no default and must be unique per row, so rows with equal timestamps page in a fixed order. An instance without one does not bind.
- A date-only property (an `Edm.DateTime` shown as a date) holds no time of day. Its rows fall in the window that contains midnight, so rows created after that window closed are never read. Use a time-of-day property.
- A unit that reads 100 pages fails the tick rather than stopping short, and the window is read again in halves. Delivery is at least once, so a window read again after a failed page or the page limit can deliver a row twice. Deduplicate downstream on `key_field`.

**Paging.** The server answers `{"d": {"results": [...], "__next": "<uri>"}}` while rows remain, and leaves `__next` out of the last page ([OData V2 JSON format](https://www.odata.org/documentation/odata-version-2-0/json-format/)). SAP Gateway builds the link with `$skiptoken` from Gateway 2.0 SP04 ([SAP Gateway feature overview](https://help.sap.com/saphelp_snc700_ehp04/helpdata/en/97/422751c639276ee10000000a445394/content.htm)).

dfe-fetcher requests the link exactly as given, except that `sap-client` travels as a header on every page. SAP's own cloud SDK sends it that way (`header-builder-for-destination.ts` in SAP/cloud-sdk-js). Two things follow, both unconfirmed on a live system:

- If the link omits `$filter`, the next page is not windowed. The link must carry the first request's filter. Check one real `__next` before enabling.
- If the link names a different `sap-client` in its query than the header carries, which one SAP honours is not confirmed.

If the link names a host other than `base_url`, the tick fails and the credential is never sent there. Gateway can build the link with an internal host name. Make the system answer with the host the fetcher calls.

The server decides what a `$skiptoken` means, and a position in the result is a common choice. If yours is one, a row inserted into the window between two page reads shifts the pages and can repeat or skip a row. A window that has closed (the 5 minute delay) is the protection.

## Verification

dfe-fetcher only reads. Each tick is one call per page, so size `interval_secs` to how often you need the data.

1. Smoke-test the entity set with Basic (the password reaches curl through `--config -`, not the command line):

   ```bash
   printf 'user = "SVC_DFE_FETCHER:%s"\n' "$S4_PASSWORD" |
     curl -sS --config - -H 'Accept: application/json' -H 'sap-client: 100' \
       "https://s4.example.internal:44300/sap/opu/odata/sap/ZAUDIT_SRV/ChangeLogEntries?\$format=json&\$top=2"
   ```

   A good answer has `d.results`. Note whether `d.__next` appears when you leave `$top` off.

2. Start the fetcher and watch the first tick. The driver logs `unit tick complete` per unit with `rows`, or `unit tick failed` with SAP's own message.

3. Common failures.
   - `401` with `Logon failed`: wrong user, password or client. A 401 or 403 is never retried.
   - `404` on the entity set: the service is not active on the Gateway, or the name is wrong.
   - `400` on the filter: the property is not filterable, or `time_type` does not match its OData type.
   - A tick failing with a host name from `__next`: see Reading a Window.

## References

- OData V2 JSON format (`d.results`, `__next`, `/Date(...)/`): <https://www.odata.org/documentation/odata-version-2-0/json-format/>
- OData V2 literals (`datetime'...'`, `datetimeoffset'...'`): <https://www.odata.org/documentation/odata-version-2-0/overview/>
- SAP Gateway feature overview, server-driven paging: <https://help.sap.com/saphelp_snc700_ehp04/helpdata/en/97/422751c639276ee10000000a445394/content.htm>
- SAP's authentication guide for S/4HANA Cloud (Basic, client certificate, SAML bearer): <https://help.sap.com/doc/6ce62b6bdda340ffbeae3f138c3cb71b/SHIP/en-US/Set_Up_Authentication_for_SAP_S4HANA_Cloud_Extensions.pdf>
- How SAP's cloud SDK sends `sap-client` (a header): <https://github.com/SAP/cloud-sdk-js/blob/main/packages/connectivity/src/scp-cf/header-builder-for-destination.ts>
- SAP Business Accelerator Hub catalog: <https://api.sap.com/odata/1.0/catalog.svc/APIContent.APIs>
