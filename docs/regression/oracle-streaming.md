# Oracle Streaming smoke tests

This smoke exercises the Kafka-compatible endpoint of **OCI Streaming**, not the separate
Oracle Streaming with Apache Kafka service. It checks TLS/SASL authentication, metadata, production
and consumption on two partitions, explicit offset commits, resuming with a new consumer, and receiving
messages published after an initial fetch from `LATEST`.

The first validation is local against real OCI resources. The next milestone is a
[read-only hosted WIF authentication check](#hosted-authentication-check) within Regression Tests; hosted E2E and recovery follow it.
The E2E is separate from `pnpm test` and `pnpm run test:ci`; missing configuration fails rather than skips it.

## Prerequisites

- Node.js and installed project dependencies as described in [CONTRIBUTING.md](../../CONTRIBUTING.md).
- OCI CLI available as `oci`, with a configured API signing key/profile.
- An OCI region and a dedicated compartment, with permissions to create, read, list and delete
  Streaming pools and streams. The Kafka user also needs permission to produce and consume.
- An OCI auth token for the Kafka user. This is different from the API signing key used by the CLI.
- Outbound TLS connectivity to the pool's public Kafka endpoint, normally on port 9092.

Account creation, identities and policies are one-time setup. The local script does not create or remove them.
OCI CLI uses its normal configuration, including `OCI_CLI_PROFILE` and `OCI_CLI_CONFIG_FILE` when set.

## Configure the local run

Set these environment variables in the shell running the commands:

| Variable                           | Used by             | Value                                                              |
| ---------------------------------- | ------------------- | ------------------------------------------------------------------ |
| `OCI_COMPARTMENT_ID`               | Resource manager    | OCID of the dedicated compartment                                  |
| `OCI_CLI_REGION`                   | Resource manager    | Region enabled for the tenancy                                     |
| `ORACLE_STREAMING_USERNAME_PREFIX` | Connection settings | `tenancy/domain/username`, without the pool OCID or trailing slash |
| `ORACLE_STREAMING_AUTH_TOKEN`      | Kafka smoke         | OCI auth token for that user                                       |

Use the tenancy/domain/user prefix shown in the pool's Kafka connection settings. Older tenancy
configurations can use `tenancy/username`. The script appends the newly created pool OCID.

For example, in Fish:

```fish
set -gx OCI_COMPARTMENT_ID '<compartment-ocid>'
set -gx OCI_CLI_REGION '<region>'
set -gx ORACLE_STREAMING_USERNAME_PREFIX '<tenancy>/<domain>/<username>'
read --silent --prompt-str 'OCI Kafka auth token: ' --global --export ORACLE_STREAMING_AUTH_TOKEN
set -gx ORACLE_STREAMING_RUN_ID local-(date -u +%Y%m%dT%H%M%SZ)-(random)
```

In Bash, use `export NAME=value`; read the secret with
`read -r -s -p 'OCI Kafka auth token: ' ORACLE_STREAMING_AUTH_TOKEN` and then
`export ORACLE_STREAMING_AUTH_TOKEN`.
If using a local environment file, load it into the shell first; these commands do not load `.env` automatically.

Run IDs accept 1–64 letters, digits and hyphens, starting with a letter or digit.
Keep the ID until cleanup has succeeded. Each concurrent invocation must use a different ID.

## Provision, test twice, and clean up

From the repository root:

```sh
node scripts/oracle-streaming-resources.ts run "$ORACLE_STREAMING_RUN_ID"
```

The command:

1. Creates a public, explicitly named stream pool with Oracle-managed encryption and topic autocreation disabled.
2. Creates `kafka-smoke` with two partitions and 24-hour retention, and waits for both resources to become active.
3. Retrieves bootstrap servers from OCI and composes the Kafka username without printing the auth token.
4. Runs the E2E twice in separate processes against the same resources. Unique group IDs and keys isolate retained records.
5. Deletes the owned streams and waits until they have disappeared or reached `DELETED`.
6. Deletes the empty pool and verifies that both pool and streams have disappeared or reached `DELETED`.

OCI rejects deletion of a non-empty pool with `InvalidParameter`. The stream-first sequence was confirmed
against `us-sanjose-1` with OCI CLI 3.94.1 on October 5, 2026. Records in state `DELETED` can remain visible
in listings; they are terminal records, not pending resources. Cleanup also resumes deletions already in progress.

The E2E sends twelve small, uncompressed records per execution and has a four-minute test deadline.
The local runner imposes a five-minute process deadline per execution. It uses `acks=ALL`, disables
idempotence and uses classic consumer groups. Transactions and idempotent production are not supported
by OCI Streaming. Fetch requests are limited to 1 MiB, including per-partition fetch limits.

CLI calls have a 60-second process timeout. Readiness, stream deletion verification, and final pool cleanup
polling each have a ten-minute deadline
(an in-flight CLI call can finish after the polling deadline). Provisioning failure triggers cleanup,
including when the pool was created but its response was lost. A smoke failure also triggers cleanup;
a cleanup failure makes the overall command fail.

## Run individual steps

For investigation, provision the resources and print non-secret connection settings:

```sh
node scripts/oracle-streaming-resources.ts provision "$ORACLE_STREAMING_RUN_ID"
node scripts/oracle-streaming-resources.ts connection "$ORACLE_STREAMING_RUN_ID"
```

The second command prints JSON with `ORACLE_STREAMING_BOOTSTRAP_SERVERS`, `ORACLE_STREAMING_TOPIC`
and `ORACLE_STREAMING_USERNAME`. Export these values in your shell, retaining `ORACLE_STREAMING_AUTH_TOKEN`,
then run:

```sh
pnpm run test:e2e:oracle-streaming
pnpm run test:e2e:oracle-streaming
node scripts/oracle-streaming-resources.ts cleanup "$ORACLE_STREAMING_RUN_ID"
```

The standalone test only consumes connection variables; it does not provision or delete resources.
Individual steps intentionally leave teardown under your control.

## Recover after interruption

If the process is interrupted, killed, or loses connectivity, cleanup may not finish. Run:

```sh
node scripts/oracle-streaming-resources.ts cleanup "$ORACLE_STREAMING_RUN_ID"
```

Use the same compartment, region, run ID and CLI profile. Kafka credentials are not needed for cleanup.
Names include a hash of compartment/region/repository and the run ID. Pools and streams are tagged with
`purpose=kafka-oracle-streaming-local`, `repository=platformatic/kafka`, and `run`.
Discovery uses all listing pages, so it does not depend on a local state file or a saved creation response.

Cleanup selects only explicitly named pools with matching ownership tags. Before issuing any deletion,
it also validates the names and ownership tags of their streams. OCI's default pool is not selected and cannot be deleted.
An already absent environment is a successful cleanup. OCI CLI can emit no output for a successful empty listing;
this is treated as an empty collection only when the CLI exits successfully. API failures and malformed JSON
responses are errors, not evidence of absence. If creation was still propagating when cleanup ran, repeat cleanup with the same ID.
There is no unattended recovery workflow for local runs.

Provisioning refuses an existing name. Choose a new ID for a fresh environment, or clean up the previous one.
Service usage and retained data can remain billable until deletion completes; OCI Streaming charges for
traffic and storage within standard service limits. Check the tenancy's applicable rates.

Script validation, CLI, readiness and cleanup errors use the existing `PLT_KFK_USER` code.
Resource operations are covered by mocked CLI tests that reject deletion of non-empty pools and retain
`DELETED` records. Two consecutive E2E executions passed against real OCI Streaming in `us-sanjose-1`
on October 5, 2026. A subsequent complete run passed both E2E executions and verified the corrected
automatic stream-first teardown without manual intervention.

## Hosted resource lifecycle

The **Regression / Oracle Streaming** job in [Regression Tests](../../.github/workflows/regression.yml)
runs on `ubuntu-latest`: WIF login, public stream-pool provisioning, two-partition stream creation,
two consecutive Kafka smoke executions, and verified stream-first teardown. It does not change IAM.
On `main`, its results participate in the aggregate regression report and Slack failure notifications.
The full hosted lifecycle passed on October 7, 2026; independent hosted recovery validation remains pending.

### Authentication model

[`scripts/oracle-streaming-auth.ts`](../../scripts/oracle-streaming-auth.ts) requests a GitHub OIDC JWT with
audience `https://cloud.oracle.com`, generates an ephemeral 2048-bit RSA key, and exchanges the JWT for an
OCI User Principal Session Token (UPST). Its request matches the OCI SDK's `TokenExchangeSigner` protocol.
OCI validates the GitHub signature and impersonation rule, then binds the session to the generated key.
The script writes a new, private CLI `security_token` profile under `RUNNER_TEMP` and exports only its path,
auth mode and profile name to subsequent steps. It never overwrites a local CLI configuration.

WIF removes the permanent OCI API signing key, **not every persistent secret**: this flow still requires an
OAuth client secret to authenticate token exchange. Kafka SASL/PLAIN needs a separate auth token,
available only to configuration validation and smoke steps. Do not upload a personal API private key.

Tokens are short-lived. OCI can limit the session to the source JWT's remaining lifetime; do not assume
that every session lasts an hour. The CI helper exchanges fresh credentials before resource operations,
refreshes at least once a minute during polling, and leaves a 90-second lifetime margin for bounded
60-second CLI calls. Cleanup obtains its own fresh session even after a failed smoke.

### One-time OCI setup (administrator)

Perform these operations in the Identity Domain used by the tenancy. Console labels may differ by domain
version; the Identity Propagation Trust can require the domain's SCIM REST API rather than a console form.

1. Create a dedicated **service user** for the GitHub workload. Do not use the personal administrator user.
   Follow the service-user setup in Oracle's WIF guide for the domain's supported SCIM schema; note the
   domain service-user identifier returned by that operation. It is this domain ID, not a tenancy OCID,
   that the trust uses.
2. Put the service user in a dedicated group. Scope its OCI policy to the smoke compartment only.
   The initial read-only check used `read stream-family`. The complete lifecycle requires
   `manage stream-family` in that compartment, without tenancy-wide administrative privileges.
3. Create and activate a **confidential OAuth application** for runtime token exchange, enabling the
   **Client credentials** grant. Save its client ID and secret. Assign **no Identity Domain administrator
   roles** to this application. Administrative SCIM setup must use a separate administrator identity/client.
4. Configure an active **Identity Propagation Trust** for GitHub. If the domain already has an active trust
   for this issuer, inspect and extend it without replacing existing mappings. Restrict this application
   and service-user mapping to the exact audience and environment subject below.

The relevant trust fields are:

```json
{
  "active": true,
  "allowImpersonation": true,
  "issuer": "https://token.actions.githubusercontent.com",
  "name": "GitHub Kafka Oracle Streaming",
  "oauthClients": ["<runtime-oauth-client-id>"],
  "publicKeyEndpoint": "https://token.actions.githubusercontent.com/.well-known/jwks",
  "clientClaimName": "aud",
  "clientClaimValues": ["https://cloud.oracle.com"],
  "impersonationServiceUsers": [
    {
      "rule": "sub eq repo:platformatic/kafka:environment:oracle",
      "value": "<identity-domain-service-user-id>"
    }
  ],
  "subjectType": "User",
  "type": "JWT",
  "schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:IdentityPropagationTrust"]
}
```

An environment subject does not include the branch. The dedicated `oracle` environment must restrict
deployments to the branch `main`, with no other branches or tags allowed. The workflow and auth script
also require a trusted ref, but those checks are additional safeguards, not substitutes for the environment's
branch policy. Authentication and resource management now require `main` only.
Never use a wildcard subject or give the runtime OAuth application administrative domain roles.
Verify WIF/service-user availability in the actual Identity Domain before making any changes.

### GitHub configuration (repository administrator)

Create the dedicated GitHub Environment **`oracle`**. In **Deployment branches and tags**, select
**Selected branches and tags** and add only **Branch → `main`**. Leave required reviewers and wait timers
disabled so that future recovery can run without manual approval. Add the environment variables and
secrets below without changing the existing `regression` or `eventhubs` settings.

| Name                               | Type     | Value                                                                                        |
| ---------------------------------- | -------- | -------------------------------------------------------------------------------------------- |
| `OCI_WIF_DOMAIN_URL`               | Variable | `https://<domain>.identity.oraclecloud.com`, no path/query                                   |
| `OCI_WIF_CLIENT_ID`                | Variable | Runtime confidential application's client ID                                                 |
| `OCI_WIF_CLIENT_SECRET`            | Secret   | Runtime application's client secret                                                          |
| `OCI_TENANCY_ID`                   | Variable | Tenancy OCID                                                                                 |
| `OCI_CLI_REGION`                   | Variable | `us-sanjose-1` for the locally validated region                                              |
| `OCI_COMPARTMENT_ID`               | Variable | Dedicated smoke compartment OCID                                                             |
| `ORACLE_STREAMING_USERNAME_PREFIX` | Variable | `<tenancy-name>/<identity-domain-name>/<dedicated-kafka-username>`, without a trailing slash |
| `ORACLE_STREAMING_AUTH_TOKEN`      | Secret   | Dedicated Kafka user's OCI auth token                                                        |
| `SLACK_WEBHOOK_URL`                | Secret   | Recovery failure notification webhook                                                        |

Set the secret through GitHub's secret input or an interactive CLI prompt, not command-line arguments,
source files, logs, or chat. Generate the Kafka auth token on a dedicated OCI user with Streaming access
in the smoke compartment. Do not assume that the WIF service user supports Kafka auth tokens;
use a separate dedicated domain user if necessary. The pool OCID is appended to the username at runtime.

Run **Actions → Regression Tests → Run workflow → main** for the full regression. Success requires provisioning, both smoke executions
and verified deletion; authentication alone is insufficient. Inspect the failed step and lifecycle summary.
The auth helper
reports HTTP status, a recognized OAuth error code and a validated OCI request ID when available, using
the stable `PLT_KFK_USER` error. Unknown codes, error descriptions, HTTP bodies, JWTs and client secrets
are never logged. Validated `x-oracle-dms-ecid` values are also reported for OCI correlation.
`invalid_client` points to client authentication; `invalid_grant` points to the supplied
grant/token validation. These codes guide investigation but do not identify a specific misconfiguration.
Do not enable HTTP/CLI debug logging to troubleshoot with secrets present.

### Validated WIF configuration

GitHub OIDC exchange and both read-only Streaming listings passed on October 7, 2026 in
[run 37609217743](https://github.com/platformatic/kafka/actions/runs/37609217743).
The impersonation rule must use the exact subject without enclosing quotes inside the rule string.
The quoted form was accepted by the SCIM API but failed at runtime with `unauthorized_client` and
`No rules matched from given token to find impersonation user.` Removing the quotes resolved the failure.

The client-credentials probe and error-description diagnostics have been removed.

### Validation results

The complete hosted lifecycle passed in [run 37613892408](https://github.com/platformatic/kafka/actions/runs/37613892408):
WIF login, provisioning, two consecutive smoke executions and verified stream/pool deletion.
Normal cancellation in [run 37616134681](https://github.com/platformatic/kafka/actions/runs/37616134681)
allowed the ordinary cleanup to finish successfully.
Forced cancellation in [run 37616460306](https://github.com/platformatic/kafka/actions/runs/37616460306)
interrupted smoke and skipped cleanup. The recovery helper was then executed locally with the previously
validated OCI API credentials, verified the owning GitHub run, and deleted the remaining stream and pool.
This validates real recovery behavior, not the independent hosted recovery workflow or its WIF credentials.

The temporary `oracle-part-2` exceptions have been removed: other regression lanes, aggregate reporting
and ordinary concurrency are restored. Remove that branch from the `oracle` environment's deployment
branch policy, leaving only `main`.

### Ownership and interrupted-run recovery

CI resources carry `purpose=kafka-oracle-streaming-regression`, `repository`, `run` and `attempt` tags.
Pool names include a compartment/region/repository hash, GitHub run ID and attempt. Local names and tags
are unchanged. Cleanup validates names, compartment, ownership and every selected stream before deletion,
then waits for streams to disappear before deleting the pool and verifies both are absent.

[Oracle Streaming resource recovery](../../.github/workflows/oracle-streaming-cleanup.yml) runs independently
when Regression Tests completes, every six hours, and on manual dispatch. It checks the owning GitHub run
using `actions: read`, deletes only completed trusted regression runs on `main`, and preserves active and local runs.
Unknown ownership, unverifiable runs, mismatched resource names and orphan streams fail closed and require
operator inspection rather than reporting successful recovery. Recovery failures notify Slack.

Automatic recovery needs the recovery workflow and helpers on trusted `main`; `workflow_run` and schedules
do not activate from an unpublished feature branch. GitHub also refused manual dispatch before the workflow
was registered on the default branch. Validate the independent hosted workflow after integration into `main`;
this follow-up has been deferred. Never dispatch recovery using untrusted code or artifacts.

## References

- [Kafka compatibility and limitations](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility.htm)
- [Kafka authentication and recommended settings](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility_topic-Configuration.htm)
- [Stream pool deletion](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/delete-stream-pool.htm)
- [Oracle A-Team: GitHub Actions and OCI OIDC token exchange](https://www.ateam-oracle.com/github-actions-oci-a-guide-to-secure-oidc-token-exchange)
  explains the confidential runtime application, service user and exact-subject impersonation trust.
- [OCI SDK TokenExchangeSigner](https://github.com/oracle/oci-python-sdk/blob/master/src/oci/auth/signers/token_exchange_signer.py)
  defines the UPST exchange parameters and ephemeral public-key binding used by this helper.
- [Oracle A-Team: Python WIF signer](https://www.ateam-oracle.com/simplifying-token-exchange-with-wif-python-signer)
  documents source-token lifetime constraints and the need for refresh in long-running jobs.
