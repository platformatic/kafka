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

## Hosted authentication check

Before enabling the hosted resource lifecycle, validate the CI identity using the
**Regression / Oracle Streaming authentication** job in [Regression Tests](../../.github/workflows/regression.yml).
This job runs on a GitHub-hosted runner on `main`, reads pools and streams in the dedicated
compartment, and creates or deletes **no resources**. It does not test Kafka or change IAM configuration.
It runs on pushes to `main` and manual regression executions on `main`. Its results are included in the
aggregate regression report and failure notifications on Slack. There is no standalone authentication workflow.

### Authentication model

[`scripts/oracle-streaming-auth.ts`](../../scripts/oracle-streaming-auth.ts) requests a GitHub OIDC JWT with
audience `https://cloud.oracle.com`, generates an ephemeral 2048-bit RSA key, and exchanges the JWT for an
OCI User Principal Session Token (UPST). Its request matches the OCI SDK's `TokenExchangeSigner` protocol.
OCI validates the GitHub signature and impersonation rule, then binds the session to the generated key.
The script writes a new, private CLI `security_token` profile under `RUNNER_TEMP` and exports only its path,
auth mode and profile name to subsequent steps. It never overwrites a local CLI configuration.

WIF removes the permanent OCI API signing key, **not every persistent secret**: this flow still requires an
OAuth client secret to authenticate token exchange. Kafka SASL/PLAIN needs its own auth token, which this
read-only workflow does not consume. Do not upload the personal API private key used for local validation.

Tokens are short-lived. OCI can limit the session to the source JWT's remaining lifetime; do not assume
that every session lasts an hour. The check exchanges immediately before its two bounded read operations.
The future resource workflow must obtain fresh credentials before provisioning and cleanup, and refresh
within long polling operations. A one-time login is not sufficient for a 45-minute lifecycle job.

### One-time OCI setup (administrator)

Perform these operations in the Identity Domain used by the tenancy. Console labels may differ by domain
version; the Identity Propagation Trust can require the domain's SCIM REST API rather than a console form.

1. Create a dedicated **service user** for the GitHub workload. Do not use the personal administrator user.
   Follow the service-user setup in Oracle's WIF guide for the domain's supported SCIM schema; note the
   domain service-user identifier returned by that operation. It is this domain ID, not a tenancy OCID,
   that the trust uses.
2. Put the service user in a dedicated group. Scope its OCI policy to the smoke compartment only.
   For the first read-only check, grant `read stream-family`. When enabling the full lifecycle later,
   grant `manage stream-family` in that compartment, without tenancy-wide administrative privileges.
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
      "rule": "sub eq 'repo:platformatic/kafka:environment:oracle'",
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
also require `main`, but those checks are additional safeguards, not substitutes for the environment's
branch policy. The OCI identity retains only `read stream-family` during this authentication validation.
Never use a wildcard subject or give the runtime OAuth application administrative domain roles.
Verify WIF/service-user availability in the actual Identity Domain before making any changes.

### GitHub configuration (repository administrator)

Create the dedicated GitHub Environment **`oracle`**. In **Deployment branches and tags**, select
**Selected branches and tags** and add only **Branch → `main`**. Leave required reviewers and wait timers
disabled so that future recovery can run without manual approval. Add the environment variables and
secret below without changing the existing `regression` or `eventhubs` settings.

| Name                    | Type     | Value                                                      |
| ----------------------- | -------- | ---------------------------------------------------------- |
| `OCI_WIF_DOMAIN_URL`    | Variable | `https://<domain>.identity.oraclecloud.com`, no path/query |
| `OCI_WIF_CLIENT_ID`     | Variable | Runtime confidential application's client ID               |
| `OCI_WIF_CLIENT_SECRET` | Secret   | Runtime application's client secret                        |
| `OCI_TENANCY_ID`        | Variable | Tenancy OCID                                               |
| `OCI_CLI_REGION`        | Variable | `us-sanjose-1` for the locally validated region            |
| `OCI_COMPARTMENT_ID`    | Variable | Dedicated smoke compartment OCID                           |

Set the secret through GitHub's secret input or an interactive CLI prompt, not command-line arguments,
source files, logs, or chat. No Kafka token is required for this check.

The regression workflow and auth script must be present on trusted `main` before dispatch. Run
**Actions → Regression Tests → Run workflow → main** to execute the full regression, including this check.
Success means WIF login and both Streaming listings passed; it does not prove create/delete permissions
or Kafka SASL authentication. Inspect the step that failed if the check is unsuccessful. The auth helper
reports HTTP status and a stable `PLT_KFK_USER` error without logging HTTP bodies, JWTs, or client secrets.
Do not enable HTTP/CLI debug logging to troubleshoot with secrets present.

### Next integration milestone

After hosted WIF succeeds, add the regression lane and independent recovery:

- Preserve the environment's `main`-only branch policy when elevating OCI permissions beyond read-only.
- Preserve local run IDs and tags; use distinct CI ownership tags with repository/run/attempt.
- Refresh WIF sessions during bounded CLI operations, including cleanup after a failed smoke.
- Run the same E2E on `ubuntu-latest` and record provisioning, smoke and verified deletion separately.
- Recover only resources owned by completed trusted regression runs; never delete active or local runs.
- Configure the dedicated Kafka user's auth token and Slack secret only where they are needed.

Only the read-only authentication job is integrated into regression at this milestone. Hosted Kafka E2E,
provisioning and resource recovery are not enabled yet.

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
