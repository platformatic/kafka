# Azure Event Hubs smoke tests

The Event Hubs smoke test exercises the Kafka endpoint of a real Azure Event Hubs namespace.
It verifies TLS/SASL authentication, metadata, production and consumption on two partitions, consumer group
joining, explicit offset commits, resuming those offsets with a new consumer, and receiving new messages
on an already running stream after its initial fetch.

In CI, the test is an independent GitHub-hosted lane of [Regression Tests](../.github/workflows/regression.yml),
on `main` only. Each execution provisions its own Azure environment, runs the smoke test, and deletes the
environment with verification. It is not part of the regular CI workflow or pull requests.
The smoke test is separate from `pnpm test`, `pnpm run test:ci`, and the API compatibility sweeps.

The manual creation, local execution, and destruction instructions below remain available for an optional
persistent development environment. Automated cleanup does not touch these manually created resources.

## Prerequisites

- A current [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) and access to a subscription
  where you can create resource groups, Event Hubs namespaces, and SAS authorization rules.
- [GitHub CLI](https://cli.github.com/) and repository administration access for the CI configuration.
- Node.js and pnpm as described in [CONTRIBUTING.md](../CONTRIBUTING.md).
- Outbound TLS access to `<namespace>.servicebus.windows.net:9093` from the test runner.

The commands below use Bash syntax. Run them in the same shell; retain the subscription, resource group,
and namespace names for future credential retrieval and teardown. Replace the example subscription and
namespace before running the commands. Namespace names must be globally unique.

The script and regression workflow default to `northeurope`, where provisioning, repeated smoke runs,
and verified deletion have been exercised against Azure. Override `AZURE_LOCATION` to use another region
available to your subscription. In Fish, use `set -gx NAME value` instead of `export NAME=value`,
`(command)` instead of `$(command)`, and `set -e NAME` instead of `unset NAME`.

## Create the Azure resources manually

```bash
az login

export AZURE_SUBSCRIPTION_ID='<subscription-id>'
export AZURE_RESOURCE_GROUP='platformatic-kafka-eventhubs-smoke'
export AZURE_LOCATION='northeurope'
export EVENTHUBS_NAMESPACE='<globally-unique-namespace>'
export EVENTHUBS_TOPIC='kafka-smoke'
export EVENTHUBS_SAS_POLICY='kafka-smoke'

az account set --subscription "$AZURE_SUBSCRIPTION_ID"
az provider register --namespace Microsoft.EventHub --wait

az group create \
  --name "$AZURE_RESOURCE_GROUP" \
  --location "$AZURE_LOCATION" \
  --tags purpose=kafka-eventhubs-smoke \
  --output none

az eventhubs namespace create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$EVENTHUBS_NAMESPACE" \
  --location "$AZURE_LOCATION" \
  --sku Standard \
  --capacity 1 \
  --enable-auto-inflate false \
  --enable-kafka true \
  --minimum-tls-version 1.2 \
  --output none

az eventhubs eventhub create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENTHUBS_NAMESPACE" \
  --name "$EVENTHUBS_TOPIC" \
  --partition-count 2 \
  --cleanup-policy Delete \
  --retention-time-in-hours 24 \
  --enable-capture false \
  --output none

az eventhubs namespace authorization-rule create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENTHUBS_NAMESPACE" \
  --name "$EVENTHUBS_SAS_POLICY" \
  --rights Send Listen \
  --output none
```

Standard is the smallest tier with Kafka support. One throughput unit is shared by both partitions.
The namespace uses its public endpoint and SAS authentication. No VM, storage account, private endpoint,
Capture, or geo-replication resource is needed. Keep this resource group dedicated to the smoke environment.

Kafka consumer groups are managed through the Kafka protocol; do not create an Azure/AMQP consumer group
for each test run. The test generates a unique Kafka group ID and message keys, filters out older runs,
and leaves messages to expire after 24 hours. It does not create or delete topics through Kafka Admin APIs.

Check the provisioned resources:

```bash
az eventhubs namespace show \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$EVENTHUBS_NAMESPACE" \
  --query '{state:provisioningState,sku:sku.name,capacity:sku.capacity,kafka:kafkaEnabled}'

az eventhubs eventhub show \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENTHUBS_NAMESPACE" \
  --name "$EVENTHUBS_TOPIC" \
  --query '{status:status,partitions:partitionCount,retention:retentionDescription}'
```

Wait for a successful namespace provisioning state and an active Event Hub before running the test.
New authorization rules can also take a short time to propagate.

### Region availability and retries

Azure may reject namespace creation with `RequestDisallowedByAzure` and "The selected region is currently
not accepting new customers". This occurred in `westeurope` during setup; the same subscription succeeded
in `northeurope`. Availability is subscription-dependent, so this is not a guarantee for every account.

For a failed scripted provisioning attempt, run `node scripts/eventhubs-resources.ts cleanup` with the same
subscription, repository, run ID, and attempt. Wait for verified deletion, change `AZURE_LOCATION`, and run
`node scripts/eventhubs-resources.ts provision` again. Provisioning intentionally refuses an existing group.
In GitHub, update the `AZURE_LOCATION` variable in the `eventhubs` Environment before rerunning regression;
an explicit environment variable takes precedence over the workflow default.

## Run locally

Retrieve the namespace connection string into the environment without printing it:

```bash
export EVENTHUBS_BOOTSTRAP_SERVERS="${EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093"
EVENTHUBS_CONNECTION_STRING="$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENTHUBS_NAMESPACE" \
  --name "$EVENTHUBS_SAS_POLICY" \
  --query primaryConnectionString \
  --output tsv)"
export EVENTHUBS_CONNECTION_STRING

pnpm install --frozen-lockfile
pnpm run test:e2e:eventhubs

unset EVENTHUBS_CONNECTION_STRING
```

Run the test a second time against the same Event Hub to check isolation from retained messages.
The test sends twelve small, uncompressed messages per run, uses manual commits, and has a four-minute
deadline with bounded request retries. A missing environment variable fails the test rather than skipping it.
The SASL username is the literal `$ConnectionString`; the password is the complete namespace connection string.

## Configure automated regression runs

The Event Hubs lane lives directly in [`.github/workflows/regression.yml`](../.github/workflows/regression.yml).
It uses Node.js 24 on a GitHub-hosted runner and has a 45-minute job limit, with separate provisioning,
smoke, and cleanup step limits. No permanent Kafka connection string is required: GitHub authenticates
to Azure using OIDC and obtains a temporary namespace's SAS key during the run. The key is masked before
being passed to later steps, and is never included in artifacts.

### One-time Azure identity and permissions

An administrator runs these commands once. Use a subscription intended for testing: the identity needs
subscription-level access to create and delete per-run resource groups, and manage Event Hubs resources.
The custom role below grants those operations without granting role-assignment management. This setup
creates a persistent identity in its own group, separate from both manual and per-run Event Hubs resources.

```bash
az login
export AZURE_SUBSCRIPTION_ID='<subscription-id>'
export AZURE_LOCATION='northeurope'
export GITHUB_REPOSITORY='platformatic/kafka'
export AZURE_AUTOMATION_GROUP='platformatic-kafka-eventhubs-automation'
export AZURE_IDENTITY_NAME='kafka-eventhubs-regression'
export AZURE_ROLE_NAME='Kafka Event Hubs Regression'
az account set --subscription "$AZURE_SUBSCRIPTION_ID"
az provider register --namespace Microsoft.EventHub --wait
az provider register --namespace Microsoft.ManagedIdentity --wait

az group create --name "$AZURE_AUTOMATION_GROUP" --location "$AZURE_LOCATION" --output none
az identity create \
  --resource-group "$AZURE_AUTOMATION_GROUP" --name "$AZURE_IDENTITY_NAME" --output none

AZURE_CLIENT_ID="$(az identity show --resource-group "$AZURE_AUTOMATION_GROUP" \
  --name "$AZURE_IDENTITY_NAME" --query clientId --output tsv)"
AZURE_PRINCIPAL_ID="$(az identity show --resource-group "$AZURE_AUTOMATION_GROUP" \
  --name "$AZURE_IDENTITY_NAME" --query principalId --output tsv)"
AZURE_TENANT_ID="$(az account show --query tenantId --output tsv)"

az identity federated-credential create \
  --resource-group "$AZURE_AUTOMATION_GROUP" \
  --identity-name "$AZURE_IDENTITY_NAME" \
  --name github-eventhubs \
  --issuer https://token.actions.githubusercontent.com \
  --subject "repo:${GITHUB_REPOSITORY}:environment:eventhubs" \
  --audiences api://AzureADTokenExchange \
  --output none

az role definition create --role-definition "$(jq -n \
  --arg name "$AZURE_ROLE_NAME" \
  --arg scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}" \
  '{Name: $name, IsCustom: true, Description: "Provision and remove Kafka Event Hubs regression resources", Actions: ["Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Resources/subscriptions/resourceGroups/write", "Microsoft.Resources/subscriptions/resourceGroups/delete", "Microsoft.EventHub/namespaces/*"], NotActions: [], AssignableScopes: [$scope]}')" \
  --output none

az role assignment create \
  --assignee-object-id "$AZURE_PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role "$AZURE_ROLE_NAME" \
  --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}" \
  --output none
```

Allow time for the identity and role assignment to propagate before the first regression run. Keep the
identity and role names so that they can be removed when retiring the automation.

### GitHub Environment

```bash
gh auth login
gh api --method PUT "repos/${GITHUB_REPOSITORY}/environments/eventhubs" \
  --input - --silent <<'JSON'
{"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}
JSON

gh api --method POST "repos/${GITHUB_REPOSITORY}/environments/eventhubs/deployment-branch-policies" \
  -f name=main -f type=branch --silent

gh variable set AZURE_CLIENT_ID --repo "$GITHUB_REPOSITORY" --env eventhubs --body "$AZURE_CLIENT_ID"
gh variable set AZURE_TENANT_ID --repo "$GITHUB_REPOSITORY" --env eventhubs --body "$AZURE_TENANT_ID"
gh variable set AZURE_SUBSCRIPTION_ID --repo "$GITHUB_REPOSITORY" --env eventhubs --body "$AZURE_SUBSCRIPTION_ID"
gh variable set AZURE_LOCATION --repo "$GITHUB_REPOSITORY" --env eventhubs --body "$AZURE_LOCATION"
```

The branch policy restricts use of the federated identity to `main`. Do not add required reviewers or wait
timers: both regression execution and unattended cleanup need access without manual approval.
Configure `SLACK_WEBHOOK_URL` in both GitHub Environments: `regression` for the aggregate report job,
and `eventhubs` for recovery failure notifications. The same webhook can be used for both. Environment
protection rules on `regression` also apply to the report job.

These commands prompt for the webhook without putting its value in shell history:

```bash
gh secret set SLACK_WEBHOOK_URL --repo "$GITHUB_REPOSITORY" --env regression
gh secret set SLACK_WEBHOOK_URL --repo "$GITHUB_REPOSITORY" --env eventhubs
```

Trigger the full regression workflow after it is present on `main`:

```bash
gh workflow run regression.yml --repo "$GITHUB_REPOSITORY" --ref main
```

### Lifecycle and verified cleanup

[`scripts/eventhubs-resources.ts`](../scripts/eventhubs-resources.ts) implements `provision`, `credentials`,
`cleanup`, and `recover`. Names include a hash of subscription/repository, the run ID, and the attempt.
The resource group is tagged with `purpose=kafka-eventhubs-regression`, `repository`, `run`, and `attempt`.
Its name is known before Azure creation begins, including when provisioning subsequently fails.

The regression lane creates a Standard namespace with one TU, an Event Hub with two partitions and
24-hour retention, and a Send/Listen SAS policy. Auto-inflate and Capture are disabled. Provisioning waits
for namespace readiness with a bounded deadline, and allows SAS propagation before the test's bounded
connection retries. Namespace creation runs synchronously with a ten-minute CLI timeout: Azure CLI 2.79.0
does not support `--no-wait` for this operation. Other CLI calls retain their 60-second timeout.
The lane reports provisioning, smoke, and cleanup outcomes separately.

An `always()` step deletes the group even after partial provisioning or a smoke failure. It validates
ownership before deletion, then polls `az group exists` until Azure explicitly returns `false`. Azure CLI
errors and polling timeouts fail the lane; a deletion request alone never counts as successful cleanup.
An already absent group is a successful, idempotent cleanup. Deletion verification polls up to 40 times
with 15-second intervals; cleanup CLI calls have their own 60-second limit, within the workflow step's 15-minute limit.

If the runner disappears, a job times out, or the workflow is force-cancelled, that final step may never run.
[Event Hubs resource recovery](../.github/workflows/eventhubs-cleanup.yml) therefore runs independently on
completion of Regression Tests and every six hours, and can also be dispatched manually. It uses trusted
`main` code, lists tagged groups in the configured subscription, and checks each owning GitHub run:

- Only matching repository, naming scheme, ownership tags, `regression.yml`, and `main` are eligible.
- Only runs whose current status is `completed` are cleaned; active or queued runs are left alone.
- Unverifiable GitHub/Azure responses fail recovery without deleting the affected group.
- Deletion is verified in the same way as ordinary cleanup; failures are reported to Slack.
- Manual development groups and the permanent identity group have different tags and are not selected.

The schedule retries leftovers, but is not an immediate billing cutoff: GitHub scheduling delays and Azure
outages can delay deletion. Check the recovery workflow if a run ends without a successful cleanup report.

```bash
gh workflow run eventhubs-cleanup.yml --repo "$GITHUB_REPOSITORY" --ref main
```

To recover a specific run locally, authenticate as an Azure administrator and use its original identifiers:

```bash
export AZURE_SUBSCRIPTION_ID='<subscription-id>'
export GITHUB_REPOSITORY='platformatic/kafka'
export GITHUB_RUN_ID='<original-run-id>'
export GITHUB_RUN_ATTEMPT='<original-attempt>'
node scripts/eventhubs-resources.ts cleanup
```

To sweep all completed runs locally, authenticate both `az` and `gh` and run
`node scripts/eventhubs-resources.ts recover`. Neither cleanup command needs a Kafka SAS key or installed
npm dependencies. These commands delete only the tagged automation resources, not the manual environment.

## Manual connection settings in GitHub (optional)

The original persistent-environment configuration below is retained for manual integrations. The regression
lane no longer consumes these variables or this secret; it uses OIDC and per-run resources instead.

Create the `eventhubs` GitHub Environment and populate its variables and secret:

```bash
gh auth login
export GITHUB_REPOSITORY='platformatic/kafka'

# Create the environment only if it does not already exist; preserve its OIDC branch policy.
gh api "repos/${GITHUB_REPOSITORY}/environments/eventhubs" --silent || \
  gh api --method PUT "repos/${GITHUB_REPOSITORY}/environments/eventhubs" --silent

gh variable set EVENTHUBS_BOOTSTRAP_SERVERS \
  --repo "$GITHUB_REPOSITORY" --env eventhubs \
  --body "${EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093"
gh variable set EVENTHUBS_TOPIC \
  --repo "$GITHUB_REPOSITORY" --env eventhubs \
  --body "$EVENTHUBS_TOPIC"

az eventhubs namespace authorization-rule keys list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENTHUBS_NAMESPACE" \
  --name "$EVENTHUBS_SAS_POLICY" \
  --query primaryConnectionString \
  --output tsv | gh secret set EVENTHUBS_CONNECTION_STRING \
    --repo "$GITHUB_REPOSITORY" --env eventhubs
```

After rotating the manual environment's SAS key, repeat the secret upload command above if you use this
optional configuration. The old standalone `eventhubs.yml` workflow has been moved into `regression.yml`.

## Costs

For the optional persistent environment, the namespace remains billable even when no tests are running.
As a reference, with one Standard throughput unit in West Europe, the public retail capacity price checked
in September 2026 is USD 0.03/hour, or USD 21.90 for a
730-hour month, plus ingress at USD 0.028 per million events. This is a capacity estimate, not a subscription
quote; check the applicable Kafka endpoint meters and regional pricing before provisioning. Taxes, outbound
network charges, and GitHub runner costs are excluded. Test data fits comfortably in the included retention
storage. Auto-inflate and Capture remain disabled to keep the footprint fixed.
This original West Europe estimate is not a verified North Europe quote; check the price for the region
selected by `AZURE_LOCATION`.

See [Event Hubs pricing](https://azure.microsoft.com/en-us/pricing/details/event-hubs/) and the
[throughput-unit billing FAQ](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-faq#how-are-throughput-units-billed).
Stopping the workflow does not stop Azure billing. Delete the namespace when retiring the environment.

Automated regression environments are billed only for their lifetime, subject to Azure's hourly TU billing.
Budget for provisioning and deletion time as well as the smoke itself. At the capacity rate above, 100 billed
TU-hours cost USD 3.00, before any other applicable meters. Resources awaiting recovery remain billable.

## Destroy all test resources

When retiring the automation, first disable Regression Tests and wait for active runs to finish. Keep the
recovery workflow enabled until all per-run groups have been removed:

```bash
gh workflow disable regression.yml --repo "$GITHUB_REPOSITORY"
gh run list --repo "$GITHUB_REPOSITORY" --workflow regression.yml
node scripts/eventhubs-resources.ts recover
```

If you also created the optional manual environment, delete its dedicated Azure resource group.
This removes the namespace, Event Hub, authorization rules,
messages, and committed offsets. All resources in this group will be deleted, so use the same dedicated
group and subscription selected during creation. The command waits for deletion to complete.

```bash
az group delete \
  --subscription "$AZURE_SUBSCRIPTION_ID" \
  --name "$AZURE_RESOURCE_GROUP" \
  --yes

az group exists \
  --subscription "$AZURE_SUBSCRIPTION_ID" \
  --name "$AZURE_RESOURCE_GROUP"
```

The final command should return `false`. Once recovery has succeeded and no regression runs remain active,
disable recovery and remove the permanent identity, its role assignment and custom role, and its group:

```bash
gh workflow disable eventhubs-cleanup.yml --repo "$GITHUB_REPOSITORY"
az role assignment delete \
  --assignee "$AZURE_PRINCIPAL_ID" --role "$AZURE_ROLE_NAME" \
  --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}"
az role definition delete --name "$AZURE_ROLE_NAME"
az group delete --subscription "$AZURE_SUBSCRIPTION_ID" --name "$AZURE_AUTOMATION_GROUP" --yes
az group exists --subscription "$AZURE_SUBSCRIPTION_ID" --name "$AZURE_AUTOMATION_GROUP"
```

The final command should return `false`; deleting the identity group also removes its federated credential.
Remove the dedicated GitHub Environment, including its secrets and variables, and clear the local connection string:

```bash
gh api --method DELETE "repos/${GITHUB_REPOSITORY}/environments/eventhubs" --silent
unset EVENTHUBS_CONNECTION_STRING
```

To restore automation, repeat identity and GitHub configuration, then enable `regression.yml` and
`eventhubs-cleanup.yml` with `gh workflow enable`. For a recreated manual namespace, retrieve its new
connection string; the previous key and committed offsets are no longer valid.
