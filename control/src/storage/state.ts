// The platform's storage as an upgrade's check reads it. Read only; run in a pod that holds the signing key, as `bun state.ts`.
// Prints each pageserver's state and the verdict of `storageStateReport` (storage-spec.ts, tested), warmth of every secondary included, and
// exits 1 with the problems when there are any.
import { debugTenants, heldByPageserver, nodes as listNodes, pageserver, tenants as listTenants } from './api'
import { shardLocations, shardsWithError, storageStateReport } from '../storage-spec'

const nodes = await listNodes()
const tenants = await listTenants()
const byId = new Map<number, any>(nodes.map((n) => [Number(n.id), n]))
const statuses = new Map<string, any>()
for (const t of tenants) for (const s of t.shards ?? []) for (const sec of (s.node_secondary ?? []) as number[]) {
  const node = byId.get(sec)
  if (!node) continue
  const r = await pageserver(node, `/v1/tenant/${s.tenant_shard_id}/secondary/status`).catch(() => undefined)
  statuses.set(`${s.tenant_shard_id}/${sec}`, r && r.status === 200 ? r.json : undefined)
}
// what the controller observes and what each pageserver says it holds, to tell a leftover error from a live one (`staleErrorProblem`): read only for the shards that carry an error
const withError = shardsWithError(tenants)
const held: Record<string, Map<string, { attached: boolean; generation: number | null }> | undefined> = {}
if (withError.size > 0) for (const n of nodes) held[String(n.id)] = await heldByPageserver(n).catch(() => undefined)
const locations = withError.size > 0 ? shardLocations(await debugTenants().catch(() => [] as any[]), held, withError) : new Map()
const { problems, warnings } = storageStateReport(nodes, tenants, nodes.map((n) => Number(n.id)), statuses, locations) // every pageserver the controller knows is expected Active
for (const n of nodes) console.log(`node ${n.id}: availability ${JSON.stringify(n.availability)}, scheduling ${JSON.stringify(n.scheduling)}`)
const perNode: Record<string, number> = {}
for (const t of tenants) for (const s of t.shards ?? []) perNode[String(s.node_attached)] = (perNode[String(s.node_attached)] ?? 0) + 1
console.log(`${tenants.length} tenants; attached per node: ${JSON.stringify(perNode)}; ${statuses.size} secondaries read`)
for (const w of warnings) console.log(`warning: ${w}`)
if (problems.length) { console.log(`NOT SETTLED:\n  ${problems.join('\n  ')}`); process.exit(1) }
console.log(`settled: every node Active, every shard attached with its secondaries, every secondary warm, nothing pending${warnings.length ? ` (${warnings.length} leftover error${warnings.length > 1 ? 's' : ''}, warned above)` : ''}`)
