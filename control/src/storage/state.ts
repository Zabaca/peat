// The platform's storage as an upgrade's check reads it. Read only; run in a pod that holds the signing key, as `bun state.ts`.
// Prints each pageserver's state and the verdict of `storageStateReport` (storage-spec.ts, tested), warmth of every secondary included, and
// exits 1 with the problems when there are any.
import { debugTenants, heldByPageserver, nodes as listNodes, pageserver, tenants as listTenants } from './api'
import { storageStateReport, type ShardLocations } from '../storage-spec'

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
const withError = new Set<string>(tenants.flatMap((t) => (t.shards ?? []) as any[]).filter((s) => s.last_error).map((s) => String(s.tenant_shard_id)))
const locations = new Map<string, ShardLocations>()
if (withError.size > 0) {
  const held: Record<string, Map<string, { attached: boolean; generation: number | null }> | undefined> = {}
  for (const n of nodes) held[String(n.id)] = await heldByPageserver(n).catch(() => undefined)
  for (const d of await debugTenants().catch(() => [] as any[])) {
    const id = String(d.tenant_shard_id)
    if (!withError.has(id)) continue
    if (Object.values(held).some((h) => h === undefined)) continue // a pageserver did not answer: the shard stays a problem
    locations.set(id, {
      generation: typeof d.generation === 'number' ? d.generation : null,
      intent: { attached: d.intent?.attached ?? null, secondary: (d.intent?.secondary ?? []) as number[] },
      observed: Object.fromEntries(Object.entries(d.observed?.locations ?? {}).map(([n, v]: [string, any]) => [n, v?.conf ? { mode: String(v.conf.mode), generation: v.conf.generation ?? null } : undefined])),
      held: Object.fromEntries(Object.entries(held).map(([n, h]) => [n, h!.get(id)])),
    })
  }
}
const { problems, warnings } = storageStateReport(nodes, tenants, [1, 2], statuses, locations)
for (const n of nodes) console.log(`node ${n.id}: availability ${JSON.stringify(n.availability)}, scheduling ${JSON.stringify(n.scheduling)}`)
const perNode: Record<string, number> = {}
for (const t of tenants) for (const s of t.shards ?? []) perNode[String(s.node_attached)] = (perNode[String(s.node_attached)] ?? 0) + 1
console.log(`${tenants.length} tenants; attached per node: ${JSON.stringify(perNode)}; ${statuses.size} secondaries read`)
for (const w of warnings) console.log(`warning: ${w}`)
if (problems.length) { console.log(`NOT SETTLED:\n  ${problems.join('\n  ')}`); process.exit(1) }
console.log(`settled: every node Active, every shard attached with its secondaries, every secondary warm, nothing pending${warnings.length ? ` (${warnings.length} leftover error${warnings.length > 1 ? 's' : ''}, warned above)` : ''}`)
