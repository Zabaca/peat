// One pageserver's restart as the storage controller's own rolling restart does it: drain the node, delete its pod, fill it. Run in a pod
// that holds the signing key, as `bun roll.ts`; the action is in ROLL_ACTION and the node's id in ROLL_NODE. Each wait ends after 10 minutes with the state that is left and exit code 1: the caller stops and reports, and a
// drain or fill that did not finish is neither cancelled nor repeated here (`cancel-drain` is the caller's decision). Actions:
//   status        read only: every node's state, and which shards are attached and secondary on this node
//   drain         PUT /control/v1/node/<id>/drain, then wait until the node is `PauseForRestart` and holds no attached shard
//   fill          wait until the node is available, put its scheduling back to `Active` (a drained node stays `PauseForRestart` through its restart), PUT /control/v1/node/<id>/fill, wait until it is `Active` again
//   cancel-drain  DELETE /control/v1/node/<id>/drain
import { nodes as listNodes, storcon, tenants as listTenants } from './api'
import { nodeDrainState, nodeFillState } from '../storage-spec'

const action = process.env['ROLL_ACTION'] ?? ''
const id = Number(process.env['ROLL_NODE'] ?? '')
const WAIT_S = Number(process.env['ROLL_WAIT_S'] ?? '600')
if (!['status', 'drain', 'fill', 'cancel-drain'].includes(action) || !Number.isInteger(id) || id < 1) { console.log('ROLL_ACTION is status, drain, fill or cancel-drain; ROLL_NODE is a node id'); process.exit(2) }
const fail = (msg: string): never => { console.log(`STOP: ${msg}`); process.exit(1) }
const waitFor = async (what: string, check: () => Promise<{ done: boolean; why: string }>) => {
  const t0 = Date.now()
  let last = ''
  while (Date.now() - t0 < WAIT_S * 1000) {
    const r = await check().catch((e) => ({ done: false, why: `read failed: ${e}` }))
    if (r.done) { console.log(`${what}: done after ${Math.round((Date.now() - t0) / 1000)} s`); return }
    if (r.why !== last) { console.log(`${what}: ${r.why}`); last = r.why }
    await new Promise((res) => setTimeout(res, 3000))
  }
  fail(`${what} did not finish in ${WAIT_S} s (${last})`)
}

if (action === 'status') {
  const [ns, ts] = [await listNodes(), await listTenants()]
  for (const n of ns) console.log(`node ${n.id}: availability ${JSON.stringify(n.availability)}, scheduling ${JSON.stringify(n.scheduling)}`)
  const shards = ts.flatMap((t) => (t.shards ?? []) as any[])
  console.log(`node ${id}: ${shards.filter((s) => Number(s.node_attached) === id).length} shards attached, ${shards.filter((s) => ((s.node_secondary ?? []) as number[]).includes(id)).length} secondary`)
  console.log(`drain would be: ${JSON.stringify(nodeDrainState(ns, ts, id))}; fill would be: ${JSON.stringify(nodeFillState(ns, id))}`)
} else if (action === 'cancel-drain') {
  const r = await storcon('DELETE', `/control/v1/node/${id}/drain`)
  console.log(`cancel drain: ${r.status} ${JSON.stringify(r.json).slice(0, 200)}`)
  if (r.status >= 300) process.exit(1)
} else if (action === 'drain') {
  const r = await storcon('PUT', `/control/v1/node/${id}/drain`)
  if (r.status >= 300) fail(`the controller refused the drain of node ${id}: ${r.status} ${JSON.stringify(r.json).slice(0, 300)}`)
  console.log(`drain of node ${id} accepted (${r.status})`)
  await waitFor(`drain of node ${id}`, async () => nodeDrainState(await listNodes(), await listTenants(), id))
} else {
  await waitFor(`node ${id} available`, async () => {
    const n = (await listNodes()).find((x) => Number(x.id) === id)
    return n?.availability === 'Active' ? { done: true, why: '' } : { done: false, why: `availability is ${JSON.stringify(n?.availability)}` }
  })
  const cur = (await listNodes()).find((x) => Number(x.id) === id)
  if (String(cur?.scheduling).toLowerCase().replace(/_/g, '') === 'pauseforrestart') {
    const c = await storcon('PUT', `/control/v1/node/${id}/config`, { node_id: id, scheduling: 'Active' })
    if (c.status >= 300) fail(`the controller refused to put node ${id} back to Active: ${c.status} ${JSON.stringify(c.json).slice(0, 300)}`)
    console.log(`node ${id} scheduling put back to Active`)
  }
  const f = await storcon('PUT', `/control/v1/node/${id}/fill`)
  if (f.status >= 300) fail(`the controller refused the fill of node ${id}: ${f.status} ${JSON.stringify(f.json).slice(0, 300)}`)
  console.log(`fill of node ${id} accepted (${f.status})`)
  await waitFor(`fill of node ${id}`, async () => nodeFillState(await listNodes(), id))
}
