// The storage controller and the pageservers as an upgrade reads and drives them: what `state.ts` and `roll.ts` call, run in a pod that
// holds the signing key (KEY_DIR, `/keys` by default: the control layer's), the one caller the storage controller's and the pageservers'
// APIs admit. Nothing here is imported by the control layer's program.
import { createPrivateKey, sign } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { fromEnv } from '../names'

const key = createPrivateKey(readFileSync(`${process.env['KEY_DIR'] ?? '/keys'}/auth_private_key.pem`, 'utf8'))
const NS = fromEnv().namespace
const b64 = (x: Buffer | string) => Buffer.from(x).toString('base64url')
export const mint = (scope: string): string => {
  const head = b64(JSON.stringify({ alg: 'EdDSA', typ: 'JWT' }))
  const body = b64(JSON.stringify({ scope }))
  return `${head}.${body}.${b64(sign(null, Buffer.from(`${head}.${body}`), key))}`
}
const call = async (url: string, scope: string, method: string, body?: unknown): Promise<{ status: number; json: any }> => {
  const r = await fetch(url, {
    method,
    headers: { authorization: `Bearer ${mint(scope)}`, ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(20_000),
  })
  const text = await r.text()
  let json: any = text
  try { json = text ? JSON.parse(text) : {} } catch { /* not JSON: keep the text */ }
  return { status: r.status, json }
}
/** The storage controller (admin scope: the controller lets it do what its infra scope does). */
export const storcon = (method: string, path: string, body?: unknown) => call(`http://storage-controller.${NS}.svc:1234${path}`, 'admin', method, body)
/** A pageserver's HTTP API, by the node the controller lists (`listen_http_addr` and `listen_http_port`). */
export const pageserver = (node: { listen_http_addr: string; listen_http_port: number }, path: string) => call(`http://${node.listen_http_addr}:${node.listen_http_port}${path}`, 'pageserverapi', 'GET')
export const nodes = async (): Promise<any[]> => (await storcon('GET', '/control/v1/node')).json as any[]
export const tenants = async (): Promise<any[]> => (await storcon('GET', '/control/v1/tenant')).json as any[]
/** The controller's own view of every shard (generation, intent, observed locations): the debug listing. */
export const debugTenants = async (): Promise<any[]> => (await storcon('GET', '/debug/v1/tenant')).json as any[]
/** What a pageserver lists as held (`GET /v1/tenant`), by tenant id: attached or not, and at which generation. */
export async function heldByPageserver(node: { listen_http_addr: string; listen_http_port: number }): Promise<Map<string, { attached: boolean; generation: number | null }>> {
  const r = await pageserver(node, '/v1/tenant')
  if (r.status !== 200 || !Array.isArray(r.json)) throw new Error(`pageserver tenant list: ${r.status}`)
  return new Map(r.json.map((t: any) => [String(t.id), { attached: t.attachment_status?.slug === 'attached', generation: typeof t.generation === 'number' ? t.generation : null }]))
}
