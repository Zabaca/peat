#!/usr/bin/env bun
// The `peat` command: a platform's databases, through the user's own Kubernetes login (their kubeconfig), so the cluster's permissions
// decide who may do what and nothing listens outside the cluster. Every action is a change to a Database object (or, for the platform, to
// its Platform object), read back until the control layer has answered it.
//
//   peat list [-n <namespace> | -A]                        the databases (all namespaces with -A)
//   peat show <db>                                         one database: where to connect, its version, whether it is awake
//   peat make <db> [--pg 18|19]                            a new empty database (the platform's default version when none is named)
//   peat branch <from> <db>                                a copy-on-write branch of <from> as it is now
//   peat restore <from> <db> --at <time | log position>    <from> as it was at a moment, as a new database (<from> is untouched)
//   peat replica <of> <db>                                 a read replica of <of>
//   peat stop <db>                                         put it to sleep now (the next connection wakes it)
//   peat close <db> | peat open <db>                       refuse every connection until it is opened
//   peat move <db> --pg <version> [--rehearse]             move it to a newer Postgres version under its own name (a rehearsal copies and compares only)
//   peat delete <db> --yes                                 delete it: its compute at once, its data after the platform's hold
//   peat url <db>                                          its connection URL (from its login Secret)
//   peat ca [-n <namespace>]                               the authority's certificate, for sslmode=verify-full
//   peat status                                            the platform: its phase, its release, every piece
//   peat retry                                             go on with a halted upgrade from the step that halted
//
// Options: -n/--namespace (default: the kubeconfig context's), --kubeconfig, --context, --no-wait, --timeout <seconds> (default 180).
import { API_VERSION } from '../api/v1alpha1'
import { DB_NAME } from '../databases'
import { fromKubeconfig, ok, type Kube } from '../kube'

export interface Out { line(s: string): void }
export class Refused extends Error {}

/** `--at 2026-12-01T09:30:00Z` (in the past) or a log position `16/B374D848`. */
export function parseAt(v: string | undefined, now = Date.now()): string {
  if (!v) throw new Refused('--at needs a time such as 2026-12-01T09:30:00Z, or a log position such as 16/B374D848')
  if (/^[0-9A-Fa-f]+\/[0-9A-Fa-f]+$/.test(v)) return v.toUpperCase()
  const t = Date.parse(v)
  if (Number.isNaN(t)) throw new Refused(`--at needs a time such as 2026-12-01T09:30:00Z, or a log position, not ${JSON.stringify(v)}`)
  if (t > now) throw new Refused(`--at ${v} is in the future`)
  return new Date(t).toISOString()
}
export function checkName(name: string | undefined, what = 'the database'): string {
  if (!name || !DB_NAME.test(name)) throw new Refused(`${what}'s name is 1 to 31 lowercase letters, digits or hyphens, starting with a letter: ${JSON.stringify(name ?? '')}`)
  if (name.startsWith('drill')) throw new Refused(`${name}: names beginning "drill" belong to the platform's drills`)
  return name
}
export function parsePg(v: string | undefined): 18 | 19 {
  if (v !== '18' && v !== '19') throw new Refused(`--pg needs 18 or 19, not ${JSON.stringify(v ?? '')}`)
  return Number(v) as 18 | 19
}

/** Arguments: positional words and options (`--x v`, `-n v`, `--flag`). */
export function parseArgs(argv: string[]): { words: string[]; opts: Record<string, string | true> } {
  const words: string[] = [], opts: Record<string, string | true> = {}
  const takes = new Set(['n', 'namespace', 'kubeconfig', 'context', 'timeout', 'pg', 'at'])
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!
    const m = /^--?([a-zA-Z][\w-]*)(?:=(.*))?$/.exec(a)
    if (!m) { words.push(a); continue }
    const k = m[1]!
    if (m[2] !== undefined) opts[k] = m[2]
    else if (takes.has(k)) { const v = argv[++i]; if (v === undefined) throw new Refused(`${a} needs a value`); opts[k] = v }
    else opts[k] = true
  }
  if (opts['n'] !== undefined) opts['namespace'] = opts['n']
  return { words, opts }
}

const dbPath = (ns: string, name?: string) => `/apis/${API_VERSION}/namespaces/${ns}/databases${name ? `/${name}` : ''}`

/** The Database of this name: in the namespace given, else the only one of that name in any namespace the login may list. */
async function findDb(k: Kube, name: string, ns: string | undefined): Promise<any> {
  if (ns) {
    const r = await k.request('GET', dbPath(ns, name))
    if (r.status === 404) throw new Refused(`no database ${name} in ${ns}`)
    return ok(r, `read ${name}`).json
  }
  const all = await k.request('GET', `/apis/${API_VERSION}/databases`)
  if (all.status === 403) return findDb(k, name, k.namespace)
  const found = (ok(all, 'list the databases').json.items as any[]).filter((d) => d.metadata.name === name)
  if (found.length === 0) throw new Refused(`no database ${name}`)
  return found[0]
}

async function waitFor(k: Kube, ns: string, name: string, done: (o: any) => boolean, opts: { timeoutS: number; everyMs: number; out: Out; what: string }): Promise<any> {
  const until = Date.now() + opts.timeoutS * 1000
  let last = ''
  for (;;) {
    const r = await k.request('GET', dbPath(ns, name))
    const o = r.status === 200 ? r.json : null
    if (o && done(o)) return o
    if (o?.status?.phase === 'Failed') throw new Refused(`${name}: ${o.status.message}`)
    const msg = o?.status?.message ?? ''
    if (msg && msg !== last) { opts.out.line(`  ${msg}`); last = msg }
    if (Date.now() > until) throw new Refused(`${name} was not ${opts.what} in ${opts.timeoutS} s${msg ? `: ${msg}` : ''}`)
    await new Promise((res) => setTimeout(res, opts.everyMs))
  }
}

const describe = (o: any): string => {
  const s = o.status ?? {}
  return [
    `${o.metadata.namespace}/${o.metadata.name}: ${s.phase ?? 'Creating'}${s.awake ? ', awake' : ''}${s.pgVersion ? `, Postgres ${s.pgVersion}` : ''}`,
    ...(s.host ? [`  connect to ${s.host}:${s.port ?? 5432}`] : []),
    ...(s.serviceHost ? [`  inside the cluster: ${s.serviceHost}:${s.port ?? 5432}`] : []),
    ...(s.loginSecret ? [`  login: Secret ${s.loginSecret}; authority's certificate: ConfigMap ${s.caConfigMap ?? 'peat-ca'} (sslmode=verify-full)`] : []),
    ...(o.spec?.parent ? [`  made from ${o.spec.parent}${o.spec.at ? ` at ${o.spec.at}` : ''}`] : []),
    ...(o.spec?.replicaOf ? [`  a replica of ${o.spec.replicaOf}`] : []),
    ...(s.message ? [`  ${s.message}`] : []),
  ].join('\n')
}

/** Runs one command. Returns the exit code: 0 done, 1 refused or failed, 2 a wrong command line. */
export async function run(argv: string[], deps: { kube?: Kube; out?: Out; everyMs?: number; now?: () => number } = {}): Promise<number> {
  const out = deps.out ?? { line: (s) => console.log(s) }
  let parsed
  try { parsed = parseArgs(argv) } catch (e) { out.line(String((e as Error).message)); return 2 }
  const { words, opts } = parsed
  const [verb, a, b] = words
  const usage = () => { out.line('usage: peat <list|show|make|branch|restore|replica|stop|close|open|move|delete|url|ca|status|retry> ... (peat help)'); return 2 }
  if (!verb || verb === 'help' || opts['help']) return usage()
  const k = deps.kube ?? fromKubeconfig({ path: typeof opts['kubeconfig'] === 'string' ? opts['kubeconfig'] : undefined, context: typeof opts['context'] === 'string' ? opts['context'] : undefined, namespace: typeof opts['namespace'] === 'string' ? opts['namespace'] : undefined })
  const ns = (typeof opts['namespace'] === 'string' ? opts['namespace'] : undefined)
  const here = ns ?? k.namespace
  const wait = !opts['no-wait']
  const w = { timeoutS: Number(opts['timeout'] ?? 180), everyMs: deps.everyMs ?? 1000, out }
  const patch = async (o: any, body: unknown) => ok(await k.request('PATCH', dbPath(o.metadata.namespace, o.metadata.name), body, 'application/merge-patch+json'), `change ${o.metadata.name}`).json
  const make = async (name: string, spec: Record<string, unknown>, inNs: string, what: string) => {
    const r = await k.request('POST', dbPath(inNs), { apiVersion: API_VERSION, kind: 'Database', metadata: { name, namespace: inNs }, spec })
    if (r.status === 409) throw new Refused(`${name} exists in ${inNs}`)
    ok(r, `make ${name}`)
    out.line(`${inNs}/${name}: ${what}`)
    if (!wait) return 0
    out.line(describe(await waitFor(k, inNs, name, (o) => Boolean(o.status?.tenant) && o.status?.phase === 'Ready' && Boolean(o.status?.loginSecret), { ...w, what: 'made, with its login published' })))
    return 0
  }
  try {
    switch (verb) {
      case 'list': {
        const r = ok(await k.request('GET', opts['A'] || opts['all-namespaces'] ? `/apis/${API_VERSION}/databases` : dbPath(here)), 'list the databases')
        const items = r.json.items as any[]
        if (!items.length) { out.line('no databases'); return 0 }
        for (const o of items) out.line(`${o.metadata.namespace}/${o.metadata.name}  ${o.status?.phase ?? 'Creating'}  ${o.status?.pgVersion ? `pg${o.status.pgVersion}` : ''}  ${o.status?.awake ? 'awake' : 'asleep'}  ${o.status?.host ?? o.status?.serviceHost ?? ''}`.replace(/\s+$/, ''))
        return 0
      }
      case 'show': out.line(describe(await findDb(k, checkName(a), ns))); return 0
      case 'make': return await make(checkName(a), opts['pg'] !== undefined ? { pgVersion: parsePg(String(opts['pg'])) } : {}, here, 'making a new database')
      case 'branch': case 'replica': {
        const parent = await findDb(k, checkName(a, 'the parent'), ns)
        return await make(checkName(b), verb === 'branch' ? { parent: a } : { replicaOf: a }, parent.metadata.namespace, verb === 'branch' ? `making a branch of ${a}` : `making a replica of ${a}`)
      }
      case 'restore': {
        const parent = await findDb(k, checkName(a, 'the parent'), ns)
        const at = parseAt(typeof opts['at'] === 'string' ? opts['at'] : undefined, (deps.now ?? Date.now)())
        return await make(checkName(b), { parent: a, at }, parent.metadata.namespace, `making ${a} as it was at ${at}, as a new database`)
      }
      case 'stop': {
        const o = await findDb(k, checkName(a), ns)
        const request = new Date((deps.now ?? Date.now)()).toISOString()
        await patch(o, { spec: { requests: { stop: request } } })
        if (wait) await waitFor(k, o.metadata.namespace, o.metadata.name, (x) => x.status?.answered?.stop === request, { ...w, what: 'asleep' })
        out.line(`${o.metadata.namespace}/${o.metadata.name}: asleep; the next connection wakes it`)
        return 0
      }
      case 'close': case 'open': {
        const o = await findDb(k, checkName(a), ns)
        await patch(o, { spec: { closed: verb === 'close' } })
        if (wait) await waitFor(k, o.metadata.namespace, o.metadata.name, (x) => (verb === 'close' ? x.status?.phase === 'Closed' : x.status?.phase === 'Ready'), { ...w, what: verb === 'close' ? 'closed' : 'open' })
        out.line(`${o.metadata.namespace}/${o.metadata.name}: ${verb === 'close' ? 'closed: every connection is refused until it is opened' : 'open'}`)
        return 0
      }
      case 'move': {
        const o = await findDb(k, checkName(a), ns)
        const pg = parsePg(typeof opts['pg'] === 'string' ? opts['pg'] : undefined)
        if (opts['rehearse']) {
          const request = `${pg}@${new Date((deps.now ?? Date.now)()).toISOString()}`
          await patch(o, { spec: { requests: { rehearseMove: request } } })
          out.line(`${o.metadata.namespace}/${o.metadata.name}: a rehearsal of the move to Postgres ${pg} asked (${request}); peat show ${a} reads its answer`)
          return 0
        }
        await patch(o, { spec: { pgVersion: pg } })
        out.line(`${o.metadata.namespace}/${o.metadata.name}: the move to Postgres ${pg} asked; peat show ${a} follows it`)
        return 0
      }
      case 'delete': {
        const o = await findDb(k, checkName(a), ns)
        if (!opts['yes']) { out.line(`deleting ${o.metadata.namespace}/${a} removes its compute now and its data after the platform's hold: add --yes`); return 1 }
        ok(await k.request('DELETE', dbPath(o.metadata.namespace, a!)), `delete ${a}`, [404])
        out.line(`${o.metadata.namespace}/${a}: deleted (its compute goes now, its data after the platform's hold)`)
        return 0
      }
      case 'url': {
        const o = await findDb(k, checkName(a), ns)
        const secret = o.status?.loginSecret
        if (!secret) throw new Refused(`${a} has no login yet: ${o.status?.message ?? 'it is being made'}`)
        const s = ok(await k.request('GET', `/api/v1/namespaces/${o.metadata.namespace}/secrets/${secret}`), `read ${secret}`).json
        out.line(Buffer.from(String(s.data?.url ?? ''), 'base64').toString())
        return 0
      }
      case 'ca': {
        const r = await k.request('GET', `/api/v1/namespaces/${here}/configmaps/peat-ca`)
        if (r.status === 404) throw new Refused(`no authority's certificate in ${here}: it is published in each namespace that holds a database`)
        out.line(String(ok(r, 'read peat-ca').json.data?.['ca.crt'] ?? '').trimEnd())
        return 0
      }
      case 'status': {
        const items = ok(await k.request('GET', `/apis/${API_VERSION}/platforms`), 'read the platform').json.items as any[]
        if (!items.length) { out.line('no Platform in this cluster'); return 1 }
        for (const p of items) {
          const s = p.status ?? {}
          out.line(`${p.metadata.name}: ${s.phase ?? 'not read yet'}, release ${p.spec?.release}${s.release && s.release !== p.spec?.release ? ` (running ${s.release})` : ''}; survives ${s.survives ?? '?'}`)
          if (s.message) out.line(`  ${s.message}`)
          for (const x of (s.pieces ?? []) as any[]) out.line(`  ${x.kind} ${x.namespace}/${x.name}: ${x.ready ? 'ready' : 'not ready'}, ${x.release}${x.message ? ` (${x.message})` : ''}`)
          if (s.upgrade?.halted) out.line(`  halted at ${s.upgrade.halted.step}: ${s.upgrade.halted.reason} (peat retry goes on; setting the earlier release back takes back what moved)`)
        }
        return 0
      }
      case 'retry': {
        const items = ok(await k.request('GET', `/apis/${API_VERSION}/platforms`), 'read the platform').json.items as any[]
        if (!items.length) { out.line('no Platform in this cluster'); return 1 }
        const request = new Date((deps.now ?? Date.now)()).toISOString()
        ok(await k.request('PATCH', `/apis/${API_VERSION}/platforms/${items[0].metadata.name}`, { spec: { requests: { retry: request } } }, 'application/merge-patch+json'), 'ask for a retry')
        out.line(`${items[0].metadata.name}: a retry asked (${request})`)
        return 0
      }
      default: return usage()
    }
  } catch (e) {
    out.line(e instanceof Refused ? `peat: ${e.message}` : `peat: ${e}`)
    return 1
  }
}

if (import.meta.main) process.exit(await run(process.argv.slice(2)))
