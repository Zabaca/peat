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
//   peat move <db> --pg <version> [--rehearse]             not in this release: refused (the release that moves a database to a newer version turns it on)
//   peat delete <db> --yes                                 delete it: its compute at once, its data after the platform's hold
//   peat url <db>                                          its connection URL (from its login Secret)
//   peat ca [<db>]                                         the authority's certificate, for sslmode=verify-full (the one <db>'s status names)
//   peat status                                            the platform: its phase, its release, every piece
//   peat retry [<platform>]                                go on with a halted upgrade from the step that halted
//
// Options: -n/--namespace (default: the kubeconfig context's), -A (a database looked up by name in every namespace: refused when more than
// one holds that name), --kubeconfig (a list joined by ':' is read as kubectl reads it), --context, --no-wait, --timeout <seconds> (180).
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

/**
 * The Database of this name: in the namespace given (-n, else the kubeconfig context's), as make and list use; with `anyNamespace` (-A)
 * the one of that name in any namespace the login may list, refused when more than one namespace holds it.
 */
async function findDb(k: Kube, name: string, ns: string, anyNamespace = false): Promise<any> {
  if (!anyNamespace) {
    const r = await k.request('GET', dbPath(ns, name))
    if (r.status === 404) throw new Refused(`no database ${name} in ${ns} (-n names another namespace; -A looks in every one)`)
    return ok(r, `read ${name}`).json
  }
  const found = (ok(await k.request('GET', `/apis/${API_VERSION}/databases`), 'list the databases').json.items as any[]).filter((d) => d.metadata.name === name)
  if (found.length === 0) throw new Refused(`no database ${name} in any namespace`)
  if (found.length > 1) throw new Refused(`${name} is in more than one namespace (${found.map((d) => d.metadata.namespace).sort().join(', ')}): name one with -n`)
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
  if (verb === 'help' || opts['help']) { for (const l of HELP) out.line(l); return 0 }
  if (!verb) return usage()
  const timeoutS = Number(opts['timeout'] ?? 180)
  if (!Number.isFinite(timeoutS) || timeoutS <= 0) { out.line(`peat: --timeout needs a number of seconds, not ${JSON.stringify(opts['timeout'])}`); return 2 }
  const k = deps.kube ?? fromKubeconfig({ path: typeof opts['kubeconfig'] === 'string' ? opts['kubeconfig'] : undefined, context: typeof opts['context'] === 'string' ? opts['context'] : undefined, namespace: typeof opts['namespace'] === 'string' ? opts['namespace'] : undefined })
  const ns = (typeof opts['namespace'] === 'string' ? opts['namespace'] : undefined)
  const here = ns ?? k.namespace
  const any = Boolean(opts['A'] || opts['all-namespaces']) && !ns
  const find = (name: string) => findDb(k, name, here, any)
  // where the command acts, named on every line that removes or shuts something, and on an error reaching the cluster
  const where = k.where ? ` (${k.where})` : ''
  const wait = !opts['no-wait']
  const w = { timeoutS, everyMs: deps.everyMs ?? 1000, out }
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
      case 'show': out.line(describe(await find(checkName(a)))); return 0
      case 'make': return await make(checkName(a), opts['pg'] !== undefined ? { pgVersion: parsePg(String(opts['pg'])) } : {}, here, 'making a new database')
      case 'branch': case 'replica': {
        const parent = await find(checkName(a, 'the parent'))
        return await make(checkName(b), verb === 'branch' ? { parent: a } : { replicaOf: a }, parent.metadata.namespace, verb === 'branch' ? `making a branch of ${a}` : `making a replica of ${a}`)
      }
      case 'restore': {
        const parent = await find(checkName(a, 'the parent'))
        const at = parseAt(typeof opts['at'] === 'string' ? opts['at'] : undefined, (deps.now ?? Date.now)())
        return await make(checkName(b), { parent: a, at }, parent.metadata.namespace, `making ${a} as it was at ${at}, as a new database`)
      }
      case 'stop': {
        const o = await find(checkName(a))
        const request = new Date((deps.now ?? Date.now)()).toISOString()
        await patch(o, { spec: { requests: { stop: request } } })
        // answered and asleep: a status written again from the record marks the requests it finds answered, whatever the compute is doing
        if (wait) await waitFor(k, o.metadata.namespace, o.metadata.name, (x) => x.status?.answered?.stop === request && x.status?.awake === false, { ...w, what: 'asleep' })
        out.line(`${o.metadata.namespace}/${o.metadata.name}: asleep; the next connection wakes it`)
        return 0
      }
      case 'close': case 'open': {
        const o = await find(checkName(a))
        await patch(o, { spec: { closed: verb === 'close' } })
        if (wait) await waitFor(k, o.metadata.namespace, o.metadata.name, (x) => (verb === 'close' ? x.status?.phase === 'Closed' : x.status?.phase === 'Ready'), { ...w, what: verb === 'close' ? 'closed' : 'open' })
        out.line(`${o.metadata.namespace}/${o.metadata.name}${where}: ${verb === 'close' ? 'closed: every connection is refused until it is opened' : 'open'}`)
        return 0
      }
      // not in this release: a version raised now would be moved, unasked, by the release that carries moves out
      case 'move': throw new Refused('the move to a newer Postgres version is not in this release: nothing was changed')
      case 'delete': {
        const o = await find(checkName(a))
        if (!opts['yes']) { out.line(`deleting ${o.metadata.namespace}/${a}${where} removes its compute now and its data after the platform's hold: add --yes`); return 1 }
        ok(await k.request('DELETE', dbPath(o.metadata.namespace, a!)), `delete ${a}`, [404])
        out.line(`${o.metadata.namespace}/${a}${where}: deleted (its compute goes now, its data after the platform's hold)`)
        return 0
      }
      case 'url': {
        const o = await find(checkName(a))
        const secret = o.status?.loginSecret
        if (!secret) throw new Refused(`${a} has no login yet: ${o.status?.message ?? 'it is being made'}`)
        const s = ok(await k.request('GET', `/api/v1/namespaces/${o.metadata.namespace}/secrets/${secret}`), `read ${secret}`).json
        out.line(Buffer.from(String(s.data?.url ?? ''), 'base64').toString())
        return 0
      }
      case 'ca': {
        // the ConfigMap a database's status names, in its namespace; with no database, the platform's name for it in this namespace
        const o = a ? await find(checkName(a)) : null
        const inNs = o?.metadata.namespace ?? here, name = o?.status?.caConfigMap ?? 'peat-ca'
        if (o && !o.status?.caConfigMap) throw new Refused(`${a}'s status names no authority's certificate: ${o.status?.message ?? 'it is being made, or the platform publishes none (an issuer named without keys.caSecret)'}`)
        const r = await k.request('GET', `/api/v1/namespaces/${inNs}/configmaps/${name}`)
        if (r.status === 404) throw new Refused(`no authority's certificate ${name} in ${inNs}: it is published in each namespace that holds a database`)
        out.line(String(ok(r, `read ${name}`).json.data?.['ca.crt'] ?? '').trimEnd())
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
        // the one Platform, or the one named: never the first of several
        const p = a ? items.find((x) => x.metadata.name === a) : items.length === 1 ? items[0] : null
        if (!p) throw new Refused(a ? `no Platform ${a}` : `more than one Platform (${items.map((x) => x.metadata.name).sort().join(', ')}): name one`)
        const request = new Date((deps.now ?? Date.now)()).toISOString()
        ok(await k.request('PATCH', `/apis/${API_VERSION}/platforms/${p.metadata.name}`, { spec: { requests: { retry: request } } }, 'application/merge-patch+json'), 'ask for a retry')
        out.line(`${p.metadata.name}${where}: a retry asked (${request})`)
        return 0
      }
      default: return usage()
    }
  } catch (e) {
    out.line(e instanceof Refused ? `peat: ${e.message}` : `peat${where}: ${e}`)
    return 1
  }
}

/** `peat help`: the commands and options, as the header of this file gives them. */
const HELP = [
  'peat: a platform\'s databases, through your own Kubernetes login',
  '  peat list [-n <namespace> | -A]', '  peat show <db>', '  peat make <db> [--pg 18|19]', '  peat branch <from> <db>', '  peat restore <from> <db> --at <time | log position>',
  '  peat replica <of> <db>', '  peat stop <db>', '  peat close <db> | peat open <db>', '  peat delete <db> --yes', '  peat url <db>', '  peat ca [<db>]', '  peat status', '  peat retry [<platform>]',
  'options: -n/--namespace (default: the kubeconfig context\'s), -A (look a database up in every namespace), --kubeconfig, --context, --no-wait, --timeout <seconds>',
]

if (import.meta.main) process.exit(await run(process.argv.slice(2)))
