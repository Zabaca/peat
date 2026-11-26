// The operator's record of what it made: for every object, its kind, namespace and name with the UID the API server gave it. An object is
// the operator's only when the record holds its UID (a label proves nothing: anyone can copy one), so an object of a piece's name that the
// operator did not make is never applied over, and nothing it did not make is deleted.
//
// It is kept in a ConfigMap of the operator's own namespace, where nothing but the operator writes (a record anyone could add to would let
// them have the operator delete what they named), one key per object, written as each object is made: a pass that fails half way loses
// nothing it made. Before an object is created its intent is recorded with a nonce, and the nonce is stamped on the object; a pass that dies
// between the create and the record finds the object by its nonce and records its UID. A record that cannot be read (any answer but a
// 404, or a value that does not parse) stops the pass: it is never taken as empty, so it is never overwritten by a partial one.
import { randomBytes } from 'node:crypto'
import { GROUP } from '../names'
import { ok, type Kube } from '../kube'

export const OWNED_CONFIG_MAP = 'peat-owned'
/** On an object the operator is creating: the nonce its record's intent holds. */
export const NONCE_ANNOTATION = `${GROUP}/owner-nonce`

/** `Kind/namespace/name` as a ConfigMap key (which allows letters, digits, `-`, `_` and `.`): `Kind.namespace.name`. */
export const cmKey = (key: string) => key.replace(/\//g, '.')
const fromCmKey = (k: string) => { const [kind, ns, ...name] = k.split('.'); return `${kind}/${ns}/${name.join('.')}` }

export type Entry = { uid: string } | { nonce: string }

export class Owned {
  private readonly entries = new Map<string, Entry>()
  private constructor(private readonly k: Kube, private readonly ns: string) {}

  /** The record as kept: made empty (and kept) only when there is none at all. */
  static async load(k: Kube, ns: string): Promise<Owned> {
    const o = new Owned(k, ns)
    const path = `/api/v1/namespaces/${ns}/configmaps/${OWNED_CONFIG_MAP}`
    const r = await k.request('GET', path)
    if (r.status === 404) {
      ok(await k.request('POST', `/api/v1/namespaces/${ns}/configmaps`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: OWNED_CONFIG_MAP, namespace: ns }, data: {} }), 'make the record of what the operator made', [409])
      return o
    }
    ok(r, 'read the record of what the operator made')
    for (const [k2, v] of Object.entries((r.json?.data ?? {}) as Record<string, string>)) {
      const m = /^(uid|nonce):([A-Za-z0-9-]+)$/.exec(String(v))
      if (!m) throw new Error(`the record of what the operator made has an entry it cannot read (${k2}): nothing is changed until it is fixed`)
      o.entries.set(fromCmKey(k2), m[1] === 'uid' ? { uid: m[2]! } : { nonce: m[2]! })
    }
    return o
  }

  private async write(key: string, value: string | null) {
    ok(await this.k.request('PATCH', `/api/v1/namespaces/${this.ns}/configmaps/${OWNED_CONFIG_MAP}`, { data: { [cmKey(key)]: value } }, 'application/merge-patch+json'), `record ${key}`)
  }
  get(key: string): Entry | undefined { return this.entries.get(key) }
  keys(): string[] { return [...this.entries.keys()] }

  /** Whether a live object is the one the operator made: its UID recorded, or its nonce the record's intent (then its UID is recorded now). */
  async ours(key: string, live: any): Promise<boolean> {
    const e = this.entries.get(key)
    const uid = String(live?.metadata?.uid ?? '')
    if (!e || !uid) return false
    if ('uid' in e) return e.uid === uid
    if (live?.metadata?.annotations?.[NONCE_ANNOTATION] !== e.nonce) return false
    await this.made(key, uid)
    return true
  }
  /** Records the intent to create an object: returns the nonce to stamp on it. */
  async intend(key: string): Promise<string> {
    const nonce = randomBytes(9).toString('hex')
    await this.write(key, `nonce:${nonce}`)
    this.entries.set(key, { nonce })
    return nonce
  }
  async made(key: string, uid: string): Promise<void> {
    await this.write(key, `uid:${uid}`)
    this.entries.set(key, { uid })
  }
  async forget(key: string): Promise<void> {
    await this.write(key, null)
    this.entries.delete(key)
  }
}
