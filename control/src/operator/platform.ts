// A Platform object made ready to act on: its defaults filled in, checked, its images named, its names (src/names.ts), and the machine of
// each pinned piece chosen. Pure: the operator reads the cluster (the labelled nodes, its own record of placements) and passes what it read.
import { check, withDefaults } from '../api/schema'
import { platformSpec, type Platform, type PlatformSpec, type Store } from '../api/v1alpha1'
import { DEFAULT_NAMES, type Names, type Taint } from '../names'

/** A release and its images: what each piece is stamped with and runs. */
export interface Images { release: string; storage: string; control: string; compute: { 18: string; 19: string } }

/** The images of a release: the registry's own, unless the Platform names one. */
export function imagesOf(spec: PlatformSpec, release: string = spec.release): Images {
  const r = spec.images.registry.replace(/\/$/, '')
  const own = release === spec.release
  return {
    release,
    storage: (own && spec.images.storage) || `${r}/storage:${release}`,
    control: (own && spec.images.control) || `${r}/control:${release}`,
    compute: { 18: (own && spec.images.compute?.['18']) || `${r}/compute-pg18:${release}`, 19: (own && spec.images.compute?.['19']) || `${r}/compute-pg19:${release}` },
  }
}

/** Where a piece keeps data: the bucket, the Secret with its key, the folder. */
export interface StoreRef { bucket: string; credentialsSecret: string; prefix: string }

/** The machine each pinned piece runs on, kept by the operator once chosen (a pageserver's and a safekeeper's data is on its machine). */
export interface Placements {
  pageservers: { id: number; node: string }[]
  safekeepers: { id: number; node: string }[]
  controller: string
}

export interface Resolved {
  name: string
  spec: PlatformSpec
  names: Names
  images: Images
  store: { endpoint: string; region: string; pageservers: StoreRef; safekeepers: StoreRef; records: StoreRef; walTrim: string }
  placements: Placements
  /** What the platform survives, for the status. */
  survives: string
}

const toTaint = (t: { key: string; value?: string; effect: string } | undefined): Taint | null => (t ? { key: t.key, value: t.value ?? '', effect: t.effect as Taint['effect'] } : null)

/** The spec with its defaults, or every way it is wrong. */
export function specOf(p: Platform): { spec: PlatformSpec } | { errors: string[] } {
  const errors = check(platformSpec, p.spec)
  if (errors.length) return { errors }
  const spec = withDefaults(platformSpec, p.spec) as PlatformSpec
  // the pageservers' count is the operator's to default: two, or one with spread none, which allows no more (two pageservers on one machine
  // would share its directory, and the second's start refuses the first's identity)
  if (spec.pageservers.count === undefined) spec.pageservers.count = spec.spread === 'none' ? 1 : 2
  if (spec.spread === 'none' && spec.pageservers.count > 1) return { errors: [`pageservers.count: spread none runs one pageserver, not ${spec.pageservers.count} (two would share one machine and its directory)`] }
  const store = spec.objectStore
  const missing: string[] = []
  for (const piece of ['pageservers', 'safekeepers', 'records'] as const) if (!store[piece] && !(store.bucket && store.credentialsSecret)) missing.push(piece)
  if (missing.length) errors.push(`objectStore: no bucket and credentialsSecret for ${missing.join(', ')} (give the one bucket and its Secret, or one for each piece)`)
  if (spec.controller.records === 'postgres' && !spec.controller.postgres) errors.push('controller.postgres: is required when records is postgres')
  for (const [field, ns] of Object.entries(spec.namespaces)) if (ns === 'default' || ns.startsWith('kube-')) errors.push(`namespaces.${field}: ${ns} is the cluster's own; the platform needs namespaces of its own`)
  if (spec.safekeepers.machines && spec.safekeepers.machines.length !== spec.safekeepers.count) errors.push(`safekeepers.machines: names ${spec.safekeepers.machines.length} machines for ${spec.safekeepers.count} safekeepers`)
  if (spec.pageservers.machines && spec.pageservers.machines.length !== spec.pageservers.count) errors.push(`pageservers.machines: names ${spec.pageservers.machines.length} machines for ${spec.pageservers.count} pageservers`)
  if (spec.pageservers.storage.storageClass || spec.safekeepers.storage.storageClass) errors.push('storage.storageClass: not supported yet; use hostPath')
  return errors.length ? { errors } : { spec }
}

export function namesOf(spec: PlatformSpec): Names {
  return {
    namespace: spec.namespaces.platform,
    computeNamespace: spec.namespaces.computes,
    gatewayNamespace: spec.namespaces.gateways,
    dbNamespace: spec.namespaces.databases,
    machineLabel: spec.machines.label,
    machineTaint: toTaint(spec.machines.taint),
    deadTaint: toTaint(spec.machines.deadTaint) ?? DEFAULT_NAMES.deadTaint,
  }
}

const storeRef = (spec: PlatformSpec, own: Store | undefined, prefix: string): StoreRef =>
  own ? { bucket: own.bucket, credentialsSecret: own.credentialsSecret, prefix: own.prefix ?? prefix } : { bucket: spec.objectStore.bucket!, credentialsSecret: spec.objectStore.credentialsSecret!, prefix }

/**
 * The machine of each pinned piece. What was chosen before is kept (the data is there); a machine the Platform names is used; the rest are
 * chosen from the labelled machines in name order. With spread `machines`: the pageservers on distinct machines, the controller on a machine
 * with no pageserver, the safekeepers' first members on distinct machines; a placement that breaks a rule is refused with the rule. With
 * spread `none`: every piece on the first machine.
 */
export function choosePlacements(spec: PlatformSpec, machines: string[], kept: Partial<Placements> = {}, zones: Record<string, string> = {}): { placements: Placements } | { errors: string[] } {
  const errors: string[] = []
  // a machine's zone: the value of safekeepers.zoneLabel on it (its own name with the default kubernetes.io/hostname)
  const zoneOf = (m: string) => zones[m] ?? m
  const sorted = [...machines].sort()
  if (sorted.length === 0) return { errors: [`no machine carries the label ${spec.machines.label.key}=${spec.machines.label.value}: label the machines the platform may use`] }
  // what was chosen is kept (the data is on that machine); a count raised adds members on machines not yet used; a count lowered is refused
  const pick = (what: string, count: number, named: string[] | undefined, keptList: { id: number; node: string }[] | undefined, avoid: string[] = []): { id: number; node: string }[] => {
    if (keptList?.length && keptList.length > count) { errors.push(`${what}.count: ${count} is fewer than the ${keptList.length} the platform has; a ${what.replace(/s$/, '')} holds data, so one is removed by hand (drained first), never by lowering the count`); return keptList }
    if (named) return named.map((node, i) => ({ id: keptList?.[i]?.id ?? i + 1, node: keptList?.[i]?.node ?? node }))
    const out = [...(keptList ?? [])]
    const used = new Set(out.map((x) => x.node))
    const free = spec.spread === 'none' ? [sorted[0]!] : [...sorted.filter((m) => !avoid.includes(m) && !used.has(m)), ...sorted.filter((m) => avoid.includes(m) && !used.has(m))]
    for (let i = out.length, f = 0; i < count; i++) out.push({ id: Math.max(0, ...out.map((x) => x.id)) + 1, node: spec.spread === 'none' ? sorted[0]! : (free[f++] ?? '') })
    return out
  }
  const pageservers = pick('pageservers', spec.pageservers.count!, spec.pageservers.machines, kept.pageservers)
  const psNodes = pageservers.map((p) => p.node)
  const controller = kept.controller ?? spec.controller.machine ?? (spec.spread === 'none' ? sorted[0]! : (sorted.find((m) => !psNodes.includes(m)) ?? ''))
  // the safekeepers' first members on machines of distinct zones (the controller picks each timeline's members from distinct zones, so a set
  // needs as many zones as members): one machine of each zone first, in name order
  const byZone = [...new Map(sorted.map((m) => [zoneOf(m), m])).values()]
  const skOrder = spec.spread === 'none' ? sorted : [...byZone, ...sorted.filter((m) => !byZone.includes(m))]
  const safekeepers = pick('safekeepers', spec.safekeepers.count, spec.safekeepers.machines, kept.safekeepers, sorted.filter((m) => !skOrder.slice(0, spec.safekeepers.count).includes(m)))
  if (spec.spread === 'machines') {
    if (pageservers.some((p) => !p.node)) errors.push(`spread machines: ${spec.pageservers.count} pageservers need ${spec.pageservers.count} machines, and ${sorted.length} carry the label`)
    if (new Set(psNodes).size !== psNodes.length) errors.push('spread machines: two pageservers on one machine')
    if (!controller) errors.push('spread machines: the controller needs a machine with no pageserver, and every labelled machine has one')
    else if (psNodes.includes(controller)) errors.push(`spread machines: the controller may not run beside a pageserver (${controller})`)
    const sk = safekeepers.map((s) => s.node)
    if (sk.some((n) => !n)) errors.push(`spread machines: ${spec.safekeepers.count} safekeepers need ${spec.safekeepers.count} machines, and ${sorted.length} carry the label`)
    else if (new Set(sk).size !== sk.length) errors.push('spread machines: two safekeepers on one machine (a machine lost would take two of a timeline\'s members)')
    else if (new Set(sk.map(zoneOf)).size !== sk.length) errors.push(`spread machines: ${spec.safekeepers.count} safekeepers need ${spec.safekeepers.count} zones of ${spec.safekeepers.zoneLabel}, and their machines are in ${new Set(sk.map(zoneOf)).size} (the controller picks a timeline's members from distinct zones)`)
  }
  for (const n of [...psNodes, controller, ...safekeepers.map((s) => s.node)]) if (n && !machines.includes(n) && !(spec.safekeepers.outside?.machines ?? []).includes(n)) errors.push(`${n} does not carry the label ${spec.machines.label.key}=${spec.machines.label.value}`)
  return errors.length ? { errors: [...new Set(errors)] } : { placements: { pageservers, safekeepers, controller } }
}

/** The Platform resolved: everything the renderer needs. `release` is the release the pieces are rendered at (an upgrade renders each piece at its own). */
export function resolve(p: Platform, spec: PlatformSpec, placements: Placements): Resolved {
  return {
    name: p.metadata.name,
    spec,
    names: namesOf(spec),
    images: imagesOf(spec),
    store: {
      endpoint: spec.objectStore.endpoint,
      region: spec.objectStore.region,
      pageservers: storeRef(spec, spec.objectStore.pageservers, 'pageserver'),
      safekeepers: storeRef(spec, spec.objectStore.safekeepers, 'safekeeper'),
      records: storeRef(spec, spec.objectStore.records, 'storage-controller'),
      walTrim: spec.objectStore.walTrim?.credentialsSecret ?? storeRef(spec, spec.objectStore.safekeepers, 'safekeeper').credentialsSecret,
    },
    placements,
    survives: spec.spread === 'none' ? "no machine's loss: every piece is on one machine (spread none)" : "one machine's loss",
  }
}
