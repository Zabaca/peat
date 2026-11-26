// The names every piece of the platform shares: its namespaces, the label that marks its machines, the taint the control layer puts on a
// machine it has declared dead, the API group of its object kinds and the annotations its programs read. One meaning, one place.
//
// Each has a neutral default. An install changes them through its Platform object (the operator passes them to the pieces it runs as
// environment values, read here by `fromEnv`), so a cluster that already uses these namespaces or labels for something else can choose others.
// The pieces' own object names (`storage-controller`, `pageserver-w<id>`, `safekeeper-w<id>`, `peat-control`, `peat-gateway`, `pg-front`)
// are fixed: the control layer, the gateways and the storage find each other by them.

/** The API group of Peat's object kinds (Platform, Database) and the prefix of its labels, annotations and taints. */
export const GROUP = 'peat.zabaca.com'
export const VERSION = 'v1alpha1'

export interface Label { key: string; value: string }
export interface Taint { key: string; value: string; effect: 'NoSchedule' | 'PreferNoSchedule' | 'NoExecute' }
export interface Toleration { key?: string; operator?: 'Equal' | 'Exists'; value?: string; effect?: string; tolerationSeconds?: number }

export interface Names {
  /** The storage, the control layer and its records, the leases. */
  namespace: string
  /** The databases' computes, their specs and certificates. */
  computeNamespace: string
  /** The gateways and the one front door. */
  gatewayNamespace: string
  /** Each database's name inside the cluster: an ExternalName Service that leads to the front door. */
  dbNamespace: string
  /** The label every platform machine carries: the control layer probes, taints and places pods on these machines and no other. */
  machineLabel: Label
  /** The taint the platform's machines carry, which its pods tolerate (none: the machines are shared with other work). */
  machineTaint: Taint | null
  /** The taint the control layer puts on a platform machine it has declared dead, and takes off when it answers again. */
  deadTaint: Taint
}

export const DEFAULT_NAMES: Names = {
  namespace: 'peat',
  computeNamespace: 'peat-compute',
  gatewayNamespace: 'peat-gateway',
  dbNamespace: 'peat-db',
  machineLabel: { key: `${GROUP}/machine`, value: 'true' },
  machineTaint: null,
  deadTaint: { key: `${GROUP}/dead`, value: 'true', effect: 'NoSchedule' },
}

/** `key=value` as a label; a missing `=` is refused, never guessed. */
export function parseLabel(text: string): Label {
  const at = text.indexOf('=')
  if (at <= 0) throw new Error(`a label is key=value, not ${JSON.stringify(text)}`)
  return { key: text.slice(0, at), value: text.slice(at + 1) }
}

/** `key=value:Effect` as a taint; an empty string is none. */
export function parseTaint(text: string): Taint | null {
  if (text === '') return null
  const m = /^([^=:]+)=([^:]*):(NoSchedule|PreferNoSchedule|NoExecute)$/.exec(text)
  if (!m) throw new Error(`a taint is key=value:Effect, not ${JSON.stringify(text)}`)
  return { key: m[1]!, value: m[2]!, effect: m[3] as Taint['effect'] }
}

export const formatLabel = (l: Label): string => `${l.key}=${l.value}`
export const formatTaint = (t: Taint | null): string => (t ? `${t.key}=${t.value}:${t.effect}` : '')

/** The names as a piece's environment gives them (the operator sets these; each one unset is the default). */
export function fromEnv(env: Record<string, string | undefined> = process.env): Names {
  return {
    namespace: env['NAMESPACE'] || DEFAULT_NAMES.namespace,
    computeNamespace: env['COMPUTE_NAMESPACE'] || DEFAULT_NAMES.computeNamespace,
    gatewayNamespace: env['GATEWAY_NAMESPACE'] || DEFAULT_NAMES.gatewayNamespace,
    dbNamespace: env['DB_NAMESPACE'] || DEFAULT_NAMES.dbNamespace,
    machineLabel: env['MACHINE_LABEL'] ? parseLabel(env['MACHINE_LABEL']) : DEFAULT_NAMES.machineLabel,
    machineTaint: env['MACHINE_TAINT'] !== undefined ? parseTaint(env['MACHINE_TAINT']) : DEFAULT_NAMES.machineTaint,
    deadTaint: env['DEAD_TAINT'] ? (parseTaint(env['DEAD_TAINT']) ?? DEFAULT_NAMES.deadTaint) : DEFAULT_NAMES.deadTaint,
  }
}

/** The environment that gives a piece these names: what the operator puts on every piece it runs. */
export function toEnv(n: Names): { name: string; value: string }[] {
  return [
    { name: 'NAMESPACE', value: n.namespace },
    { name: 'COMPUTE_NAMESPACE', value: n.computeNamespace },
    { name: 'GATEWAY_NAMESPACE', value: n.gatewayNamespace },
    { name: 'DB_NAMESPACE', value: n.dbNamespace },
    { name: 'MACHINE_LABEL', value: formatLabel(n.machineLabel) },
    { name: 'MACHINE_TAINT', value: formatTaint(n.machineTaint) },
    { name: 'DEAD_TAINT', value: formatTaint(n.deadTaint) },
  ]
}

/** What every pod placed on the platform's machines carries: the label selected and, when the machines are tainted, the taint tolerated. */
export function placement(n: Names): { nodeSelector: Record<string, string>; tolerations: Toleration[] } {
  return {
    nodeSelector: { [n.machineLabel.key]: n.machineLabel.value },
    tolerations: n.machineTaint ? [{ key: n.machineTaint.key, operator: 'Equal', value: n.machineTaint.value, effect: n.machineTaint.effect }] : [],
  }
}

/** The control layer's Service, which the storage controller's hook, the safekeepers, the gateways and the computes call. */
export const controlUrl = (n: Names): string => `http://peat-control.${n.namespace}.svc:8080`
/** The one front door: a Service in the gateways' namespace that leads to every gateway pod. */
export const frontDoor = (n: Names): string => `pg-front.${n.gatewayNamespace}.svc.cluster.local`
/** The control layer's account, as the API server names it (the admission rules judge its requests). */
export const controlUser = (n: Names): string => `system:serviceaccount:${n.namespace}:peat-control`
