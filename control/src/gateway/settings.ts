// What a platform gateway pod reads from its environment (the operator sets it; every value unset is the default below): the names the
// gateway program shares with the control layer, the control layer's address, its own ports and the idle time. Read once at start by
// main.ts; kept apart from it so that a test can read the settings without starting a gateway.
import { GROUP, controlUrl, fromEnv } from '../names'
import type { Names } from './gateway'

/**
 * The annotations of the rows the control layer serves (`GET /services`, `GET /clusters`): its own under the platform's group, and the
 * third-party keys the gateway program reads from a row because it was written for CloudNativePG Clusters (the control layer writes its
 * rows in that shape, so the program runs on them as it is).
 */
export const ANNOTATIONS = {
  /** On a route row: `<namespace>/<database>`, the database the route leads to. The control layer writes the same key. */
  route: `${GROUP}/route`,
  /** The two sleep annotations a row carries: "declared to sleep" ("true") and the idle minutes, under the prefix of the plugin that first defined them. */
  sleepEnabled: 'xata.io/scale-to-zero-enabled',
  sleepIdleMinutes: 'xata.io/scale-to-zero-inactivity-minutes',
  /** The full-sleep time in whole minutes, or `never`. A platform row never carries it: a database goes from awake to full sleep at the idle time. */
  sleepFullMinutes: `${GROUP}/full-sleep-minutes`,
  /** CloudNativePG's sleep mark: "on" is asleep. The control layer writes it from whether the database's compute exists. */
  hibernation: 'cnpg.io/hibernation',
  /** CloudNativePG's fence and its value for every instance: light sleep, which has no counterpart on the platform and is never reached. */
  fencedInstances: 'cnpg.io/fencedInstances',
  /** The gateway's own mark on a row it fenced (never written on the platform: no light sleep). */
  idleSince: `${GROUP}/idle-since`,
  /** On a backup schedule the gateway suspended (the platform's rows have none). */
  suspendedBy: `${GROUP}/suspended-by`,
} as const

export interface GatewaySettings {
  names: Names
  /** The control layer's namespace: the first half of every route's database (`<namespace>/<database>`). */
  controlNamespace: string
  controlUrl: string
  pod: string
  idleMs: number
  frontPort: number
  metricsPort: number
}

const port = (env: Record<string, string | undefined>, name: string, fallback: number): number => {
  const v = env[name] === undefined || env[name] === '' ? fallback : Number(env[name])
  if (!Number.isInteger(v) || v < 1 || v > 65535) throw new Error(`${name}: ${JSON.stringify(env[name])} is not a port`)
  return v
}

/** The settings of a gateway pod. A value that is set and wrong is refused, never guessed at. */
export function gatewaySettings(env: Record<string, string | undefined> = process.env): GatewaySettings {
  const n = fromEnv(env)
  const idle = env['IDLE_MINUTES'] === undefined || env['IDLE_MINUTES'] === '' ? 30 : Number(env['IDLE_MINUTES'])
  if (!(idle > 0)) throw new Error(`IDLE_MINUTES: ${JSON.stringify(env['IDLE_MINUTES'])} is not a number of minutes above 0`)
  return {
    names: {
      namespace: n.gatewayNamespace,
      routeAnnotation: env['ROUTE_ANNOTATION'] || ANNOTATIONS.route,
      sleepEnabledAnnotation: ANNOTATIONS.sleepEnabled,
      sleepIdleMinutesAnnotation: ANNOTATIONS.sleepIdleMinutes,
      sleepFullMinutesAnnotation: ANNOTATIONS.sleepFullMinutes,
      sleepNever: 'never',
      hibernationAnnotation: ANNOTATIONS.hibernation,
      fencedInstancesAnnotation: ANNOTATIONS.fencedInstances,
      fenceAll: '["*"]',
      idleSinceAnnotation: ANNOTATIONS.idleSince,
      suspendedByAnnotation: ANNOTATIONS.suspendedBy,
      // the control layer gives each database a port of its own in this range (inside the pod, behind the front door)
      portRanges: [{ from: 15000, to: 19999 }],
      // no platform database has backup schedules for the gateway to keep: an empty range, so no route is a permanent database's
      permanentPorts: { from: 0, to: -1 },
    },
    controlNamespace: n.namespace,
    controlUrl: env['CONTROL_URL'] || controlUrl(n),
    pod: env['POD_NAME'] || 'gateway',
    idleMs: idle * 60_000,
    frontPort: port(env, 'FRONT_PORT', 5432),
    metricsPort: port(env, 'METRICS_PORT', 9090),
  }
}
