// The gateway program's `Api` (the few calls it makes of the cluster) over the platform's control layer: the program reaches its databases only
// through that interface, and this file is that interface for Peat. A "Cluster" is a database record of the control layer (`GET /clusters`,
// whose rows carry the annotations and status the program reads); `wake` is the control layer's start of a compute and `hibernate` its stop
// (a database with no compute pod is asleep). The routes and the compute pods come from the control layer too, so a gateway pod holds no
// Kubernetes credential. Light sleep (CloudNativePG's fence: Postgres stopped, the pod kept) has no counterpart on Neon's storage and is not
// built: `fence` and `unfence` are never reached, because no row carries the full-sleep annotation, so the program goes straight from awake
// to full sleep at the idle time.
import type { Api, BackupRow, ClusterRow, PodRow, ScheduleRow, ServiceRow } from './gateway'

const jsonOf = async <T>(r: Response, what: string): Promise<T> => {
  if (!r.ok) throw new Error(`${what}: ${r.status} ${await r.text()}`)
  return (await r.json()) as T
}

/** `allowSleep` (two gateways decide sleep one at a time): null when this gateway may put the database to sleep now, else why not. `control` is the control layer's URL. */
export function neonApi(allowSleep: (name: string) => Promise<string | null>, control: string): Api {
  const call = async (method: string, path: string): Promise<Response> => fetch(`${control}${path}`, { method, signal: AbortSignal.timeout(10_000) })
  const clusters = async (): Promise<ClusterRow[]> => jsonOf<ClusterRow[]>(await call('GET', '/clusters'), 'list databases')
  return {
    listServices: async () => jsonOf<ServiceRow[]>(await call('GET', '/services'), 'list routes'),
    // one database's row from the control layer, not the whole list: with a thousand databases the list took long enough under load that a replaced compute's callers waited 50 s
    getCluster: async (_namespace, name) => {
      const r = await call('GET', `/cluster/${encodeURIComponent(name)}`)
      if (r.status === 404) return null
      return jsonOf<ClusterRow>(r, 'read database')
    },
    listClusters: async () => clusters(),
    getPod: async (_namespace, name) => {
      const r = await call('GET', `/pod/${encodeURIComponent(name)}`)
      if (r.status === 404) return null
      return jsonOf<PodRow>(r, 'read compute pod')
    },
    listBackups: async (): Promise<BackupRow[]> => [],
    listSchedules: async (): Promise<ScheduleRow[]> => [],
    suspendSchedule: async () => {},
    resumeSchedule: async () => {},
    // full sleep is the only stage: the compute pod is made again
    wake: async (_namespace, name) => {
      const r = await call('POST', `/db/${encodeURIComponent(name)}/start`)
      if (!r.ok) throw new Error(`start ${name}: ${r.status} ${await r.text()}`)
    },
    // the decision was made on a reading whose resourceVersion names the compute pod it saw: refused when the database has changed since (a compute started, or gone), as the API server refuses a stale patch
    hibernate: async (_namespace, name, resourceVersion) => {
      // two gateways: only the one that holds the sleep lease decides, and not while the other has had a session on this database within the idle time (a refusal is a 409: counted and decided again at the next look)
      const why = await allowSleep(name)
      if (why) throw new Error(`409 ${name}: ${why}`)
      const rr = await call('GET', `/cluster/${encodeURIComponent(name)}`)
      const now = rr.ok ? ((await rr.json()) as ClusterRow) : undefined
      if (now?.metadata?.resourceVersion !== resourceVersion) throw new Error(`409 ${name} changed since it was read`)
      const r = await call('POST', `/db/${encodeURIComponent(name)}/stop`)
      if (!r.ok) throw new Error(`stop ${name}: ${r.status} ${await r.text()}`)
    },
    fence: async () => { throw new Error('light sleep is not built on the platform') },
    unfence: async () => {},
  }
}
