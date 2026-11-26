import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { crds } from '../src/api/crds'
import { check, withDefaults, type Schema } from '../src/api/schema'
import { API_VERSION, KINDS, databaseSpec, platformSpec } from '../src/api/v1alpha1'
import { DEFAULT_NAMES, formatTaint } from '../src/names'

// The two kinds as a user writes them: the examples a user copies pass, the defaults are what the descriptions say, and a mistake is named.

const examples = (file: string) => (Bun.YAML.parse(readFileSync(join(import.meta.dir, '../examples', file), 'utf8')) as unknown) as any
const docs = (file: string): any[] => {
  const v = Bun.YAML.parse(readFileSync(join(import.meta.dir, '../examples', file), 'utf8')) as any
  return Array.isArray(v) ? v : [v]
}

describe('the examples', () => {
  test('every example is a valid object of its kind in peat.zabaca.com/v1alpha1', () => {
    for (const file of ['platform.yaml', 'trial.yaml', 'database.yaml']) {
      for (const o of docs(file)) {
        expect(o.apiVersion).toBe(API_VERSION)
        const kind = KINDS[o.kind as keyof typeof KINDS]
        expect([file, o.kind, kind ? check(kind.spec, o.spec) : ['no such kind']]).toEqual([file, o.kind, []])
      }
    }
  })
  test('the trial is spread none, one pageserver, three safekeepers by default, host directories made', () => {
    const t = withDefaults(platformSpec, examples('trial.yaml').spec) as any
    expect(t.spread).toBe('none')
    expect(t.pageservers.count).toBe(1)
    expect(t.safekeepers.count).toBe(3)
    expect(t.safekeepers.storage.hostPath).toEqual({ path: '/var/lib/peat/safekeeper', create: true })
  })
})

describe('Platform', () => {
  const minimal = { release: 'release-2026-12-01', objectStore: { endpoint: 'https://s3.example.com', bucket: 'b', credentialsSecret: 's' } }
  test('the release and the store are all an install must say; the rest has the defaults its descriptions name', () => {
    expect(check(platformSpec, minimal)).toEqual([])
    const p = withDefaults(platformSpec, minimal) as any
    expect(p.namespaces).toEqual({ platform: 'peat', computes: 'peat-compute', gateways: 'peat-gateway', databases: 'peat-db' })
    expect(p.machines.label).toEqual({ key: 'peat.zabaca.com/machine', value: 'true' })
    expect(p.spread).toBe('machines')
    expect(p.images.registry).toBe('ghcr.io/zabaca/peat')
    expect(p.pageservers.count).toBeUndefined() // the operator's: 2, or 1 with spread none
    expect(p.pageservers).toMatchObject({ memory: { request: '768Mi', limit: '1536Mi' }, storage: { hostPath: { path: '/var/lib/peat/pageserver', create: false } } })
    expect(p.safekeepers).toMatchObject({ count: 3, zoneLabel: 'kubernetes.io/hostname', memory: { request: '128Mi', limit: '512Mi' } })
    expect(p.controller).toMatchObject({ records: 'kubernetes', memory: { request: '64Mi', limit: '256Mi' } })
    expect(p.gateways).toMatchObject({ count: 2, service: { type: 'ClusterIP' } })
    expect(p.computes).toMatchObject({ defaultPgVersion: 19, idleAfter: '30m', scratch: '2Gi', memory: { request: '512Mi', limit: '1Gi' } })
    expect(p.databases.deleteHold).toBe('7d')
    expect(p.upgrades).toMatchObject({ quietHours: [], restartHours: [], checkTimeout: '10m' })
    expect(p.control).toMatchObject({ replicas: 2 })
    expect(p.suspend).toBe(false)
    // nothing given is left as none: keys made, the platform's own authority
    expect(p.keys).toEqual({})
    expect(p.certificates).toEqual({})
  })
  test('a missing release, an unknown field, a wrong enum, five keepers allowed but four refused, a bad window, a bad hold', () => {
    expect(check(platformSpec, { objectStore: minimal.objectStore })).toEqual(['release: is required'])
    expect(check(platformSpec, { ...minimal, storage: {} })).toEqual(['storage: is not a field'])
    expect(check(platformSpec, { ...minimal, spread: 'zones' })).toEqual(['spread: is one of machines, none'])
    expect(check(platformSpec, { ...minimal, safekeepers: { count: 5 } })).toEqual([])
    expect(check(platformSpec, { ...minimal, safekeepers: { count: 4 } })).toEqual(['safekeepers.count: is one of 3, 5'])
    expect(check(platformSpec, { ...minimal, upgrades: { quietHours: [{ start: '25:00', end: '01:00' }] } })).toEqual(['upgrades.quietHours[0].start: "25:00" does not match ^([01][0-9]|2[0-3]):[0-5][0-9]$'])
    expect(check(platformSpec, { ...minimal, databases: { deleteHold: 'never' } })).toEqual([])
    expect(check(platformSpec, { ...minimal, databases: { deleteHold: 'a week' } })).toHaveLength(1)
    expect(check(platformSpec, { ...minimal, controller: { records: 'postgres', postgres: { urlSecret: 'records-db' } } })).toEqual([])
  })
})

describe('the shared names and the kinds\' defaults', () => {
  test('the Platform\'s default namespaces and machine label are src/names.ts\'s', () => {
    const p = withDefaults(platformSpec, { release: 'r', objectStore: { endpoint: 'https://s3.example.com', bucket: 'b', credentialsSecret: 's' } }) as any
    expect(p.namespaces).toEqual({ platform: DEFAULT_NAMES.namespace, computes: DEFAULT_NAMES.computeNamespace, gateways: DEFAULT_NAMES.gatewayNamespace, databases: DEFAULT_NAMES.dbNamespace })
    expect(p.machines.label).toEqual(DEFAULT_NAMES.machineLabel)
  })
  test('a taint with no value is written with an empty one', () => {
    expect(formatTaint({ key: 'k', effect: 'NoSchedule' } as any)).toBe('k=:NoSchedule')
  })
})

describe('Database', () => {
  test('a database, a branch, a restore to a moment and a replica', () => {
    expect(check(databaseSpec, {})).toEqual([])
    expect(check(databaseSpec, { pgVersion: 18 })).toEqual([])
    expect(check(databaseSpec, { parent: 'orders' })).toEqual([])
    expect(check(databaseSpec, { parent: 'orders', at: '2026-12-01T09:30:00Z' })).toEqual([])
    expect(check(databaseSpec, { replicaOf: 'orders' })).toEqual([])
    expect(withDefaults(databaseSpec, {})).toEqual({ closed: false, keepOldFor: '30d' })
  })
  test('a version the platform does not run and a parent that is no database name are refused', () => {
    expect(check(databaseSpec, { pgVersion: 17 })).toEqual(['pgVersion: is one of 18, 19'])
    expect(check(databaseSpec, { parent: 'Orders' })).toHaveLength(1)
  })
})

describe('the CustomResourceDefinitions', () => {
  const all = crds() as any[]
  test('two, in peat.zabaca.com: platforms cluster-scoped, databases namespaced, each with a status subresource', () => {
    expect(all.map((c) => [c.metadata.name, c.spec.scope])).toEqual([['platforms.peat.zabaca.com', 'Cluster'], ['databases.peat.zabaca.com', 'Namespaced']])
    for (const c of all) expect(c.spec.versions[0].subresources).toEqual({ status: {} })
  })
  test('structural, as the API server requires: every node has a type, and no additionalProperties false', () => {
    const walk = (s: Schema, at: string): string[] => {
      const out: string[] = []
      if (!s.type && !s['x-kubernetes-int-or-string']) out.push(`${at}: no type`)
      if ((s as any).additionalProperties === false) out.push(`${at}: additionalProperties false`)
      for (const [k, v] of Object.entries(s.properties ?? {})) out.push(...walk(v, `${at}.${k}`))
      if (s.items) out.push(...walk(s.items, `${at}[]`))
      if (typeof s.additionalProperties === 'object') out.push(...walk(s.additionalProperties, `${at}{}`))
      return out
    }
    for (const c of all) expect(walk(c.spec.versions[0].schema.openAPIV3Schema, c.metadata.name)).toEqual([])
  })
  test('the rules across fields the descriptions promise are the API server\'s to judge, in the CustomResourceDefinitions', () => {
    const text = JSON.stringify(all)
    for (const message of [
      'the namespaces cannot change', 'may not be default or one of the cluster', 'the store needs the one bucket', 'records postgres needs controller.postgres', 'spread none runs one pageserver',
      'at is the moment of a parent', 'not both', 'parent cannot change', 'at cannot change', 'replicaOf cannot change', 'pgVersion is never lowered',
    ]) expect([message, text.includes(message)]).toEqual([message, true])
  })
  test('every duration is written one way: a number and s, m, h or d', () => {
    expect(check(platformSpec, { ...{ release: 'r', objectStore: { endpoint: 'https://s3.example.com', bucket: 'b', credentialsSecret: 's' } }, computes: { idleAfter: '30 minutes' } })).toHaveLength(1)
    expect(check(databaseSpec, { idleAfter: '5m', keepOldFor: '30d' })).toEqual([])
  })
  test('every field says what it is for (kubectl explain prints it)', () => {
    const missing: string[] = []
    const walk = (s: Schema, at: string) => {
      for (const [k, v] of Object.entries(s.properties ?? {})) {
        if (!v.description) missing.push(`${at}.${k}`)
        walk(v, `${at}.${k}`)
      }
      if (s.items) walk(s.items, `${at}[]`)
    }
    for (const [kind, k] of Object.entries(KINDS)) { walk(k.spec, `${kind}.spec`); walk(k.status, `${kind}.status`) }
    expect(missing).toEqual([])
  })
})
