import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { crds } from '../src/api/crds'
import { check, withDefaults, type Schema } from '../src/api/schema'
import { API_VERSION, KINDS, databaseSpec, platformSpec } from '../src/api/v1alpha1'

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
    expect(p.pageservers).toMatchObject({ count: 2, memory: { request: '768Mi', limit: '1536Mi' }, storage: { hostPath: { path: '/var/lib/peat/pageserver', create: false } } })
    expect(p.safekeepers).toMatchObject({ count: 3, zoneLabel: 'kubernetes.io/hostname', memory: { request: '128Mi', limit: '512Mi' } })
    expect(p.controller).toMatchObject({ records: 'kubernetes', memory: { request: '64Mi', limit: '256Mi' } })
    expect(p.gateways).toMatchObject({ count: 2, service: { type: 'ClusterIP' } })
    expect(p.computes).toMatchObject({ defaultPgVersion: 19, idleMinutes: 30, scratch: '2Gi', memory: { request: '512Mi', limit: '1Gi' } })
    expect(p.databases.deleteHold).toBe('7d')
    expect(p.upgrades).toMatchObject({ quietHours: [], restartHours: [], checkSeconds: 600 })
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

describe('Database', () => {
  test('a database, a branch, a restore to a moment and a replica', () => {
    expect(check(databaseSpec, {})).toEqual([])
    expect(check(databaseSpec, { pgVersion: 18 })).toEqual([])
    expect(check(databaseSpec, { parent: 'orders' })).toEqual([])
    expect(check(databaseSpec, { parent: 'orders', at: '2026-12-01T09:30:00Z' })).toEqual([])
    expect(check(databaseSpec, { replicaOf: 'orders' })).toEqual([])
    expect(withDefaults(databaseSpec, {})).toEqual({ closed: false, keepOldDays: 30 })
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
