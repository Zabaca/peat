import { describe, expect, test } from 'bun:test'
import { spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { publicNameHits } from '../src/public-names'

// The public-names check (src/public-names.ts): its rules on made-up lines, then the whole folder, which must hold no finding.
// The scan leaves out the check's own two files: this one holds offending lines on purpose, and the check's source names what it allows.

const kinds = (line: string, path = 'notes.txt') => publicNameHits(path, line).map((h) => h.kind)

describe('the rules', () => {
  test('a dash: em and en dashes are findings; a hyphen is not', () => {
    expect(kinds('one \u2014 two')).toEqual(['dash'])
    expect(kinds('one \u2013 two')).toEqual(['dash'])
    expect(kinds('one - two, well-known')).toEqual([])
  })

  test('an address: only the documentation ranges, loopback and the unspecified address', () => {
    expect(kinds('listen on 0.0.0.0:5432 and 127.0.0.1')).toEqual([])
    expect(kinds('pod at 192.0.2.10, 198.51.100.7, 203.0.113.250')).toEqual([])
    expect(kinds('pod at 10.244.1.5')).toEqual(['address'])
    expect(kinds('the host 172.16.0.4')).toEqual(['address'])
    expect(kinds('version 1.2.3 and 1.2.3.4.5')).toEqual([])
    expect(kinds("seq(oid('2.5.4.3'), name) // RFC 5280, 4.1.2.5")).toEqual([])
  })

  test('a host: reserved examples, the listed third parties and the API group pass; anything else is a finding', () => {
    expect(kinds('db.example.com, a.example, b.test, svc.cluster.local')).toEqual([])
    expect(kinds("nodeSelector: { 'kubernetes.io/hostname': x }, rbac.authorization.k8s.io")).toEqual([])
    expect(kinds('group peat.zabaca.com/v1alpha1, label peat.zabaca.com/machine')).toEqual([])
    expect(kinds('see https://github.com/neondatabase/neon')).toEqual([])
    expect(kinds('see https://github.com/someone-else/fork')).toEqual(['host'])
    expect(kinds('image ghcr.io/zabaca/peat/storage')).toEqual([])
    expect(kinds('image ghcr.io/other/thing')).toEqual(['host'])
    expect(kinds('the store at storage.acme-corp.com')).toEqual(['host'])
    expect(kinds('cfg.spec.pageserver_connstring = x; console.log(a.b)')).toEqual([])
  })

  test('an email: only at a reserved example domain', () => {
    expect(kinds('owner: ops@example.com')).toEqual([])
    expect(kinds('git@github.com:Zabaca/peat.git')).toEqual([])
    expect(kinds('owner: someone@acme-corp.com')).toEqual(['email'])
    expect(kinds('image ghcr.io/zabaca/peat/storage@sha256:abc')).toEqual([])
  })

  test("the maintainer's name: in its public addresses and credit lines only", () => {
    expect(kinds('Peat is maintained by Zabaca.', 'README.md')).toEqual([])
    expect(kinds('Copyright 2026 Zabaca LLC', 'README.md')).toEqual([])
    expect(kinds('Zabaca runs this on its cluster.')).toEqual(['maintainer'])
    expect(kinds('a label zabaca.com/peat')).toEqual(['host', 'maintainer'])
    expect(kinds('anything at all about Zabaca', 'NOTICE')).toEqual([])
  })

  test('a record: a path of a document this repository does not hold', () => {
    const exists = (p: string) => p === 'docs/sourcetree.md'
    expect(publicNameHits('README.md', 'see docs/some-folder/some-file.md', exists).map((h) => h.kind)).toEqual(['record'])
    expect(publicNameHits('README.md', 'see docs/sourcetree.md and the home directory', exists)).toEqual([])
  })

  test('a host put together in code, after a dot, a star or an expression, and under any real top-level domain', () => {
    expect(kinds('const h = `${db}.acme-corp.com`', 'src/a.ts')).toEqual(['host'])
    expect(kinds("const w = '*.acme-corp.com'", 'src/a.ts')).toEqual(['host'])
    expect(kinds("const s = '.acme-corp.com'", 'src/a.ts')).toEqual(['host'])
    expect(kinds("const a = 'acme.co'", 'src/a.ts')).toEqual(['host'])
    expect(kinds("const a = 'acme.app'", 'src/a.ts')).toEqual(['host'])
    expect(kinds("const m = 'ops@acme.co'", 'src/a.ts')).toEqual(['email'])
    expect(kinds('// the backups live at acme-backups.s3.us-west-2.amazonaws.com', 'src/a.ts')).toEqual(['host'])
    expect(kinds('// path style: s3.us-west-2.amazonaws.com/acme-backups/x', 'src/a.ts')).toEqual(['host'])
    expect(kinds('// the API is at s3.us-west-2.amazonaws.com', 'src/a.ts')).toEqual([])
  })

  test('code is not prose: property chains, files and quoted code in comments are no hosts', () => {
    expect(kinds('const n = o.metadata.name; console.log(res.json, cfg.spec.co)', 'src/a.ts')).toEqual([])
    expect(kinds("// see README.md, control.ts and `o.spec.co` for why; a file x.sh", 'src/a.ts')).toEqual([])
    expect(kinds("const re = /'acme.co'/; const p = '.spec.template.containers'", 'src/a.ts')).toEqual([])
    expect(kinds('acme.co in a document', 'README.md')).toEqual(['host'])
  })
})

describe('the folder', () => {
  test('no file of this folder has a finding', () => {
    const root = join(import.meta.dir, '..')
    const listed = spawnSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard', '.'], { cwd: root, encoding: 'utf8' })
    expect(listed.status).toBe(0)
    const files = listed.stdout.split('\0').filter((f) => f && !['test/public-names.test.ts', 'src/public-names.ts', 'bun.lock'].includes(f))
    // a scan that read nothing would pass by finding nothing
    expect(files.length).toBeGreaterThan(5)
    const repo = join(root, '..')
    const found = files.flatMap((f) => publicNameHits(f, readFileSync(join(root, f), 'utf8'), (p) => existsSync(join(repo, p))).map((h) => `${h.path}:${h.line}  ${h.kind}  ${h.detail}`))
    expect(found).toEqual([])
  })
})
