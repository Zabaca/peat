import { describe, expect, test } from 'bun:test'
import { spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { publicNameHits } from '../src/public-names'

// The public-names check (src/public-names.ts): its rules on made-up lines, then the whole folder and the repository's .github, which must hold no finding.
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
    // home.arpa (RFC 8375) as the bare suffix; a machine under it is someone's, and a finding
    expect(kinds('a name under home.arpa')).toEqual([])
    expect(kinds('the store at nas-of-someone.home.arpa')).toEqual(['host'])
  })

  test('an email: only at a reserved example domain', () => {
    expect(kinds('owner: ops@example.com')).toEqual([])
    expect(kinds('git@github.com:Zabaca/peat.git')).toEqual([])
    expect(kinds('owner: someone@acme-corp.com')).toEqual(['email', 'host'])
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
    expect(kinds("const m = 'ops@acme.co'", 'src/a.ts')).toEqual(['email', 'host'])
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

describe('the second review\'s holes, and attempts to break the check', () => {
  const code = (line: string) => kinds(line, 'src/a.ts')
  test('a host quoted in backticks inside a comment', () => {
    expect(code('// see `db.acme-corp.com`')).toEqual(['host'])
    expect(code('/* the store: `store.acme-corp.com` */')).toEqual(['host'])
  })
  test('a host right after @ with no local part', () => {
    expect(code('const a = `${user}@acme-corp.com`')).toEqual(['host'])
    expect(kinds('<user>@acme-corp.com')).toEqual(['host'])
    expect(code('const o = { domain: "@acme-corp.com" }')).toEqual(['host'])
  })
  test('second-level domains of two letters under a country', () => {
    for (const h of ['acme-corp.co.uk', 'acme.co.jp', 'acme.co.nz', 'acme.co.za', 'acme.co.in', 'acme.ac.uk', 'acme.or.jp', 'hp.de', 'qm.io']) expect([h, kinds(`at ${h}`)]).toEqual([h, ['host']])
  })
  test('the newer generic domains, from IANA\'s list', () => {
    for (const t of ['health', 'clinic', 'law', 'dental', 'media', 'design', 'consulting', 'church', 'nyc', 'vip', 'fun']) expect([t, kinds(`at acme.${t}`)]).toEqual([t, ['host']])
  })
  test('git\'s own address names its owner after a colon; an s3:// address names its bucket', () => {
    expect(kinds('git@github.com:acme-corp/private.git')).toEqual(['host'])
    expect(kinds('git@github.com:Zabaca/peat.git')).toEqual([])
    expect(kinds('the copy at s3://acme-backups/path')).toEqual(['host'])
    expect(kinds('for example s3://example-bucket/path')).toEqual([])
  })
  test('IPv6: only the documentation prefix, loopback and the unspecified address', () => {
    expect(kinds('listen on [::1] and :: and 2001:db8::5')).toEqual([])
    expect(kinds('the node at 2a01:4f8:c17:1234::1')).toEqual(['address'])
    expect(kinds('at 05:00:30 and 12:30')).toEqual([])
  })
  test('a slash after x++ is a division, and the string after it is still read', () => {
    expect(code("let x = i++ / 2; const h = 'db.acme-corp.com'")).toEqual(['host'])
  })
  test('attempts to break it: a host split by a template, in a URL, uppercase, with a port, in YAML, in a shell line', () => {
    expect(code('const u = `https://${sub}.acme-corp.com:8443/x`')).toEqual(['host'])
    expect(code("const u = 'HTTPS://DB.ACME-CORP.COM'")).toEqual(['host'])
    expect(kinds('endpoint: https://store.acme-corp.io:9000', 'x.yaml')).toEqual(['host'])
    expect(kinds('curl -fsS https://api.acme-corp.dev/v1', 'x.sh')).toEqual(['host'])
    expect(code("const m = { 'ops+alerts@acme-corp.com': 1 }")).toEqual(['email', 'host'])
    expect(code('// mail goes to "Ops" <ops@acme.health>')).toEqual(['email', 'host'])
  })
  test('what it lets through on purpose, with why: property chains, files, the listed third parties and the examples', () => {
    expect(code('// o.metadata.name, Date.now(), status.host, images.storage')).toEqual([])
    expect(code("const f = 'README.md', g = 'run.sh', h = 'control.ts'")).toEqual([])
    expect(code("const a = 'db.example.com', b = 'x.test', c = 'kubernetes.io/hostname'")).toEqual([])
  })
})

describe('the third review\'s follow-ups', () => {
  test('the exemptions are code\'s alone: in a document a name like a property chain is a host', () => {
    expect(kinds('mail acme.name and see acme.host or x.acme-corp.com', 'README.md')).toEqual(['host', 'host', 'host'])
    expect(kinds('endpoint: https://x.acme-corp.com', 'examples/a.yaml')).toEqual(['host'])
    expect(kinds("const n = 'o.metadata.name'", 'src/a.ts')).toEqual([])
    expect(kinds("const d = 'json.data'", 'src/a.ts')).toEqual(['host']) // data is no longer exempt
  })
  test('under amazonaws.com only the S3 API\'s hosts; an account\'s registry, load balancer or database is a finding', () => {
    expect(kinds('the API at s3.us-west-2.amazonaws.com')).toEqual([])
    for (const h of ['123456789012.dkr.ecr.us-east-1.amazonaws.com', 'my-lb-123.us-east-1.elb.amazonaws.com', 'abc.gr7.us-east-1.eks.amazonaws.com', 'db.abc.us-east-1.rds.amazonaws.com']) expect([h, kinds(`at ${h}`)]).toEqual([h, ['host']])
  })
  test('docker.io names its owner as ghcr.io does', () => {
    expect(kinds('image docker.io/library/postgres:17')).toEqual([])
    expect(kinds('image docker.io/acmecorp/private-image:1')).toEqual(['host'])
  })
  test('a store\'s own addresses: an S3 ARN, gs:// and r2://', () => {
    expect(kinds('the bucket arn:aws:s3:::acme-backups')).toEqual(['host'])
    expect(kinds('copy to gs://acme-backups/x and r2://acme-records')).toEqual(['host', 'host'])
    expect(kinds('for example gs://example-bucket')).toEqual([])
  })
  test('private suffixes: .local (but the cluster\'s own), .internal, .lan, .corp', () => {
    expect(kinds('the router at gw.home.lan, nas.local, db.acme.internal, ad.acme.corp')).toEqual(['host', 'host', 'host', 'host'])
    expect(kinds('the service at storage-controller.peat.svc.cluster.local')).toEqual([])
  })
})

describe('the allowances the CI files need, each as narrow as its case', () => {
  test('Python: code is not prose (tarfile.open, entry.name); its comments and strings are', () => {
    expect(kinds('with tarfile.open(path, "w:gz") as tar:', 'ci/report.py')).toEqual([])
    expect(kinds('if not entry.name[len(stem) :].isdigit():', 'ci/report.py')).toEqual([])
    expect(kinds('# the results are copied to evil.com', 'ci/report.py')).toEqual(['host'])
    expect(kinds('url = "https://evil.com/x"', 'ci/report.py')).toEqual(['host'])
    expect(kinds('doc = """see evil.com"""', 'ci/report.py')).toEqual(['host'])
  })
  test('a workflow: its own expressions are code; every other host counts', () => {
    expect(kinds('          username: ${{ github.actor }}', '.github/workflows/ci.yml')).toEqual([])
    expect(kinds('          registry: evil.com', '.github/workflows/ci.yml')).toEqual(['host'])
  })
  test('a shell script: jq and JSONPath fields and SQL columns are not hosts; a host is', () => {
    expect(kinds("k get configmap peat-ca -o jsonpath='{.data.ca\\.crt}'", 'ci/test.sh')).toEqual([])
    expect(kinds('select 1 from w where w.id = a.id', 'ci/test.sh')).toEqual([])
    expect(kinds('curl -fsS https://evil.com/install.sh', 'ci/test.sh')).toEqual(['host'])
    expect(kinds('curl -fsS https://x.acme-corp.com/install.sh', 'ci/test.sh')).toEqual(['host'])
    expect(kinds('the field .data.ca', 'README.md')).toEqual(['host']) // not in a script
  })
  test('pip-compile\'s input file in a pinned requirements file, and pytest ids in the durations file', () => {
    expect(kinds('    # via -r poetry.in', 'ci/poetry-requirements.txt')).toEqual([])
    expect(kinds('poetry.in', 'README.md')).toEqual(['host'])
    expect(kinds('  "test_x[release-pg17-EmptyTimeline.EMPTY-DeletionSubject.TENANT-RestartStorcon.ONLINE]": 9.1,', 'ci/test-durations.json')).toEqual([])
    expect(kinds('  "see evil.com": 1,', 'ci/test-durations.json')).toEqual(['host'])
  })
  test('an admission rule\'s CEL in code reads its roots\' fields, not hosts; the same name in a document is a host', () => {
    expect(kinds("{ expression: \"has(params.data) && params.data.exists(m, true)\" }", 'src/render.ts')).toEqual([])
    expect(kinds('see params.data', 'README.md')).toEqual(['host'])
  })
  test('the CI files\' third parties: the Rust installer, cargo-nextest and actionlint', () => {
    expect(kinds('curl https://static.rust-lang.org/rustup/dist/x', 'ci/deps.sh')).toEqual([])
    expect(kinds('https://github.com/nextest-rs/nextest/releases', 'ci/deps.sh')).toEqual([])
    expect(kinds('https://github.com/rhysd/actionlint', 'ci/lint.yml')).toEqual([])
    expect(kinds('https://github.com/someone-else/tool', 'ci/lint.yml')).toEqual(['host'])
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
  test('no file of the repository\'s CI (.github) has a finding', () => {
    const repo = join(import.meta.dir, '..', '..')
    const listed = spawnSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard', '.github'], { cwd: repo, encoding: 'utf8' })
    expect(listed.status).toBe(0)
    const files = listed.stdout.split('\0').filter(Boolean)
    expect(files.length).toBeGreaterThan(5)
    const found = files.flatMap((f) => publicNameHits(f, readFileSync(join(repo, f), 'utf8'), (p) => existsSync(join(repo, p))).map((h) => `${h.path}:${h.line}  ${h.kind}  ${h.detail}`))
    expect(found).toEqual([])
  })
})
