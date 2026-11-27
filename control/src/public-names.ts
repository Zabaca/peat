// The public-names check: what must not be in this folder, because everything in it is published. It carries no list of names (it cannot
// know who is real); it finds the shapes a real name takes and allows only what is plainly not one:
//
//  - dash: an em dash or an en dash (the folder's prose uses commas, colons, semicolons and parentheses);
//  - address: an IPv4 address outside the documentation ranges (RFC 5737), loopback, the unspecified address and the first address of a
//    private, link-local or shared range (as a CIDR names it); an IPv6 address outside the documentation prefix 2001:db8::/32 (RFC
//    3849), loopback and the unspecified address;
//  - host: a host name under a real top-level domain that is not a reserved example (RFC 2606, and home.arpa, RFC 8375) or a third party listed below with its
//    reason, and a bucket named in an Amazon S3 host or path or an s3:// address;
//  - email: an address at any domain but a reserved example;
//  - maintainer: the maintainer's name anywhere but its public addresses (this repository, its images, the API group of Peat's object kinds)
//    and a credit or licence line;
//  - record: a path of a document this repository does not hold (a reference a reader cannot follow).
//
// In TypeScript and JavaScript, hosts and addresses of mail are looked for in comments and in the text of strings only: code is property
// chains (`o.metadata.name`), which read like host names. A host may begin after a `.`, `*.`, `${...}.` or `@`, so a name put together in
// a template, or the domain of an address, is found. Every top-level domain IANA lists counts (src/tlds.txt), less: those that are also a
// file's extension (md, sh, rs, py, pl, so, ps, mk: `README.md` is a file); a name with a label of one letter (`d.name`, `o.metadata.at`:
// property chains); and a few last labels that are property names of this code (CODE_PROPERTIES, each with its reason).
//
// A pure function over one file's path and text, so its rules are tested on made-up lines (test/public-names.test.ts); the same test runs
// it over the whole folder and fails on any finding. A second layer, with a private list of names, is run by the maintainer before a change
// to this folder merges.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'

export interface Hit { path: string; line: number; kind: string; detail: string }

/** The top-level domains, as IANA lists them (src/tlds.txt). */
const IANA = readFileSync(join(import.meta.dir, 'tlds.txt'), 'utf8').split('\n').map((l) => l.trim()).filter((l) => l && !l.startsWith('#'))
/** Two-letter domains that are also files' extensions: read as files. */
const EXTENSIONS = new Set(['md', 'sh', 'rs', 'py', 'pl', 'so', 'ps', 'mk'])
const TLDS = new Set(IANA.filter((t) => !EXTENSIONS.has(t)))

/** Third parties' domains the code or its documentation names, each with why; a domain listed allows everything under it. */
export const THIRD_PARTY_DOMAINS: Record<string, string> = {
  'kubernetes.io': 'Kubernetes: its well-known labels and taints (kubernetes.io/hostname, node.kubernetes.io/unreachable) and documentation',
  'k8s.io': 'Kubernetes: API groups (rbac.authorization.k8s.io, admissionregistration.k8s.io, coordination.k8s.io, networking.k8s.io)',
  'x-k8s.io': 'Kubernetes SIG projects: kind and its node image',
  'cert-manager.io': 'cert-manager: its API group, which the platform uses for database certificates',
  'cnpg.io': 'CloudNativePG: the annotation the gateway program reads (its rows are written as CloudNativePG Clusters)',
  'cloudnative-pg.io': 'CloudNativePG: the barman-cloud plugin, which the restore drill of a controller database in Postgres uses',
  'xata.io': 'The sleep annotations the gateway program reads, under the prefix of the plugin that first defined them',
  'github.com': 'GitHub: this repository and the projects it names; an owner other than the ones in `GITHUB_OWNERS` is a finding',
  'githubusercontent.com': 'GitHub: raw files of the projects named',
  'ghcr.io': 'GitHub container registry: the published images; an owner other than the maintainer is a finding',
  'docker.io': 'Docker Hub: the base images',
  'postgresql.org': 'PostgreSQL: documentation',
  'neon.tech': 'Neon: documentation of the storage this platform runs',
  'amazonaws.com': 'Amazon S3: the store whose API every object store here speaks (a bucket named in its host or path is a finding)',
  'bun.sh': 'Bun: the runtime and its documentation',
  'min.io': 'MinIO: the S3-compatible store the install test starts',
  'ietf.org': 'IETF: RFCs',
  'iana.org': 'IANA: the list of top-level domains this check reads (src/tlds.txt)',
  'rfc-editor.org': 'RFCs',
  'rust-lang.org': 'Rust: the toolchain installer the CI images fetch (static.rust-lang.org)',
}

/** The GitHub and registry owners an address may name, each with why. */
export const GITHUB_OWNERS: Record<string, string> = {
  zabaca: 'the maintainer: this repository, peat-postgres and the published images',
  neondatabase: 'Neon: the code Peat is forked from',
  'jclab-oss': 'the community fork Peat was forked at',
  'kubernetes-sigs': 'kind, the throwaway cluster of the install test',
  'etcd-io': "etcd: the release whose etcdctl and etcdutl the storage controller's etcd restore test runs",
  'cert-manager': 'cert-manager',
  oven: 'Bun',
  minio: 'MinIO',
  library: "Docker Hub's official images (docker.io/library/postgres, busybox)",
  bitnamilegacy: 'The MinIO image the install test runs as its store',
  'nextest-rs': 'cargo-nextest, the Rust test runner CI installs',
  rhysd: 'actionlint, the workflow linter CI runs',
}

/** The maintainer's name, and the places it may stand: its public addresses. */
const MAINTAINER = 'zabaca'
const MAINTAINER_ADDRESSES = [/\bpeat\.zabaca\.com\b/gi, /github\.com\/zabaca\/peat(?:-postgres)?\b/gi, /ghcr\.io\/zabaca\/peat\b/gi, /\bzabaca\/peat(?:-postgres)?\b/gi]
/** A credit or licence line: "maintained by Zabaca", "Copyright 2026 Zabaca LLC". */
const CREDIT = /(?:\b(?:maintained|made|built|published|developed|written)\s+by|copyright(?:\s+\(c\))?\s*(?:\d{4}\s*,?\s*)?)\s*\[?zabaca\b/i

// and the first address of each private, link-local and shared range (RFC 1918, 3927, 6598) as it names the range in a CIDR: a range, never
// a host of anyone's
const DOC_RANGES = [/^192\.0\.2\.\d+$/, /^198\.51\.100\.\d+$/, /^203\.0\.113\.\d+$/, /^127\.\d+\.\d+\.\d+$/, /^0\.0\.0\.0$/, /^255\.255\.255\.255$/, /^(10\.0\.0\.0|172\.16\.0\.0|192\.168\.0\.0|169\.254\.0\.0|100\.64\.0\.0)$/]

const endsWithDomain = (host: string, domain: string) => host === domain || host.endsWith(`.${domain}`)
const reservedHost = (host: string) => /(?:^|\.)(?:example|test|invalid|localhost|home\.arpa)$/.test(host) || ['example.com', 'example.org', 'example.net'].some((d) => endsWithDomain(host, d))
const realDomain = (host: string) => TLDS.has(host.split('.').pop()!)

/**
 * The text of a TypeScript or JavaScript file where a host could be: its comments (all of them, backtick-quoted text included) and the
 * text of its strings (less the `${...}` of a template), every other character blanked, line breaks kept. A small reader of the language:
 * strings, templates nested in their expressions, comments and regular-expression literals (which are code).
 */
export function proseOf(text: string): string {
  const out: string[] = text.split('').map((c) => (c === '\n' ? '\n' : ' '))
  const keep = (i: number) => { out[i] = text[i]! }
  const stack: ('code' | 'template')[] = ['code']
  const braces: number[] = [0]
  let i = 0
  let prev = '' // the last significant code character, to tell a regular expression from a division
  while (i < text.length) {
    const c = text[i]!, n = text[i + 1]
    const mode = stack[stack.length - 1]
    if (mode === 'template') {
      if (c === '\\') { keep(i); if (n !== undefined) keep(i + 1); i += 2; continue }
      if (c === '`') { stack.pop(); i++; prev = '`'; continue }
      if (c === '$' && n === '{') { stack.push('code'); braces.push(0); i += 2; prev = '{'; continue }
      keep(i); i++; continue
    }
    if (c === '/' && n === '/') { // a line comment: prose, its backtick-quoted text included (a host quoted there is still a host)
      let j = i + 2
      while (j < text.length && text[j] !== '\n') { keep(j); j++ }
      i = j; continue
    }
    if (c === '/' && n === '*') {
      let j = i + 2
      while (j < text.length && !(text[j] === '*' && text[j + 1] === '/')) { keep(j); j++ }
      i = j + 2; continue
    }
    if (c === '"' || c === "'") {
      let j = i + 1
      while (j < text.length && text[j] !== c && text[j] !== '\n') { if (text[j] === '\\') { keep(j); j++ } keep(j); j++ }
      i = j + 1; prev = c; continue
    }
    if (c === '`') { stack.push('template'); i++; continue }
    // a slash after `x++` or `x--` is a division, not the start of a regular expression
    const afterStep = (prev === '+' || prev === '-') && /(\+\+|--)\s*$/.test(text.slice(Math.max(0, i - 6), i))
    if (c === '/' && !afterStep && (prev === '' || /[(,=:[!&|?{};+\-*%<>~^]/.test(prev) || /\b(?:return|typeof|case|in|of)\s*$/.test(text.slice(Math.max(0, i - 8), i)))) {
      // a regular-expression literal: code, read to its end (a slash in a character class does not end it)
      let j = i + 1, cls = false
      while (j < text.length && text[j] !== '\n') { if (text[j] === '\\') { j += 2; continue } if (text[j] === '[') cls = true; else if (text[j] === ']') cls = false; else if (text[j] === '/' && !cls) break; j++ }
      i = j + 1; prev = '/'; continue
    }
    if (c === '{') braces[braces.length - 1]!++
    if (c === '}') {
      if (stack.length > 1 && braces[braces.length - 1] === 0) { stack.pop(); braces.pop(); i++; prev = '}'; continue }
      braces[braces.length - 1]!--
    }
    if (!/\s/.test(c)) prev = c
    i++
  }
  return out.join('')
}

/**
 * The text of a Python file where a host could be: its comments and the text of its strings (single, double and triple quoted), every
 * other character blanked, line breaks kept. `tarfile.open` and `entry.name` are code.
 */
export function pythonProseOf(text: string): string {
  const out: string[] = text.split('').map((c) => (c === '\n' ? '\n' : ' '))
  const keep = (i: number) => { out[i] = text[i]! }
  let i = 0
  while (i < text.length) {
    const c = text[i]!
    if (c === '#') { while (i < text.length && text[i] !== '\n') { keep(i); i++ } continue }
    if (c === '"' || c === "'") {
      const triple = text.slice(i, i + 3) === c.repeat(3)
      const end = triple ? c.repeat(3) : c
      let j = i + end.length
      while (j < text.length && text.slice(j, j + end.length) !== end && (triple || text[j] !== '\n')) { if (text[j] === '\\') { keep(j); j++ } keep(j); j++ }
      i = j + end.length; continue
    }
    i++
  }
  return out.join('')
}

/** The roots of an admission rule's CEL expressions, whose fields read like names. */
const CEL_ROOTS = new Set(['params', 'object', 'oldobject', 'request', 'variables'])

const CODE_FILE = /\.(?:[cm]?[jt]sx?|py)$/
/** Shell scripts and workflows: their jq and JSONPath (`.data.ca`) and their SQL (`w.id = a.id`) name fields, not hosts. */
const SCRIPT_FILE = /\.(?:sh|ya?ml)$/
/**
 * Last labels that are top-level domains and also property names this code reads (quoted in its comments, its tests' strings and its
 * kinds' rules), each with its reason: a host under one of these is not found.
 */
const CODE_PROPERTIES = new Set([
  'pg', // a database's Postgres version (d.pg)
  'id', // every record's id (sk.id)
  'at', // a Database's moment (self.at)
  'name', // every object's name (metadata.name)
  'now', // the clock (Date.now)
  'host', // a database's host (status.host)
  'storage', // the storage image (images.storage)
  'direct', // a wake's direct dial (wake.direct)
])

/** The findings of one file. `path` is relative to the folder scanned; `exists` says whether a path relative to the repository's root is in it. */
export function publicNameHits(path: string, text: string, exists: (repoPath: string) => boolean = () => true): Hit[] {
  const hits: Hit[] = []
  const lines = text.split(/\r?\n/)
  // a workflow's own expressions (`${{ github.actor }}`) are code, not prose
  const prose = (/\.py$/.test(path) ? pythonProseOf(text) : CODE_FILE.test(path) ? proseOf(text) : /\.ya?ml$/.test(path) ? text.replace(/\$\{\{[^}]*\}\}/g, (m) => ' '.repeat(m.length)) : text).split(/\r?\n/)
  // pip-compile's input files (`-r poetry.in`) in a pinned requirements file, and the parameters of a pytest id (`test_x[A-B.C]`) in a
  // file of test durations: names, not hosts
  const pipInput = /requirements[^/]*\.txt$/.test(path)
  const durations = /test-durations\.json$/.test(path)
  const licence = /(?:^|\/)(?:LICENSE|NOTICE|COPYING)(?:\.[a-z]+)?$/i.test(path)
  for (const [i, line] of lines.entries()) {
    const said = prose[i] ?? ''
    const add = (kind: string, detail: string) => hits.push({ path, line: i + 1, kind, detail })
    if (/[\u2013\u2014]/.test(line)) add('dash', 'an em dash or an en dash')
    for (const m of line.matchAll(/(?<![\d.])(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?![\d.]*\d)/g)) {
      const ip = m[0]
      if (m.slice(1, 5).some((x) => Number(x) > 255)) continue
      // an object identifier (`oid('2.5.4.3')`, `extension('2.5.29.19', ...)`) and a numbered section of an RFC ("RFC 5280, 4.1.2.5") are written as dotted numbers too
      const before = line.slice(0, m.index)
      if (/\b(?:oid|extension)\(['"]$/.test(before) || /\bRFC \d+,? (?:section )?$/i.test(before)) continue
      if (!DOC_RANGES.some((r) => r.test(ip))) add('address', ip)
    }
    for (const m of said.matchAll(/([A-Za-z0-9._%+-]+)@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)/g)) {
      const domain = m[2]!.toLowerCase()
      if (m[1] === 'git' && domain === 'github.com') continue
      if (!reservedHost(domain) && realDomain(domain)) add('email', m[0])
    }
    const params = durations ? [...said.matchAll(/\btest_\w+\[([^\]]*)\]/g)].map((x) => [x.index! + x[0].indexOf('[') + 1, x.index! + x[0].length - 1] as const) : []
    for (const m of said.matchAll(/(?<![A-Za-z0-9_-])((?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])(?![A-Za-z0-9_-])/g)) {
      const host = m[1]!.toLowerCase()
      if (!realDomain(host) || reservedHost(host)) continue
      if (pipInput && host.endsWith('.in')) continue
      if (params.some(([a, b]) => m.index! >= a && m.index! < b)) continue
      // a jq or JSONPath field begins with a dot; an SQL column is a one-letter alias and a field name of the code (`w.id`)
      if (SCRIPT_FILE.test(path) && (said[m.index! - 1] === '.' || (host.split('.').some((l) => l.length === 1) && CODE_PROPERTIES.has(host.split('.').at(-1)!)))) continue
      // in code, a name with a label of one letter (`d.name`, `i.data.pg`) is a property chain, not a host; so is one ending in a property
      // name of this code. In a document or a configuration file (README, YAML, the examples) every host counts.
      const labels = host.split('.')
      if (CODE_FILE.test(path) && (labels.some((l) => l.length === 1) || CODE_PROPERTIES.has(labels.at(-1)!))) continue
      // in code, a CEL expression of an admission rule reads its roots' fields (params.data, object.spec): names, not hosts
      if (CODE_FILE.test(path) && CEL_ROOTS.has(labels[0]!)) continue
      if (host === 'peat.zabaca.com' || host.endsWith('.peat.zabaca.com')) continue
      const third = Object.keys(THIRD_PARTY_DOMAINS).find((d) => endsWithDomain(host, d))
      if (!third) { add('host', host); continue }
      // under amazonaws.com only the S3 API's own hosts are named here; any other host there is an account's (a registry, a load balancer, a database)
      if (third === 'amazonaws.com' && !/^(?:[a-z0-9.-]+\.)?s3(?:[.-][a-z0-9-]+)*\.amazonaws\.com$/.test(host)) { add('host', `${host}: an account's host`); continue }
      const rest = said.slice((m.index ?? 0) + m[0].length)
      if (third === 'github.com' || third === 'ghcr.io' || third === 'githubusercontent.com' || third === 'docker.io') {
        // the owner after the host: `/owner` in an address, `:owner` in git's own (git@github.com:owner/repository.git)
        const owner = /^[/:]([A-Za-z0-9_.-]+)/.exec(rest)?.[1]?.toLowerCase()
        if (owner && !(owner in GITHUB_OWNERS)) add('host', `${host}/${owner}`)
      }
      if (third === 'amazonaws.com') {
        // a bucket in the host (virtual-hosted style) or in the path (path style)
        const inHost = /^(.+)\.s3[.-](?:[a-z0-9-]+\.)*amazonaws\.com$/.exec(host)?.[1]
        const inPath = /^s3[.-](?:[a-z0-9-]+\.)*amazonaws\.com$|^s3\.amazonaws\.com$/.test(host) ? /^\/([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/.exec(rest)?.[1] : undefined
        const bucket = inHost ?? inPath
        if (bucket && !/^(?:example|my-bucket|bucket|your-bucket)(?:[.-]|$)/.test(bucket)) add('host', `${host}${inPath ? `/${inPath}` : ''}: a bucket`)
      }
    }
    // a bucket in a store's own address: s3://, gs://, r2://, an S3 ARN
    for (const m of said.matchAll(/(?:\b(?:s3|gs|r2):\/\/|\barn:aws:s3:::)([a-z0-9][a-z0-9._-]{1,62}[a-z0-9])/g)) if (!/^(?:example|my-bucket|bucket|your-bucket)(?:[._-]|$)/.test(m[1]!)) add('host', `${m[0].slice(0, m[0].length - m[1]!.length)}${m[1]}: a bucket`)
    // a host under a private suffix (.local, .internal, .lan, .corp), less the cluster's own names (svc.cluster.local)
    for (const m of said.matchAll(/(?<![A-Za-z0-9_-])((?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+(?:local|internal|lan|corp))(?![A-Za-z0-9_-])/gi)) {
      const host = m[1]!.toLowerCase()
      if (host.endsWith('.cluster.local') || host === 'cluster.local' || (CODE_FILE.test(path) && host.split('.').some((l) => l.length === 1))) continue
      add('host', `${host}: a private network's name`)
    }
    // an IPv6 address: two or more groups of hex digits, with a :: or eight groups, and a letter or a :: (so a time of day is none)
    for (const m of line.matchAll(/(?<![\w:.])((?:[0-9a-fA-F]{1,4})?(?::[0-9a-fA-F]{0,4}){2,7})(?![\w:.])/g)) {
      const a = m[1]!.toLowerCase()
      const groups = a.split(':')
      if (!(a.includes('::') || groups.length === 8) || !(/[a-f]/.test(a) || a.includes('::')) || groups.filter(Boolean).length === 0) continue
      if (/^2001:0?db8(?::|$)/.test(a) || a === '::1' || a === '::' || /^::ffff:/.test(a) || /^fe80:/.test(a)) continue
      add('address', a)
    }
    if (!licence && !CREDIT.test(line)) {
      let rest = line
      for (const r of MAINTAINER_ADDRESSES) rest = rest.replace(r, '')
      if (rest.toLowerCase().includes(MAINTAINER)) add('maintainer', "the maintainer's name outside its public addresses and credit lines")
    }
    // a path of a document, given from the repository's root, that the repository does not hold: a reference a reader cannot follow
    for (const m of line.matchAll(/(?<![\w./-])((?:docs|packages|environments)\/[A-Za-z0-9_.\/-]*[A-Za-z0-9_])/g)) if (!exists(m[1]!)) add('record', `${m[1]} is not in this repository`)
  }
  return hits
}
