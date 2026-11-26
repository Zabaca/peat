// The public-names check: what must not be in this folder, because everything in it is published. It carries no list of names (it cannot
// know who is real); it finds the shapes a real name takes and allows only what is plainly not one:
//
//  - dash: an em dash or an en dash (the folder's prose uses commas, colons, semicolons and parentheses);
//  - address: an IPv4 address outside the documentation ranges (RFC 5737), loopback and the unspecified address;
//  - host: a host name under a real top-level domain that is not a reserved example (RFC 2606) or a third party listed below with its reason;
//  - email: an address at any domain but a reserved example;
//  - maintainer: the maintainer's name anywhere but its public addresses (this repository, its images, the API group of Peat's object kinds)
//    and a credit or licence line;
//  - record: a reference to a record the reader cannot open (another repository's planning documents).
//
// A pure function over one file's path and text, so its rules are tested on made-up lines (test/public-names.test.ts); the same test runs
// it over the whole folder and fails on any finding. A second layer, with a private list of names, is run by the maintainer before a change
// to this folder merges.

export interface Hit { path: string; line: number; kind: string; detail: string }

/** Top-level domains a real host name ends in, as far as this folder's text goes (a reserved example TLD is never one). */
const REAL_TLDS = ['com', 'net', 'org', 'io', 'ai', 'us', 'cloud', 'tech', 'xyz', 'dev']

/** Third parties' domains the code or its documentation names, each with why; a domain listed allows everything under it. */
export const THIRD_PARTY_DOMAINS: Record<string, string> = {
  'kubernetes.io': 'Kubernetes: its well-known labels and taints (kubernetes.io/hostname, node.kubernetes.io/unreachable) and documentation',
  'k8s.io': 'Kubernetes: API groups (rbac.authorization.k8s.io, admissionregistration.k8s.io, coordination.k8s.io, networking.k8s.io)',
  'x-k8s.io': 'Kubernetes SIG projects: kind and its node image',
  'cert-manager.io': 'cert-manager: its API group, which the platform uses for database certificates',
  'cnpg.io': 'CloudNativePG: the annotation the gateway program reads (its rows are written as CloudNativePG Clusters)',
  'xata.io': 'The sleep annotations the gateway program reads, under the prefix of the plugin that first defined them',
  'github.com': 'GitHub: this repository and the projects it names; an owner other than the ones in `GITHUB_OWNERS` is a finding',
  'githubusercontent.com': 'GitHub: raw files of the projects named',
  'ghcr.io': 'GitHub container registry: the published images; an owner other than the maintainer is a finding',
  'docker.io': 'Docker Hub: the base images',
  'postgresql.org': 'PostgreSQL: documentation',
  'neon.tech': 'Neon: documentation of the storage this platform runs',
  'amazonaws.com': 'Amazon S3: the store whose API every object store here speaks',
  'bun.sh': 'Bun: the runtime and its documentation',
  'min.io': 'MinIO: the S3-compatible store the install test starts',
  'cloudnative-pg.io': 'CloudNativePG: the barman-cloud plugin, which the restore drill of a controller database in Postgres uses',
  'ietf.org': 'IETF: RFCs',
  'rfc-editor.org': 'RFCs',
}

/** The GitHub and registry owners an address may name, each with why. */
export const GITHUB_OWNERS: Record<string, string> = {
  zabaca: 'the maintainer: this repository, peat-postgres and the published images',
  neondatabase: 'Neon: the code Peat is forked from',
  'jclab-oss': 'the community fork Peat was forked at',
  'kubernetes-sigs': 'kind, the throwaway cluster of the install test',
  'cert-manager': 'cert-manager',
  oven: 'Bun',
  minio: 'MinIO',
}

/** The maintainer's name, and the places it may stand: its public addresses. */
const MAINTAINER = 'zabaca'
const MAINTAINER_ADDRESSES = [/\bpeat\.zabaca\.com\b/gi, /github\.com\/zabaca\/peat(?:-postgres)?\b/gi, /ghcr\.io\/zabaca\/peat\b/gi, /\bzabaca\/peat(?:-postgres)?\b/gi]
/** A credit or licence line: "maintained by Zabaca", "Copyright 2026 Zabaca LLC". */
const CREDIT = /(?:\b(?:maintained|made|built|published|developed|written)\s+by|copyright(?:\s+\(c\))?\s*(?:\d{4}\s*,?\s*)?)\s*\[?zabaca\b/i

/** References to records a reader of this repository cannot open. */
const RECORD_PATTERNS: [RegExp, string][] = [
  [/\bdocs\/(?:problems|decisions|intents|clients|agents|prospects|finance)\//i, "another repository's planning documents"],
  [/\bpackages\/infra\b/i, "another repository's deploy code"],
  [/\benvironments\/[a-z0-9-]+\//i, "another repository's environments"],
  [/\bHome(?:'s|\u2019s)\b/, "another repository's name for itself"],
  [/\bCHECKPOINT\.md\b/, "another repository's session record"],
  [/\blab\/stage-\d/i, "another repository's lab record"],
]

const DOC_RANGES = [/^192\.0\.2\.\d+$/, /^198\.51\.100\.\d+$/, /^203\.0\.113\.\d+$/, /^127\.\d+\.\d+\.\d+$/, /^0\.0\.0\.0$/, /^255\.255\.255\.255$/]

const endsWithDomain = (host: string, domain: string) => host === domain || host.endsWith(`.${domain}`)
const reservedHost = (host: string) => /(?:^|\.)(?:example|test|invalid|localhost)$/.test(host) || ['example.com', 'example.org', 'example.net'].some((d) => endsWithDomain(host, d))

/** The findings of one file. `path` is relative to the folder scanned. */
export function publicNameHits(path: string, text: string): Hit[] {
  const hits: Hit[] = []
  const lines = text.split(/\r?\n/)
  const licence = /(?:^|\/)(?:LICENSE|NOTICE|COPYING)(?:\.[a-z]+)?$/i.test(path)
  for (const [i, raw] of lines.entries()) {
    const line = raw
    const add = (kind: string, detail: string) => hits.push({ path, line: i + 1, kind, detail })
    if (/[\u2013\u2014]/.test(line)) add('dash', 'an em dash or an en dash')
    for (const m of line.matchAll(/(?<![\d.])(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?![\d.]*\d)/g)) {
      const ip = m[0]
      if (m.slice(1, 5).some((x) => Number(x) > 255)) continue
      if (!DOC_RANGES.some((r) => r.test(ip))) add('address', ip)
    }
    for (const m of line.matchAll(/([A-Za-z0-9._%+-]+)@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)/g)) {
      const domain = m[2]!.toLowerCase()
      if (m[1] === 'git' && domain === 'github.com') continue
      if (!reservedHost(domain) && REAL_TLDS.includes(domain.split('.').pop()!)) add('email', m[0])
    }
    const hostRe = new RegExp(String.raw`(?<![A-Za-z0-9_.@-])((?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+(?:${REAL_TLDS.join('|')}))(?![A-Za-z0-9_-])`, 'gi')
    for (const m of line.matchAll(hostRe)) {
      const host = m[1]!.toLowerCase()
      if (reservedHost(host)) continue
      const third = Object.keys(THIRD_PARTY_DOMAINS).find((d) => endsWithDomain(host, d))
      if (host === 'peat.zabaca.com' || host.endsWith('.peat.zabaca.com')) continue
      if (!third) { add('host', host); continue }
      if (third === 'github.com' || third === 'ghcr.io' || third === 'githubusercontent.com') {
        const owner = /^\/([A-Za-z0-9_.-]+)/.exec(line.slice((m.index ?? 0) + m[0].length))?.[1]?.toLowerCase()
        if (owner && !(owner in GITHUB_OWNERS)) add('host', `${host}/${owner}`)
      }
    }
    if (!licence && !CREDIT.test(line)) {
      let rest = line
      for (const r of MAINTAINER_ADDRESSES) rest = rest.replace(r, '')
      if (rest.toLowerCase().includes(MAINTAINER)) add('maintainer', "the maintainer's name outside its public addresses and credit lines")
    }
    for (const [re, what] of RECORD_PATTERNS) if (re.test(line)) add('record', what)
  }
  return hits
}
