// The platform's keys, made once when the install gives none, and kept: the Ed25519 pair Neon's tokens are signed with, the tokens each
// storage piece carries, and a certificate authority of the platform's own that signs each database's server certificate.
//
//   - The pair: the public half goes to every piece that verifies a token; the private half to the control layer alone, which signs the
//     tokens it hands the computes (scope `tenant`, one tenant each) and calls the storage with. Neon's tokens carry no expiry and no
//     rotation (its own documentation); none is added here.
//   - Seven tokens, one per holder and scope (TOKENS). Stored rather than minted at every reconcile, so a pod reads the same bytes after a
//     restart of the operator.
//   - The authority: valid ten years (nothing renews an authority, and a replaced one means every client must trust the new certificate).
//     Its certificate is public (published in every namespace that holds a Database); its key is not.
//
// Nothing here ever replaces a stored value: `plan` fills what is missing and refuses a half-made state it cannot complete (a token with no
// private key, a certificate without its key), because overwriting one half would orphan everything signed or trusted by the other.
// Certificates are written as DER by hand: Node can parse a certificate but cannot make one, and an external tool would put a private key
// in a file or a process's arguments.
import { X509Certificate, createHash, createPrivateKey, createPublicKey, generateKeyPairSync, randomBytes, sign, verify } from 'node:crypto'

export const AUTH_PRIVATE = 'auth_private_key.pem'
export const AUTH_PUBLIC = 'auth_public_key.pem'
export const CA_CERT = 'ca.crt'
export const CA_KEY = 'ca.key'
export const CA_COMMON_NAME = 'Peat platform CA'
export const CA_DAYS = 3650

/** Neon's token scopes, and which holder carries which. `key` is the entry in the operator's record and in the holder's Secret. */
export const TOKENS = [
  { key: 'storcon_to_pageservers.jwt', scope: 'pageserverapi', holder: 'controller', use: 'the storage controller to every pageserver' },
  { key: 'storcon_to_safekeepers.jwt', scope: 'safekeeperdata', holder: 'controller', use: 'the storage controller to every safekeeper' },
  { key: 'storcon_to_control_plane.jwt', scope: 'generations_api', holder: 'controller', use: "the storage controller's calls to the control layer's hook" },
  { key: 'storcon_peer.jwt', scope: 'admin', holder: 'controller', use: 'a controller to another controller (its handover)' },
  { key: 'pageserver_to_controller.jwt', scope: 'generations_api', holder: 'pageserver', use: "a pageserver's upcalls (re-attach, validate)" },
  { key: 'pageserver_to_safekeepers.jwt', scope: 'safekeeperdata', holder: 'pageserver', use: "a pageserver's log streams from the safekeepers" },
  { key: 'safekeeper_peer.jwt', scope: 'safekeeperdata', holder: 'safekeeper', use: 'a safekeeper to another (pull_timeline)' },
] as const
export type Holder = (typeof TOKENS)[number]['holder']

/** Every entry of the operator's record of the keys, in the order it writes them. */
export const ENTRIES = [AUTH_PRIVATE, AUTH_PUBLIC, ...TOKENS.map((t) => t.key), CA_CERT, CA_KEY] as const

const b64u = (b: Buffer | string) => Buffer.from(b).toString('base64url')

export function newAuthPair(): { privatePem: string; publicPem: string } {
  const { publicKey, privateKey } = generateKeyPairSync('ed25519')
  return { privatePem: privateKey.export({ format: 'pem', type: 'pkcs8' }) as string, publicPem: publicKey.export({ format: 'pem', type: 'spki' }) as string }
}

export const publicOf = (privatePem: string): string => createPublicKey(createPrivateKey(privatePem)).export({ format: 'pem', type: 'spki' }) as string

/** A token as Neon reads it: an EdDSA JWT whose claims are the scope (and, for a tenant's token, its tenant), with no expiry. */
export function mintToken(privatePem: string, scope: string, more: Record<string, unknown> = {}): string {
  const head = b64u(JSON.stringify({ alg: 'EdDSA', typ: 'JWT' }))
  const body = b64u(JSON.stringify({ scope, ...more }))
  return `${head}.${body}.${b64u(sign(null, Buffer.from(`${head}.${body}`), createPrivateKey(privatePem)))}`
}

/** The scope of a token whose signature `publicPem` verifies, or null when the token is malformed or signed by another key. */
export function verifiedScope(token: string, publicPem: string): string | null {
  const parts = token.split('.')
  if (parts.length !== 3) return null
  try {
    const head = JSON.parse(Buffer.from(parts[0]!, 'base64url').toString())
    if (head.alg !== 'EdDSA') return null
    const good = verify(null, Buffer.from(`${parts[0]}.${parts[1]}`), createPublicKey(publicPem), Buffer.from(parts[2]!, 'base64url'))
    const claims = JSON.parse(Buffer.from(parts[1]!, 'base64url').toString())
    return good && typeof claims.scope === 'string' ? claims.scope : null
  } catch {
    return null
  }
}

// ---- certificates (RFC 5280), as DER

const der = (tag: number, ...content: Buffer[]): Buffer => {
  const body = Buffer.concat(content)
  const n = body.length
  const len = n < 0x80 ? Buffer.from([n]) : n < 0x100 ? Buffer.from([0x81, n]) : Buffer.from([0x82, n >> 8, n & 0xff])
  return Buffer.concat([Buffer.from([tag]), len, body])
}
const seq = (...c: Buffer[]) => der(0x30, ...c)
const oid = (dotted: string) => {
  const [a, b, ...rest] = dotted.split('.').map(Number) as [number, number, ...number[]]
  const bytes = [a * 40 + b]
  for (const part of rest) {
    const groups = [part & 0x7f]
    for (let v = part >> 7; v > 0; v >>= 7) groups.unshift((v & 0x7f) | 0x80)
    bytes.push(...groups)
  }
  return der(0x06, Buffer.from(bytes))
}
/** UTCTime before 2050, GeneralizedTime from then (RFC 5280, 4.1.2.5). */
const time = (d: Date) => {
  const p = (n: number) => String(n).padStart(2, '0')
  const year = d.getUTCFullYear()
  const rest = `${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}Z`
  return year < 2050 ? der(0x17, Buffer.from(`${p(year % 100)}${rest}`)) : der(0x18, Buffer.from(`${year}${rest}`))
}
const name = (commonName: string) => seq(der(0x31, seq(oid('2.5.4.3'), der(0x0c, Buffer.from(commonName)))))
const ECDSA_SHA256 = seq(oid('1.2.840.10045.4.3.2'))
const extension = (id: string, critical: boolean, value: Buffer) => seq(oid(id), ...(critical ? [der(0x01, Buffer.from([0xff]))] : []), der(0x04, value))
const serialNumber = () => {
  const s = randomBytes(16)
  s[0] = (s[0]! & 0x7f) | 0x01 // positive, and never a leading zero byte that DER would have to drop
  return s
}
const keyIdOf = (spki: Buffer) => createHash('sha1').update(spki.subarray(spki.length - 65)).digest() // RFC 5280, 4.2.1.2, method 1: a P-256 key's bit string
const pem = (kind: string, b: Buffer) => `-----BEGIN ${kind}-----\n${b.toString('base64').match(/.{1,64}/g)!.join('\n')}\n-----END ${kind}-----\n`
const signTbs = (tbs: Buffer, key: ReturnType<typeof createPrivateKey>) => seq(tbs, ECDSA_SHA256, der(0x03, Buffer.concat([Buffer.from([0x00]), sign('sha256', tbs, { key, dsaEncoding: 'der' })])))

/** A self-signed authority: basicConstraints CA (critical), keyUsage keyCertSign and cRLSign (critical), its key identifier. */
export function newCa(now: Date = new Date(), days: number = CA_DAYS): { certPem: string; keyPem: string } {
  const { publicKey, privateKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
  const spki = publicKey.export({ format: 'der', type: 'spki' })
  const subject = name(CA_COMMON_NAME)
  const tbs = seq(
    der(0xa0, der(0x02, Buffer.from([0x02]))),
    der(0x02, serialNumber()),
    ECDSA_SHA256,
    subject,
    seq(time(now), time(new Date(now.getTime() + days * 86_400_000))),
    subject,
    spki,
    der(0xa3, seq(
      extension('2.5.29.19', true, seq(der(0x01, Buffer.from([0xff])))),
      extension('2.5.29.15', true, der(0x03, Buffer.from([0x01, 0x06]))), // keyCertSign (bit 5) and cRLSign (bit 6)
      extension('2.5.29.14', false, der(0x04, keyIdOf(spki))),
    )),
  )
  return { certPem: pem('CERTIFICATE', signTbs(tbs, privateKey)), keyPem: privateKey.export({ format: 'pem', type: 'pkcs8' }) as string }
}

/**
 * A database's server certificate, signed by the authority: its names in subjectAltName (the first is also its common name), keyUsage
 * digitalSignature and keyEncipherment, extendedKeyUsage serverAuth, and the authority's key identifier. A key of its own, P-256.
 */
export function newLeaf(ca: { certPem: string; keyPem: string }, dnsNames: string[], now: Date = new Date(), days = 365): { certPem: string; keyPem: string } {
  if (dnsNames.length === 0) throw new Error('a certificate needs at least one name')
  const caCert = new X509Certificate(ca.certPem)
  const caSpki = caCert.publicKey.export({ format: 'der', type: 'spki' })
  const { publicKey, privateKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
  const spki = publicKey.export({ format: 'der', type: 'spki' })
  const tbs = seq(
    der(0xa0, der(0x02, Buffer.from([0x02]))),
    der(0x02, serialNumber()),
    ECDSA_SHA256,
    name(CA_COMMON_NAME),
    seq(time(new Date(now.getTime() - 300_000)), time(new Date(now.getTime() + days * 86_400_000))), // five minutes back: a client whose clock is a little behind
    name(dnsNames[0]!),
    spki,
    der(0xa3, seq(
      extension('2.5.29.15', true, der(0x03, Buffer.from([0x05, 0xa0]))), // digitalSignature (bit 0) and keyEncipherment (bit 2)
      extension('2.5.29.37', false, seq(oid('1.3.6.1.5.5.7.3.1'))), // serverAuth
      extension('2.5.29.17', false, seq(...dnsNames.map((n) => der(0x82, Buffer.from(n))))),
      extension('2.5.29.35', false, seq(der(0x80, keyIdOf(caSpki)))),
      extension('2.5.29.14', false, der(0x04, keyIdOf(spki))),
    )),
  )
  return { certPem: pem('CERTIFICATE', signTbs(tbs, createPrivateKey(ca.keyPem))), keyPem: privateKey.export({ format: 'pem', type: 'pkcs8' }) as string }
}

// ---- what to make, from what is stored

/** The entries to make, given which are stored. Throws, naming entries, for a state it cannot complete without replacing a stored value. */
export function plan(present: ReadonlySet<string>): { pair: boolean; derivePublic: boolean; tokens: string[]; ca: boolean } {
  const hasPrivate = present.has(AUTH_PRIVATE)
  const heldTokens = TOKENS.filter((t) => present.has(t.key)).map((t) => t.key)
  if (!hasPrivate && (present.has(AUTH_PUBLIC) || heldTokens.length > 0)) {
    const orphans = [...(present.has(AUTH_PUBLIC) ? [AUTH_PUBLIC] : []), ...heldTokens]
    throw new Error(`${orphans.join(', ')} ${orphans.length === 1 ? 'is' : 'are'} kept without ${AUTH_PRIVATE}: a new pair would orphan what the old one signed, and a key cannot be derived from its public half. Nothing was written; remove them, or give the private key.`)
  }
  if (present.has(CA_CERT) !== present.has(CA_KEY)) throw new Error(`${present.has(CA_CERT) ? CA_CERT : CA_KEY} is kept without ${present.has(CA_CERT) ? CA_KEY : CA_CERT}: a new authority would orphan the kept half. Nothing was written.`)
  return { pair: !hasPrivate, derivePublic: hasPrivate && !present.has(AUTH_PUBLIC), tokens: TOKENS.filter((t) => !present.has(t.key)).map((t) => t.key), ca: !present.has(CA_CERT) }
}

/** The values to add to what is kept, by entry. `kept` is the record as it is (and any given keys). */
export function makeMissing(kept: Readonly<Record<string, string>>, now: Date = new Date()): Record<string, string> {
  const todo = plan(new Set(Object.keys(kept).filter((k) => (ENTRIES as readonly string[]).includes(k) && kept[k] !== '')))
  const out: Record<string, string> = {}
  let privatePem = kept[AUTH_PRIVATE]
  if (todo.pair) {
    const pair = newAuthPair()
    privatePem = pair.privatePem
    out[AUTH_PRIVATE] = pair.privatePem
    out[AUTH_PUBLIC] = pair.publicPem
  } else if (todo.derivePublic) out[AUTH_PUBLIC] = publicOf(privatePem!)
  for (const t of TOKENS) if (todo.tokens.includes(t.key)) out[t.key] = mintToken(privatePem!, t.scope)
  if (todo.ca) {
    const ca = newCa(now)
    out[CA_CERT] = ca.certPem
    out[CA_KEY] = ca.keyPem
  }
  return out
}

/** How long before an issuer's certificate expires the status says so. */
export const ISSUER_WARN_DAYS = 30

/**
 * With certificates.issuer, what keys.caSecret gives: the issuer's certificate (ca.crt first, the authority above an intermediate issuer's
 * own certificate, else tls.crt), and what is wrong with it. Its key must not be there: the control layer's account can read the platform
 * namespace's Secrets, and an issuer's authority may be trusted far beyond the platform. Problems fail the pass; warnings go in the status.
 */
export function issuerCaFindings(given: Readonly<Record<string, string>>, now: Date = new Date()): { cert?: string; problems: string[]; warnings: string[] } {
  const problems: string[] = [], warnings: string[] = []
  if (given['tls.key']) problems.push("keys.caSecret: holds tls.key; with certificates.issuer give the issuer's certificate alone (ca.crt or tls.crt): the control layer can read the platform namespace's Secrets")
  const cert = given['ca.crt'] || given['tls.crt'] || undefined
  if (!cert) return { problems: [...problems, 'keys.caSecret: holds no ca.crt or tls.crt'], warnings }
  try {
    const c = new X509Certificate(cert)
    if (!c.ca) problems.push("keys.caSecret: the issuer's certificate is not an authority")
    if (c.validFromDate.getTime() > now.getTime()) problems.push(`keys.caSecret: the issuer's certificate is not valid until ${c.validFromDate.toISOString()}`)
    if (c.validToDate.getTime() < now.getTime()) problems.push("keys.caSecret: the issuer's certificate has expired")
    else if (c.validToDate.getTime() - now.getTime() < ISSUER_WARN_DAYS * 86_400_000) warnings.push(`keys.caSecret: the issuer's certificate expires on ${c.validToDate.toISOString()}: give its successor before then`)
  } catch { problems.push("keys.caSecret: the issuer's certificate is not a certificate") }
  return { cert, problems, warnings }
}

/** Whether what is kept is whole and right: every problem, by entry, naming a property and never a value. Empty when all is well. */
export function keyProblems(kept: Readonly<Record<string, string>>, now: Date = new Date()): string[] {
  const out: string[] = []
  const has = (e: string) => typeof kept[e] === 'string' && kept[e] !== ''
  for (const e of ENTRIES) if (!has(e)) out.push(`${e}: absent`)
  if (has(AUTH_PRIVATE) && has(AUTH_PUBLIC)) {
    try { if (publicOf(kept[AUTH_PRIVATE]!).trim() !== kept[AUTH_PUBLIC]!.trim()) out.push(`${AUTH_PUBLIC}: is not the public half of ${AUTH_PRIVATE}`) } catch { out.push(`${AUTH_PRIVATE}: not a private key`) }
    for (const t of TOKENS) if (has(t.key) && verifiedScope(kept[t.key]!, kept[AUTH_PUBLIC]!) !== t.scope) out.push(`${t.key}: not signed by the kept key with scope ${t.scope}`)
  }
  if (has(CA_CERT)) {
    try {
      const cert = new X509Certificate(kept[CA_CERT]!)
      if (!cert.ca) out.push(`${CA_CERT}: not an authority`)
      if (new Date(cert.validTo).getTime() < now.getTime()) out.push(`${CA_CERT}: expired`)
      if (has(CA_KEY) && !cert.checkPrivateKey(createPrivateKey(kept[CA_KEY]!))) out.push(`${CA_KEY}: is not the key of ${CA_CERT}`)
    } catch { out.push(`${CA_CERT}: not a certificate`) }
  }
  return out
}
