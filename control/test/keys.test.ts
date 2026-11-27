import { describe, expect, test } from 'bun:test'
import { X509Certificate, createPrivateKey, createPublicKey } from 'node:crypto'
import { connect, createServer } from 'node:tls'
import { AUTH_PRIVATE, AUTH_PUBLIC, CA_CERT, CA_KEY, ENTRIES, TOKENS, keyProblems, makeMissing, mintToken, newCa, newLeaf, plan, verifiedScope } from '../src/operator/keys'

describe('the keys an install gives none of', () => {
  test('made whole from nothing: the pair, seven tokens each of its scope, the authority; and nothing is left to make', () => {
    const made = makeMissing({})
    expect(Object.keys(made).sort()).toEqual([...ENTRIES].sort())
    expect(keyProblems(made)).toEqual([])
    for (const t of TOKENS) expect(verifiedScope(made[t.key]!, made[AUTH_PUBLIC]!)).toBe(t.scope)
    expect(makeMissing(made)).toEqual({})
  })
  test('a given private key is kept: its public half is derived and the tokens are signed by it', () => {
    const given = makeMissing({})
    const made = makeMissing({ [AUTH_PRIVATE]: given[AUTH_PRIVATE]! })
    expect(made[AUTH_PRIVATE]).toBeUndefined()
    expect(made[AUTH_PUBLIC]!.trim()).toBe(createPublicKey(createPrivateKey(given[AUTH_PRIVATE]!)).export({ format: 'pem', type: 'spki' }).toString().trim())
    expect(verifiedScope(made[TOKENS[0].key]!, given[AUTH_PUBLIC]!)).toBe(TOKENS[0].scope)
  })
  test('a half-made state is refused, never completed by replacing the kept half', () => {
    expect(() => plan(new Set([AUTH_PUBLIC]))).toThrow('without auth_private_key.pem')
    expect(() => plan(new Set([TOKENS[0].key]))).toThrow('without auth_private_key.pem')
    expect(() => plan(new Set([AUTH_PRIVATE, CA_CERT]))).toThrow('ca.crt is kept without ca.key')
  })
  test('a token signed by another key, or of another scope, is a problem', () => {
    const a = makeMissing({}), b = makeMissing({})
    expect(keyProblems({ ...a, [TOKENS[0].key]: b[TOKENS[0].key]! })).toEqual([`${TOKENS[0].key}: not signed by the kept key with scope ${TOKENS[0].scope}`])
    expect(keyProblems({ ...a, [TOKENS[0].key]: mintToken(a[AUTH_PRIVATE]!, 'admin') })).toHaveLength(1)
    expect(keyProblems({ ...a, [CA_KEY]: b[CA_KEY]! })).toEqual([`${CA_KEY}: is not the key of ${CA_CERT}`])
  })
})

describe('certificates', () => {
  test('the authority is a self-signed CA valid ten years', () => {
    const ca = newCa(new Date('2026-12-01T00:00:00Z'))
    const c = new X509Certificate(ca.certPem)
    expect(c.ca).toBe(true)
    expect(c.verify(c.publicKey)).toBe(true)
    expect(c.subject).toBe('CN=Peat platform CA')
    expect(new Date(c.validTo).getUTCFullYear()).toBe(2036)
  })
  test('a database certificate is signed by the authority, names the database and is for a server', () => {
    const ca = newCa()
    const leaf = newLeaf(ca, ['orders.peat-db.svc', 'orders.peat-db.svc.cluster.local', 'orders.db.example.com'])
    const c = new X509Certificate(leaf.certPem)
    expect(c.ca).toBe(false)
    expect(c.verify(new X509Certificate(ca.certPem).publicKey)).toBe(true)
    expect(c.checkHost('orders.db.example.com')).toBe('orders.db.example.com')
    expect(c.checkHost('orders.peat-db.svc')).toBe('orders.peat-db.svc')
    expect(c.checkHost('other.db.example.com')).toBeUndefined()
    expect(c.keyUsage).toContain('1.3.6.1.5.5.7.3.1')
    expect(c.checkPrivateKey(createPrivateKey(leaf.keyPem))).toBe(true)
  })
  test('a TLS client that trusts only the authority and checks the name accepts the database certificate (verify-full)', async () => {
    const ca = newCa()
    const leaf = newLeaf(ca, ['orders.db.example.com'])
    const server = createServer({ cert: leaf.certPem, key: leaf.keyPem }, (s) => s.end('ok'))
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()))
    const port = (server.address() as { port: number }).port
    const talk = (servername: string) => new Promise<string>((resolve, reject) => {
      const s = connect({ host: '127.0.0.1', port, servername, ca: ca.certPem, rejectUnauthorized: true }, () => {})
      let got = ''
      s.on('data', (d) => { got += d })
      s.on('end', () => resolve(got))
      s.on('error', reject)
    })
    expect(await talk('orders.db.example.com')).toBe('ok')
    await expect(talk('other.db.example.com')).rejects.toThrow()
    server.close()
  })
})
