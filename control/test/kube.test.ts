import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fromKubeconfig } from '../src/kube'

// The kubeconfig as kubectl reads it: a list of files joined by ':', each name from the first file that has it.

let dir: string
afterEach(() => { if (dir) rmSync(dir, { recursive: true, force: true }) })
const yaml = (o: unknown) => JSON.stringify(o) // JSON is YAML

describe('a kubeconfig of several files', () => {
  test('the current context from the first file that sets one; its cluster and user from whichever file has them; where it acts named', () => {
    dir = mkdtempSync(join(tmpdir(), 'peat-kube-'))
    const a = join(dir, 'a'), b = join(dir, 'b')
    writeFileSync(a, yaml({ 'current-context': 'staging', contexts: [{ name: 'staging', context: { cluster: 'c2', user: 'u2', namespace: 'shop' } }] }))
    writeFileSync(b, yaml({ 'current-context': 'prod', clusters: [{ name: 'c2', cluster: { server: 'https://staging.example.com/' } }], users: [{ name: 'u2', user: { token: 't' } }] }))
    const k = fromKubeconfig({ path: `${a}:${join(dir, 'missing')}:${b}` })
    expect(k.namespace).toBe('shop')
    expect(k.where).toBe('context staging at https://staging.example.com')
  })
  test('a context whose namespace is empty reads as unset: the default namespace', () => {
    dir = mkdtempSync(join(tmpdir(), 'peat-kube-'))
    const a = join(dir, 'a')
    writeFileSync(a, yaml({ 'current-context': 'c', contexts: [{ name: 'c', context: { cluster: 'x', user: 'u', namespace: '' } }], clusters: [{ name: 'x', cluster: { server: 'https://x.example.com' } }], users: [{ name: 'u', user: { token: 't' } }] }))
    expect(fromKubeconfig({ path: a }).namespace).toBe('default')
  })
  test('none of the files there: refused, naming them', () => {
    dir = mkdtempSync(join(tmpdir(), 'peat-kube-'))
    expect(() => fromKubeconfig({ path: join(dir, 'x') })).toThrow('no kubeconfig at')
  })
})
