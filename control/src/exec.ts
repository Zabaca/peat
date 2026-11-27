// A command run in a pod's container through the Kubernetes API (pods/exec), as kubectl exec runs one: a WebSocket of the
// v5.channel.k8s.io protocol, where each binary frame's first byte is its stream (0 stdin, 1 stdout, 2 stderr, 3 the exit status,
// 255 a stream closed). Stdin is written whole and then closed, so a program that reads it to its end (psql -f -) finishes. The major-version
// move is its one user; what it may run is bounded by an admission rule (render.ts, the exec guard), not here.

export const STDIN = 0, STDOUT = 1, STDERR = 2, STATUS = 3, CLOSE = 255

/** A frame of the protocol: its stream's byte, then the data. */
export function frame(stream: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(data.length + 1)
  out[0] = stream
  out.set(data, 1)
  return out
}

/** The exec's path: the pod, the container and the command (one `command` parameter per word, as the API takes it). */
export function execPath(namespace: string, pod: string, container: string, command: string[], stdin: boolean): string {
  const q = new URLSearchParams()
  q.set('container', container)
  for (const c of command) q.append('command', c)
  q.set('stdout', 'true')
  q.set('stderr', 'true')
  if (stdin) q.set('stdin', 'true')
  return `/api/v1/namespaces/${namespace}/pods/${pod}/exec?${q.toString()}`
}

/** The exit code an exec's status says: 0 for Success, the ExitCode cause for a non-zero exit, -1 for a failure without one. */
export function exitCodeOf(status: any): number {
  if (!status || status.status === 'Success') return 0
  const cause = (status.details?.causes ?? []).find((c: any) => c.reason === 'ExitCode')
  return cause ? Number(cause.message) : -1
}

export interface ExecResult { code: number; stdout: string; stderr: string; status?: any }

export interface ExecDeps {
  /** The API server's address (https://...). */
  server: string
  token: () => string
  ca?: string
}

/**
 * Runs `command` in the pod's container with `stdin` written and closed, and returns what it printed and its exit code. Bounded by
 * `timeoutMs`: past it the socket is closed and the result says so (code -1).
 */
export function exec(d: ExecDeps, namespace: string, pod: string, container: string, command: string[], stdin = '', timeoutMs = 600_000): Promise<ExecResult> {
  const url = d.server.replace(/^http/, 'ws') + execPath(namespace, pod, container, command, true)
  return new Promise((resolve) => {
    const out: Uint8Array[] = [], err: Uint8Array[] = []
    let status: any, done = false
    const finish = (code?: number) => {
      if (done) return
      done = true
      clearTimeout(timer)
      const dec = new TextDecoder()
      const text = (parts: Uint8Array[]) => dec.decode(Buffer.concat(parts.map((p) => Buffer.from(p))))
      resolve({ code: code ?? exitCodeOf(status), stdout: text(out), stderr: text(err), status })
    }
    const ws = new WebSocket(url, { headers: { authorization: `Bearer ${d.token()}` }, protocols: ['v5.channel.k8s.io'], tls: d.ca ? { ca: d.ca } : undefined } as any)
    ws.binaryType = 'arraybuffer'
    const timer = setTimeout(() => { err.push(new TextEncoder().encode(`\nthe exec did not end in ${Math.round(timeoutMs / 1000)} s`)); try { ws.close() } catch { /* closed */ } finish(-1) }, timeoutMs)
    ws.onopen = () => {
      const data = new TextEncoder().encode(stdin)
      // in pieces, so a large input is not one frame
      for (let i = 0; i < data.length; i += 32_768) ws.send(frame(STDIN, data.subarray(i, i + 32_768)))
      ws.send(new Uint8Array([CLOSE, STDIN]))
    }
    ws.onmessage = (ev) => {
      const b = new Uint8Array(ev.data as ArrayBuffer)
      if (b.length === 0) return
      const body = b.subarray(1)
      if (b[0] === STDOUT) out.push(body)
      else if (b[0] === STDERR) err.push(body)
      else if (b[0] === STATUS) { try { status = JSON.parse(new TextDecoder().decode(body)) } catch { status = { status: 'Failure', message: 'an unreadable status' } } }
    }
    ws.onerror = (ev: any) => { err.push(new TextEncoder().encode(`\nthe exec's connection failed: ${ev?.message ?? 'no detail'}`)); finish(status ? undefined : -1) }
    ws.onclose = (ev: CloseEvent) => {
      // a refusal before the exec starts (an admission rule, a missing pod) is an HTTP answer, given as the close reason
      if (!status && ev.reason) err.push(new TextEncoder().encode(`\n${ev.reason}`))
      finish(status ? undefined : -1)
    }
  })
}
