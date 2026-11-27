import { describe, expect, test } from 'bun:test'
import { exitCodeOf, execPath, frame } from '../src/exec'
import { ROLES_SQL, SETTINGS_COPY_SQL, WRITES_SQL, compareSides, dropRolesSql, endOutsideSessionsSql, filterGlobals, fingerprintSql, makeRoleSql, moveAction, oldNameOf, outsideSessionsSql, rehearsalVersion, targetNameOf, writtenSince } from '../src/move'

// The move's pure parts: the globals filtered, the two sides compared, the decision of the database loop; and the exec's framing.

// pg_dumpall --globals-only of a compute, as Postgres 18 prints it (read 2026-11-27 from a compute of the install test; verifiers made up)
const GLOBALS = `--
-- PostgreSQL database cluster dump
--
\\restrict abc
SET default_transaction_read_only = off;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
--
-- Roles
--
CREATE ROLE app;
ALTER ROLE app WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:a$b:c';
CREATE ROLE cloud_admin;
ALTER ROLE cloud_admin WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;
CREATE ROLE lake_reader;
ALTER ROLE lake_reader WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:d$e:f';
CREATE ROLE "Lake Writer";
ALTER ROLE "Lake Writer" WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:g$h:i';
CREATE ROLE neon_superuser;
ALTER ROLE neon_superuser WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB NOLOGIN REPLICATION BYPASSRLS;
CREATE ROLE peat_move_1a2b;
ALTER ROLE peat_move_1a2b WITH SUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD 'SCRAM-SHA-256$4096:j$k:l' VALID UNTIL '2030-01-01 00:00:00+00';
--
-- User Configurations
--
ALTER ROLE lake_reader SET statement_timeout TO '30s';
ALTER ROLE peat_move_1a2b SET work_mem TO '64MB';
--
-- Role memberships
--
GRANT lake_reader TO "Lake Writer" WITH INHERIT TRUE GRANTED BY cloud_admin;
GRANT neon_superuser TO app WITH INHERIT TRUE GRANTED BY cloud_admin;
GRANT neon_superuser TO lake_reader WITH INHERIT TRUE GRANTED BY cloud_admin;
GRANT pg_monitor TO neon_superuser WITH ADMIN OPTION, INHERIT TRUE GRANTED BY cloud_admin;
\\unrestrict abc
`

describe('the globals', () => {
  test('every role but the platform\'s and the moves\' is kept, with its attributes, verifier, settings and memberships; the platform\'s are left out', () => {
    const f = filterGlobals(GLOBALS)
    expect(f.roles).toEqual(['lake_reader', 'Lake Writer'])
    for (const kept of ["ALTER ROLE lake_reader WITH NOSUPERUSER", 'CREATE ROLE "Lake Writer";', "ALTER ROLE lake_reader SET statement_timeout TO '30s';", 'GRANT lake_reader TO "Lake Writer"', 'GRANT neon_superuser TO lake_reader', 'SET client_encoding', '\\restrict abc']) expect(f.sql).toContain(kept)
    for (const left of ['CREATE ROLE app;', 'CREATE ROLE cloud_admin;', 'CREATE ROLE neon_superuser;', 'peat_move_1a2b', 'GRANT neon_superuser TO app', 'GRANT pg_monitor TO neon_superuser']) expect(f.sql).not.toContain(left)
    // a person's role granted to the platform's login is the person's, and kept
    expect(filterGlobals('GRANT reporter TO app WITH INHERIT TRUE GRANTED BY cloud_admin;').sql).toContain('GRANT reporter TO app')
  })
  test('a statement it does not read stops the move rather than pass unread', () => {
    expect(() => filterGlobals("CREATE TABLESPACE x LOCATION '/x';")).toThrow('a statement the move does not read: CREATE TABLESPACE x')
  })
})

describe('the temporary role', () => {
  test('a superuser login of four connections, valid for minutes; leftovers dropped first; only a move role', () => {
    const sql = makeRoleSql('peat_move_ab12', 'p', 10, ['peat_move_old'])
    expect(sql).toContain('drop role if exists "peat_move_old";')
    expect(sql).toContain(`create role "peat_move_ab12" superuser login connection limit 4 password 'p' valid until '`)
    expect(() => makeRoleSql('lake_writer', 'p', 10)).toThrow('not a move role')
    expect(dropRolesSql(['peat_move_ab12'])).toContain('pg_terminate_backend')
  })
})

describe('the door', () => {
  test('the sessions that got in after it shut are every client but the compute\'s own and the move\'s; ending them waits for each to go', () => {
    for (const sql of [outsideSessionsSql('peat_move_ab12'), endOutsideSessionsSql('peat_move_ab12')]) {
      expect(sql).toContain("backend_type = 'client backend' and pid <> pg_backend_pid() and usename is distinct from 'cloud_admin' and usename is distinct from 'peat_move_ab12'")
    }
    expect(endOutsideSessionsSql('')).toContain('pg_terminate_backend(pid, 5000)')
    // who, never what it runs
    expect(outsideSessionsSql('')).not.toContain('query')
  })
  test('a client that came, wrote one row and left during the comparison is seen in the write counts; reads and an unchanged count are not', () => {
    expect(WRITES_SQL).toContain('tup_inserted + tup_updated + tup_deleted')
    // the counts as pg_stat_database gives them (read from a Postgres 17, 2026-11-27: `-` is the shared catalogs' row)
    const before = '- 35\napp 50062\npostgres 0\ntemplate1 18426'
    expect(writtenSince(before, before)).toEqual([])
    expect(writtenSince(before, '- 35\napp 50063\npostgres 0\ntemplate1 18426')).toEqual(['app'])
    expect(writtenSince(before, '- 36\napp 50062\npostgres 0\ntemplate1 18426')).toEqual(['-'])
    // a database made since is a write too
    expect(writtenSince(before, `${before}\nnewdb 0`)).toEqual(['newdb'])
  })
})

describe('the settings (ALTER DATABASE ... SET, ALTER ROLE ... [IN DATABASE ...] SET)', () => {
  test('compared for every database and role but the compute\'s own, by name, as the md5 of each list', () => {
    expect(ROLES_SQL).toContain("select 'setting ' || coalesce(d.datname, '*') || '/' || coalesce(r.rolname, '*') || ' md5=' || md5(")
    expect(ROLES_SQL).toContain("r.rolname in ('cloud_admin', 'neon_superuser')")
  })
  test('copied for the databases the compute makes and for the platform login, which the dumps leave out; a list setting as it is held', () => {
    expect(SETTINGS_COPY_SQL).toContain("where (d.datname in ('postgres', 'app') or (s.setdatabase = 0 and r.rolname = 'app'))")
    expect(SETTINGS_COPY_SQL).toContain("'search_path'")
    expect(SETTINGS_COPY_SQL).toContain('quote_literal(')
  })
  test('a setting on one side only is never equal, said apart from the roles', () => {
    const roles = (extra: string): [string, string] => ['role lake_reader verifier=1\nsetting app/* md5=a', `role lake_reader verifier=1${extra}`]
    const sides = (r: [string, string]) => ({ databases: ['app', 'app'] as [string, string], roles: r, fingerprints: { app: ['', ''] as [string, string] } })
    expect(compareSides(sides(roles(''))).why).toBe("the settings differ (a database's, a role's or a role's in a database, as database/role): app/*")
    expect(compareSides(sides(roles('\nsetting app/* md5=b'))).same).toBe(false)
    expect(compareSides(sides(roles('\nsetting app/* md5=a'))).same).toBe(true)
  })
})

describe('names', () => {
  test('the target and the old copy', () => {
    expect(targetNameOf('0123456789abcdef')).toBe('mv-0123456789ab')
    expect(oldNameOf('orders', 18)).toBe('orders-pg18')
    expect(oldNameOf('a'.repeat(31), 18)).toBe(`${'a'.repeat(26)}-pg18`)
    expect(oldNameOf(`${'a'.repeat(25)}-bbbbb`, 18).length).toBeLessThanOrEqual(31)
  })
})

describe('the comparison', () => {
  const same = { databases: ['app\nlake\npostgres', 'app\nlake\npostgres'] as [string, string], roles: ['role lake_reader verifier=1', 'role lake_reader verifier=1'] as [string, string], fingerprints: { app: ['table public.t owner=app acl=- rows=3 md5=x', 'table public.t owner=app acl=- rows=3 md5=x'] as [string, string], lake: ['', ''] as [string, string], postgres: ['', ''] as [string, string] } }
  test('equal: the same databases, roles and fingerprints', () => {
    expect(compareSides(same)).toMatchObject({ same: true, tables: 1, rows: 3, databases: 3, roles: 1 })
  })
  test('a database or a role on one side only, a verifier changed, a row changed, a foreign table: never equal, said in a line', () => {
    expect(compareSides({ ...same, databases: ['app\nlake\npostgres', 'app\npostgres'] }).why).toBe('the databases differ: only on the old side lake; only on the new -')
    expect(compareSides({ ...same, roles: ['role lake_reader verifier=1\nrole lake_writer verifier=2', 'role lake_reader verifier=1'] }).why).toContain('role lake_writer')
    expect(compareSides({ ...same, roles: ['role lake_reader verifier=1', 'role lake_reader verifier=9'] }).same).toBe(false)
    expect(compareSides({ ...same, fingerprints: { ...same.fingerprints, app: ['table public.t owner=app acl=- rows=3 md5=x', 'table public.t owner=app acl=- rows=3 md5=y'] } }).why).toBe('app differs: only on the old side table public.t owner=app acl=- rows=3; only on the new table public.t owner=app acl=- rows=3')
    expect(compareSides({ ...same, fingerprints: { ...same.fingerprints, lake: ['unsupported public.remote kind=f', 'unsupported public.remote kind=f'] } }).why).toBe('lake holds what the move cannot copy and compare: public.remote (a foreign table)')
    const { lake: _, ...missing } = same.fingerprints
    expect(compareSides({ ...same, fingerprints: missing as any }).why).toBe('lake was not compared')
  })
  test('a real move\'s check is every table\'s row count; a full one adds a hash of every row, order-independent and of no size limit', () => {
    const counts = fingerprintSql('app', 'counts'), full = fingerprintSql('app', 'full')
    expect(counts).toContain("'' rows='' || count(*) from %I.%I t")
    expect(counts).not.toMatch(/hash=|string_agg\(t::text/)
    // each row's md5, its two halves summed as 64-bit numbers: nothing held but two sums (the old hash of every row joined stopped at 1 GB)
    expect(full).toContain("sum((''x'' || substr(md5(t::text), 1, 16))::bit(64)::bigint::numeric)")
    expect(full).toContain("sum((''x'' || substr(md5(t::text), 17, 16))::bit(64)::bigint::numeric)")
    expect(full).not.toContain('string_agg(t::text')
    expect(fingerprintSql('app')).toBe(full)
    // read from a Postgres 17 (2026-11-27): the same rows in another order give the same line; a changed, missing or doubled row does not
    const line = (rows: number, hash: string) => `table public.t owner=app acl=- rows=${rows} hash=${hash}`
    const same = line(3, '15824462171170473472/-15499744221373098623')
    const sides = (a: string, b: string) => ({ databases: ['app', 'app'] as [string, string], roles: ['', ''] as [string, string], fingerprints: { app: [a, b] as [string, string] } })
    expect(compareSides(sides(same, same))).toMatchObject({ same: true, tables: 1, rows: 3 })
    for (const other of [line(3, '18633034878675430594/-18494230842328794849'), line(2, '9416011521456614484/-12053845330010379241'), line(4, '22232912820884332460/-18945643112735818005')]) expect(compareSides(sides(same, other)).same).toBe(false)
    // the difference is said with its row counts, never its hash
    expect(compareSides(sides(same, line(2, '9416011521456614484/-12053845330010379241'))).why).toBe('app differs: only on the old side table public.t owner=app acl=- rows=3; only on the new table public.t owner=app acl=- rows=2')
    expect(compareSides(sides('table public.t owner=app acl=- rows=5', 'table public.t owner=app acl=- rows=5'))).toMatchObject({ same: true, rows: 5 })
  })
  test('the fingerprint reads every user schema and leaves the platform\'s own out', () => {
    const sql = fingerprintSql('postgres')
    expect(sql).toContain("not in ('pg_catalog', 'information_schema', 'pg_toast', 'neon', 'neon_migration')")
    expect(sql).toContain("not in ('public.health_check')")
    expect(fingerprintSql('app')).not.toContain('health_check')
    for (const k of ["'view '", "'partitioned '", "'type '", "'function '", "'trigger '", "'sequence '", "'unsupported '", '\\gexec']) expect(sql).toContain(k)
  })
})

describe('what the database loop does about a move', () => {
  const o = (spec: any, status: any) => ({ spec, status: { pgVersion: 18, ...status } })
  test('the version raised starts a move; one under way is resumed; a failed one waits for retryMove', () => {
    expect(moveAction(o({ pgVersion: 19 }, {}), [18, 19])).toEqual({ do: 'start', to: 19, rehearsal: false, request: '19' })
    expect(moveAction(o({ pgVersion: 19 }, { move: { state: 'copying' } }), [18, 19])).toEqual({ do: 'resume' })
    expect(moveAction(o({ pgVersion: 19 }, { move: { state: 'failed', to: 19 } }), [18, 19])).toEqual({ do: 'none' })
    expect(moveAction(o({ pgVersion: 19, requests: { retryMove: 'r1' } }, { move: { state: 'failed', to: 19 } }), [18, 19])).toEqual({ do: 'start', to: 19, rehearsal: false, request: 'r1' })
    expect(moveAction(o({ pgVersion: 19, requests: { retryMove: 'r1' } }, { move: { state: 'failed', to: 19 }, answered: { retryMove: 'r1' } }), [18, 19])).toEqual({ do: 'none' })
    expect(moveAction(o({ pgVersion: 20 }, {}), [18, 19])).toMatchObject({ do: 'refuse' })
  })
  test('a rehearsal names its version, and is answered once', () => {
    expect(rehearsalVersion('19@2026-12-01T00:00:00Z')).toBe(19)
    expect(rehearsalVersion('soon')).toBeNull()
    expect(moveAction(o({ requests: { rehearseMove: '19@a' } }, {}), [18, 19])).toEqual({ do: 'start', to: 19, rehearsal: true, request: '19@a' })
    expect(moveAction(o({ requests: { rehearseMove: '19@a' } }, { answered: { rehearseMove: '19@a' } }), [18, 19])).toEqual({ do: 'none' })
    expect(moveAction(o({ requests: { rehearseMove: '18@a' } }, {}), [18, 19])).toMatchObject({ do: 'refuse' })
  })
})

describe('the exec', () => {
  test('a frame is its stream\'s byte and the data; the path names the container and each word of the command', () => {
    expect([...frame(0, new Uint8Array([7, 8]))]).toEqual([0, 7, 8])
    expect(execPath('peat-compute', 'compute-x', 'compute', ['bash', '/move/copy.sh', 'globals'], true)).toBe('/api/v1/namespaces/peat-compute/pods/compute-x/exec?container=compute&command=bash&command=%2Fmove%2Fcopy.sh&command=globals&stdout=true&stderr=true&stdin=true')
  })
  test('the exit code of a status', () => {
    expect(exitCodeOf({ status: 'Success' })).toBe(0)
    expect(exitCodeOf({ status: 'Failure', details: { causes: [{ reason: 'ExitCode', message: '3' }] } })).toBe(3)
    expect(exitCodeOf({ status: 'Failure', message: 'x' })).toBe(-1)
  })
})
