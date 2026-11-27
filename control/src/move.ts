// The move of a database to a newer Postgres major version under its own name, address and login: its pure parts (the control layer,
// control.ts, runs them). Neon has no in-place major upgrade, so a move is a copy into a new timeline of the newer version, in the same
// tenant: every role but the platform's own (with its password's verifier, its attributes and its memberships), then every database the
// compute holds, each object made by its own owner, then a comparison of the two sides that never says equal when something was not
// copied. A real move shuts the database's door for the copy and then points its record at the new timeline; a rehearsal copies and compares
// with the door open and switches nothing. The old timeline stays, as a stopped database of its own, for the database's keepOldFor.
//
// How the copy reaches both sides: reading verifiers and making roles with them needs a superuser on each side, and the compute's only one
// (cloud_admin) logs in only over the pod's own 127.0.0.1. So the control layer makes a temporary superuser on the source (over the
// source's own socket, through an exec), and the target compute, made for the move, is the client: in it the target version's pg_dumpall and
// pg_dump read the source over TLS as that role, verified against the platform's authority, and its psql loads locally as cloud_admin. The
// dumps never pass through the control layer; the globals and the comparison's lines do (held in memory only: never logged or stored).

/** The prefix of the temporary role a move makes on its source. The gateways refuse a login of a user named so. */
export const MOVE_ROLE_PREFIX = 'peat_move_'
/** The roles that are the platform's own, never copied or compared: the compute's superuser and its group, and the platform's login (the
 * compute's spec makes it on both sides, with a verifier of its own salt). */
export const PLATFORM_ROLES = ['cloud_admin', 'neon_superuser', 'app'] as const
/** The schemas and tables the compute makes for itself in a database: not copied (the target's compute made its own) and not compared. */
export const PLATFORM_SCHEMAS = ['neon', 'neon_migration'] as const
export const PLATFORM_TABLES: Record<string, string[]> = { postgres: ['public.health_check'] }
/** The databases a compute makes itself: loaded into, never made again. */
export const COMPUTE_DATABASES = ['postgres', 'app'] as const
/** Where the move's script is mounted in the target compute, and the ConfigMap (in the compute namespace) the operator renders it in. */
export const MOVE_SCRIPT_PATH = '/move/copy.sh'
export const MOVE_SCRIPT_CONFIG_MAP = 'peat-move-script'
/** The ConfigMap (in the platform namespace) naming the pods a move may exec into: the exec guard's parameter. */
export const MOVE_EXECS_CONFIG_MAP = 'peat-move-execs'
/** The label on the move's target compute pod: the network rule that lets it reach the source on 5432 selects it. */
export const MOVE_TARGET_LABEL = 'peat-move'
/** The commands the exec guard allows, as an exec names them: psql, and bash with the move's script. */
export const MOVE_EXEC_COMMANDS = [['psql'], ['bash', MOVE_SCRIPT_PATH]] as const

/** The internal name of a move's target compute: `mv-` and hex, unlike any database a person names (they are checked to be free). */
export const targetNameOf = (hex: string) => `mv-${hex.slice(0, 12)}`
/** The name of the old copy a real move leaves: `<name>-pg<old version>`, cut to fit 31 characters. */
export const oldNameOf = (name: string, from: number) => `${name.slice(0, 31 - `-pg${from}`.length).replace(/-+$/, '')}-pg${from}`

const ident = (s: string) => `"${s.replace(/"/g, '""')}"`
const lit = (s: string) => `'${s.replace(/'/g, "''")}'`

// ---- the temporary role on the source

/** Makes the move's role on the source: superuser, a login of a few connections, valid for `minutes`; a role left by an earlier attempt is dropped first. */
export function makeRoleSql(role: string, password: string, minutes: number, leftovers: string[] = []): string {
  if (!role.startsWith(MOVE_ROLE_PREFIX) || !/^[a-z0-9_]+$/.test(role)) throw new Error(`not a move role: ${role}`)
  return [
    ...leftovers.map((r) => `drop role if exists ${ident(r)};`),
    `create role ${ident(role)} superuser login connection limit 4 password ${lit(password)} valid until ${lit(new Date(Date.now() + minutes * 60_000).toISOString())};`,
  ].join('\n') + '\n'
}
/** Renews the role's validity: the copy is bounded in short spans, not one long one. */
export const renewRoleSql = (role: string, minutes: number) => `alter role ${ident(role)} valid until ${lit(new Date(Date.now() + minutes * 60_000).toISOString())};\n`
/** Every move role on a compute (a leftover from an earlier attempt is found by its prefix). */
export const MOVE_ROLES_SQL = `select rolname from pg_roles where rolname like '${MOVE_ROLE_PREFIX}%' order by 1;\n`
/** Drops move roles, ending their sessions first. */
export const dropRolesSql = (roles: string[]) => roles.map((r) => `select pg_terminate_backend(pid) from pg_stat_activity where usename = ${lit(r)};\ndrop role if exists ${ident(r)};`).join('\n') + '\n'

// ---- the globals

const ROLE_NAME = String.raw`("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)`
const unquote = (r: string) => (r.startsWith('"') ? r.slice(1, -1).replace(/""/g, '"') : r)
/** Whether a role is the platform's or a move's: never copied. */
export const isPlatformRole = (r: string) => (PLATFORM_ROLES as readonly string[]).includes(r) || r.startsWith(MOVE_ROLE_PREFIX) || r.startsWith('pg_')

/**
 * The source's globals (pg_dumpall --globals-only) less every statement about a role that is the platform's or a move's: its CREATE ROLE,
 * its ALTER ROLE, its settings and a membership granted TO it. Every other line is kept as it is (a membership of a person's role in one of
 * the platform's, such as neon_superuser, included). A line that names a role in a way this does not read stops the move.
 */
export function filterGlobals(sql: string): { sql: string; roles: string[] } {
  const roles: string[] = []
  const out: string[] = []
  for (const line of sql.split('\n')) {
    const create = new RegExp(`^CREATE ROLE ${ROLE_NAME};$`).exec(line)
    const alter = new RegExp(`^ALTER ROLE ${ROLE_NAME} `).exec(line)
    const grant = new RegExp(`^GRANT ${ROLE_NAME} TO ${ROLE_NAME}[ ;]`).exec(line)
    if (create) { const r = unquote(create[1]!); if (isPlatformRole(r)) continue; roles.push(r); out.push(line); continue }
    if (alter) { if (isPlatformRole(unquote(alter[1]!))) continue; out.push(line); continue }
    // a membership of one of the platform's roles is the compute's own (the target's compute makes it again), except a person's role granted
    // to the platform's login (GRANT reporter TO app); a membership of a person's role is the person's, in whatever role (neon_superuser too)
    if (grant) { const [granted, grantee] = [unquote(grant[1]!), unquote(grant[2]!)]; if (isPlatformRole(grantee) && (grantee !== 'app' || isPlatformRole(granted))) continue; out.push(line); continue }
    if (/^(CREATE|ALTER|GRANT|REVOKE|DROP|COMMENT|SECURITY) /i.test(line) && !/^(SET|SELECT) /.test(line)) throw new Error(`the source's globals hold a statement the move does not read: ${line.split(' ').slice(0, 3).join(' ')}`)
    out.push(line)
  }
  return { sql: out.join('\n'), roles }
}

// ---- what "the same" means

const SYSTEM = `('pg_catalog', 'information_schema', 'pg_toast', ${PLATFORM_SCHEMAS.map(lit).join(', ')})`
/** The non-template databases of a compute, one a line. */
export const DATABASES_SQL = `select datname from pg_database where not datistemplate order by 1;\n`
/** Every role but the platform's and the moves', with its attributes, the md5 of its password's verifier (never the verifier) and its memberships. */
export const ROLES_SQL = `select 'role ' || rolname || ' super=' || rolsuper || ' inherit=' || rolinherit || ' createrole=' || rolcreaterole || ' createdb=' || rolcreatedb || ' login=' || rolcanlogin || ' replication=' || rolreplication || ' bypassrls=' || rolbypassrls || ' limit=' || rolconnlimit || ' verifier=' || coalesce(md5(rolpassword), '-') || ' until=' || coalesce(rolvaliduntil::text, '-') from pg_authid where rolname !~ '^pg_' and rolname not like '${MOVE_ROLE_PREFIX}%' and rolname not in (${PLATFORM_ROLES.map(lit).join(', ')}) order by rolname;
select 'member ' || m.rolname || ' in ' || g.rolname || ' admin=' || a.admin_option || ' inherit=' || a.inherit_option || ' set=' || a.set_option from pg_auth_members a join pg_roles m on m.oid = a.member join pg_roles g on g.oid = a.roleid where m.rolname !~ '^pg_' and m.rolname not like '${MOVE_ROLE_PREFIX}%' and m.rolname not in ('cloud_admin', 'neon_superuser') and not (m.rolname = 'app' and (g.rolname ~ '^pg_' or g.rolname in (${PLATFORM_ROLES.map(lit).join(', ')}))) order by 1;
`

/**
 * One database's fingerprint, read by psql in unaligned tuples-only mode (one `\gexec` per table): the database's owner and privileges;
 * every schema's owner and privileges; the default privileges; the extensions; every column, index and constraint; every table's owner,
 * privileges, row count and an md5 of its rows (each row as text, sorted by bytes, so either side's collation does not matter); every
 * sequence's last value; every view's and materialized view's definition (a materialized view's rows too); every partitioned table;
 * every composite, enum and domain type; functions and triggers by their definitions. An object of a kind it cannot read (a foreign table)
 * prints an `unsupported` line, which fails the comparison. Rows are never printed, only counts and hashes. The platform's own schemas and
 * tables are left out.
 */
export function fingerprintSql(database: string): string {
  const skip = (PLATFORM_TABLES[database] ?? []).map((t) => lit(t))
  const notSkipped = skip.length ? ` and (n.nspname || '.' || c.relname) not in (${skip.join(', ')})` : ''
  return `
select 'database ' || datname || ' owner=' || pg_get_userbyid(datdba) || ' acl=' || coalesce((select string_agg(a::text, ',' order by a::text) from unnest(datacl) a), '-') from pg_database where datname = current_database();
select 'schema ' || nspname || ' owner=' || pg_get_userbyid(nspowner) || ' acl=' || coalesce((select string_agg(a::text, ',' order by a::text) from unnest(nspacl) a), '-') from pg_namespace where nspname not in ${SYSTEM} and nspname !~ '^pg_' order by nspname;
select 'default-acl ' || pg_get_userbyid(defaclrole) || ' ' || coalesce(defaclnamespace::regnamespace::text, '-') || ' ' || defaclobjtype::text || ' ' || (select string_agg(a::text, ',' order by a::text) from unnest(defaclacl) a) from pg_default_acl order by 1;
select 'extension ' || extname from pg_extension where extname <> 'neon' order by 1;
select 'columns ' || count(*) || ' md5=' || coalesce(md5(string_agg(table_schema || '.' || table_name || '.' || column_name || ':' || data_type || ':' || coalesce(column_default, '-') || ':' || is_nullable, ',' order by table_schema, table_name, ordinal_position)), '-') from information_schema.columns where table_schema not in ${SYSTEM};
select 'indexes ' || count(*) || ' md5=' || coalesce(md5(string_agg(schemaname || '.' || indexdef, ',' order by schemaname, indexname)), '-') from pg_indexes where schemaname not in ${SYSTEM};
select 'constraints ' || count(*) || ' md5=' || coalesce(md5(string_agg(n.nspname || '.' || c.conname || ':' || pg_get_constraintdef(c.oid), ',' order by n.nspname, c.conname)), '-') from pg_constraint c join pg_namespace n on n.oid = c.connamespace where n.nspname not in ${SYSTEM};
select 'unsupported ' || n.nspname || '.' || c.relname || ' kind=' || c.relkind::text from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname not in ${SYSTEM} and n.nspname !~ '^pg_' and c.relkind not in ('r', 'i', 't', 'S', 'v', 'm', 'p', 'I', 'c') order by 1;
select 'view ' || n.nspname || '.' || c.relname || ' owner=' || pg_get_userbyid(c.relowner) || ' acl=' || coalesce((select string_agg(a::text, ',' order by a::text) from unnest(c.relacl) a), '-') || ' md5=' || md5(pg_get_viewdef(c.oid)) from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('v', 'm') and n.nspname not in ${SYSTEM} order by 1;
select 'partitioned ' || n.nspname || '.' || c.relname || ' owner=' || pg_get_userbyid(c.relowner) || ' key=' || pg_get_partkeydef(c.oid) from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'p' and n.nspname not in ${SYSTEM} order by 1;
select 'type ' || n.nspname || '.' || t.typname || ' kind=' || t.typtype::text || ' md5=' || md5(coalesce((select string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' order by a.attnum) from pg_attribute a where a.attrelid = t.typrelid and a.attnum > 0), '') || coalesce((select string_agg(e.enumlabel, ',' order by e.enumsortorder) from pg_enum e where e.enumtypid = t.oid), '') || coalesce(format_type(t.typbasetype, t.typtypmod), '')) from pg_type t join pg_namespace n on n.oid = t.typnamespace left join pg_class r on r.oid = t.typrelid where n.nspname not in ${SYSTEM} and (t.typtype in ('e', 'd') or (t.typtype = 'c' and r.relkind = 'c')) order by 1;
select 'function ' || n.nspname || '.' || p.proname || ' owner=' || pg_get_userbyid(p.proowner) || ' md5=' || md5(pg_get_functiondef(p.oid)) from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname not in ${SYSTEM} and p.prokind in ('f', 'p') order by 1;
select 'trigger ' || n.nspname || '.' || c.relname || '.' || t.tgname || ' md5=' || md5(pg_get_triggerdef(t.oid)) from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace where not t.tgisinternal and n.nspname not in ${SYSTEM} order by 1;
select 'sequence ' || schemaname || '.' || sequencename || ' last=' || coalesce(last_value::text, '-') from pg_sequences where schemaname not in ${SYSTEM} order by 1;
select format('select %L || '' owner='' || %L || '' acl='' || %L || '' rows='' || count(*) || '' md5='' || coalesce(md5(string_agg(t::text, E''\\n'' order by t::text collate "C")), ''-'') from %I.%I t', 'table ' || n.nspname || '.' || c.relname, pg_get_userbyid(c.relowner), coalesce((select string_agg(a::text, ',' order by a::text) from unnest(c.relacl) a), '-'), n.nspname, c.relname)
  from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r', 'm') and n.nspname not in ${SYSTEM} and n.nspname !~ '^pg_'${notSkipped} order by n.nspname, c.relname \\gexec
`
}

export function diff(a: string[], b: string[]): { onlyA: string[]; onlyB: string[] } {
  const setA = new Set(a), setB = new Set(b)
  return { onlyA: a.filter((l) => !setB.has(l)), onlyB: b.filter((l) => !setA.has(l)) }
}
const lines = (s: string) => s.split('\n').map((l) => l.trimEnd()).filter(Boolean)

export interface Sides { databases: [string, string]; roles: [string, string]; fingerprints: Record<string, [string, string]> }
export interface Compared { same: boolean; why: string; tables: number; rows: number; databases: number; roles: number }

/**
 * Whether the two sides are the same: the same databases, the same roles (attributes, verifiers, memberships), and every database's
 * fingerprint equal with no unsupported object. `why` names the first difference in a line (counts and names, never a row or a verifier).
 */
export function compareSides(s: Sides): Compared {
  const [dbA, dbB] = s.databases.map(lines) as [string[], string[]]
  let tables = 0, rows = 0
  const fail = (why: string): Compared => ({ same: false, why, tables, rows, databases: dbA.length, roles: 0 })
  const dd = diff(dbA, dbB)
  if (dd.onlyA.length || dd.onlyB.length) return fail(`the databases differ: only on the old side ${dd.onlyA.join(', ') || '-'}; only on the new ${dd.onlyB.join(', ') || '-'}`)
  const [rA, rB] = s.roles.map(lines) as [string[], string[]]
  const rd = diff(rA, rB)
  if (rd.onlyA.length || rd.onlyB.length) {
    const name = (l: string) => l.split(' ').slice(0, 2).join(' ')
    return fail(`the roles differ (their attributes, passwords or memberships): ${[...new Set([...rd.onlyA, ...rd.onlyB].map(name))].slice(0, 6).join(', ')}`)
  }
  for (const db of dbA) {
    const pair = s.fingerprints[db]
    if (!pair) return fail(`${db} was not compared`)
    const [a, b] = pair.map(lines) as [string[], string[]]
    const unsupported = [...a, ...b].filter((l) => l.startsWith('unsupported '))
    if (unsupported.length) return fail(`${db} holds what the move cannot copy and compare: ${[...new Set(unsupported.map((l) => l.slice('unsupported '.length).replace(' kind=f', ' (a foreign table)')))].slice(0, 5).join(', ')}`)
    const d = diff(a, b)
    if (d.onlyA.length || d.onlyB.length) {
      const what = (l: string) => l.replace(/ md5=\S+/, '').replace(/ rows=\d+/, '').slice(0, 120)
      return fail(`${db} differs: only on the old side ${d.onlyA.slice(0, 4).map(what).join(' | ') || '-'}; only on the new ${d.onlyB.slice(0, 4).map(what).join(' | ') || '-'}`)
    }
    for (const l of a) { const m = /^table \S+ .* rows=(\d+) md5=/.exec(l); if (m) { tables++; rows += Number(m[1]) } }
  }
  return { same: true, why: '', tables, rows, databases: dbA.length, roles: rA.filter((l) => l.startsWith('role ')).length }
}

// ---- the script the target compute runs

/**
 * The move's script, rendered by the operator into a ConfigMap of the compute namespace (which the control layer cannot write) and mounted
 * only in the move's target compute. Run as `bash /move/copy.sh <mode> <source address> <role> ...`, the role's password the first line of
 * stdin. Modes: `globals` (the source's roles, printed), `shell <db>` (a database the target does not have: made as on the source, with its
 * owner, privileges and settings), `load <db>` (its contents, each object made by its owner, into the target's database), `query <db>`
 * (the rest of stdin run on the source, printed). Loads run locally as cloud_admin; reads go to the source over TLS, verified against the
 * platform's authority.
 */
export const MOVE_SCRIPT = [
  '#!/bin/bash',
  "# The major-version move's copy, in the move's target compute (rendered by the Peat operator; see the control folder's src/move.ts).",
  'set -euo pipefail',
  'mode=$1; src=$2; role=$3',
  `[[ "$role" == ${MOVE_ROLE_PREFIX}* ]] || { echo "not a move role: $role" >&2; exit 2; }`,
  'IFS= read -r PGPASSWORD; export PGPASSWORD',
  'remote() { echo "host=$src port=5432 user=$role sslmode=verify-ca sslrootcert=/tls/ca.crt connect_timeout=10 dbname=$1"; }',
  'local_psql() { PGPASSWORD= psql -h 127.0.0.1 -U cloud_admin -d "$1" -X -q -o /dev/null -v ON_ERROR_STOP=1 -f -; }',
  'case "$mode" in',
  '  globals) pg_dumpall --globals-only --no-tablespaces -d "$(remote postgres)" ;;',
  "  shell) pg_dump -C --schema-only --exclude-schema='*' -d \"$(remote \"$4\")\" | local_psql postgres ;;",
  '  load)',
  `    skip=(${PLATFORM_SCHEMAS.map((x) => `--exclude-schema=${x}`).join(' ')} --exclude-extension=neon)`,
  ...Object.entries(PLATFORM_TABLES).map(([db, ts]) => `    [ "$4" = ${db} ] && skip+=(${ts.map((t) => `--exclude-table=${t}`).join(' ')})`),
  '    pg_dump --use-set-session-authorization "${skip[@]}" -d "$(remote "$4")" | local_psql "$4" ;;',
  '  query) psql -d "$(remote "$4")" -X -At -v ON_ERROR_STOP=1 -f - ;;',
  '  *) echo "unknown mode $mode" >&2; exit 2 ;;',
  'esac',
  '',
].join('\n')

// ---- what the database loop does about a move

export interface MoveStatus { from?: number; to?: number; rehearsal?: boolean; state?: string; request?: string; target?: string; toTimeline?: string; oldDatabase?: string; startedAt?: string; switchedAt?: string; deleteOldAfter?: string; seconds?: number; equal?: boolean; message?: string }
export type MoveAction =
  | { do: 'none' }
  | { do: 'start'; to: number; rehearsal: boolean; request: string }
  | { do: 'resume' }
  | { do: 'refuse'; why: string }

/** A rehearsal's request names the version: `19`, or `19@<anything>` (a new value asks again). */
export function rehearsalVersion(request: string | undefined): number | null {
  const m = /^(\d+)(?:@.*)?$/.exec(String(request ?? ''))
  return m ? Number(m[1]) : null
}

/**
 * What the move needs this pass, for a made database: resume one under way (a control layer that restarted), start one asked for (the
 * version raised, a rehearsal asked, a failed move asked again by requests.retryMove), or nothing. A move stays failed, or back, until it
 * is asked again.
 */
export function moveAction(o: { spec?: any; status?: any }, versions: readonly number[]): MoveAction {
  const s = o.status ?? {}, mv: MoveStatus = s.move ?? {}, req = o.spec?.requests ?? {}
  const running = Number(s.pgVersion ?? 0)
  if (mv.state && !['done', 'failed', 'back', 'refused'].includes(mv.state)) return { do: 'resume' }
  const want = Number(o.spec?.pgVersion ?? 0)
  const retry = req.retryMove && req.retryMove !== s.answered?.retryMove ? String(req.retryMove) : ''
  if (want > running) {
    if (!versions.includes(want)) return { do: 'refuse', why: `Postgres ${want} is not one this platform runs (${versions.join(', ')})` }
    const settled = mv.to === want && !mv.rehearsal && ['failed', 'back', 'refused'].includes(String(mv.state))
    if (!settled || retry) return { do: 'start', to: want, rehearsal: false, request: retry || `${want}` }
  }
  const rehearse = req.rehearseMove && req.rehearseMove !== s.answered?.rehearseMove ? String(req.rehearseMove) : ''
  if (rehearse) {
    const to = rehearsalVersion(rehearse)
    if (!to || to <= running) return { do: 'refuse', why: `requests.rehearseMove names a version above ${running} (19, or 19@<anything>), not ${JSON.stringify(rehearse)}` }
    if (!versions.includes(to)) return { do: 'refuse', why: `Postgres ${to} is not one this platform runs (${versions.join(', ')})` }
    return { do: 'start', to, rehearsal: true, request: rehearse }
  }
  return { do: 'none' }
}
