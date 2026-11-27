// How a live object differs from what the operator would make: the fields it sets, the annotations it sets (less its release stamp) and a
// Secret's values. The one user in this release is the remake of a Job whose template changed (a Job's template cannot change in place).
import { RELEASE_ANNOTATION } from './render'

/**
 * How `live` differs from what the operator would make (`want`), as the paths of every field the operator sets whose value is not the live
 * one; fields the operator does not set (defaults the API server filled in, another tool's fields) are not differences. The operator's own
 * labels are not either (they are its own to set).
 */
export function differences(want: unknown, live: unknown, path = ''): string[] {
  if (path === '.metadata.labels') return []
  // the annotations the operator sets, less its release stamp (which an upgrade moves on its own); another tool's are not its business
  if (path === '.metadata.annotations') return differences(Object.fromEntries(Object.entries((want ?? {}) as Record<string, unknown>).filter(([a]) => a !== RELEASE_ANNOTATION)), live ?? {}, '.metadata.annotations.')
  if (path === '.stringData') return []
  if (want === null || typeof want !== 'object') return JSON.stringify(want) === JSON.stringify(live) ? [] : [`${path || '.'}: ${JSON.stringify(live) ?? 'none'} would be ${JSON.stringify(want)}`.slice(0, 240)]
  // an empty list is what an API server does not store (its list fields are omitempty): absent live, it is no difference. Only a list: an
  // empty object can mean everything (a selector that selects every namespace), so it is always compared
  if ((live === undefined || live === null) && Array.isArray(want) && want.length === 0) return []
  if (Array.isArray(want)) {
    if (!Array.isArray(live) || live.length !== want.length) return [`${path}: ${Array.isArray(live) ? `${live.length} items` : JSON.stringify(live) ?? 'none'} would be ${want.length} items`]
    return want.flatMap((w, i) => differences(w, live[i], `${path}[${i}]`))
  }
  if (!live || typeof live !== 'object' || Array.isArray(live)) return [`${path}: none would be set`]
  // an empty object wanted is not a subset of a live one that has fields: `{}` selects everything where the live one selects some
  if (Object.keys(want as object).length === 0 && Object.keys(live as object).length > 0 && path !== '.metadata.annotations.') return [`${path}: ${JSON.stringify(live).slice(0, 120)} would be {}`]
  const out = Object.entries(want as Record<string, unknown>).flatMap(([k2, v]) => differences(v, (live as Record<string, unknown>)[k2], path.endsWith('.') ? `${path}${k2}` : `${path}.${k2}`))
  // a Secret's stringData is written as its data: each value compared, base64, with the live data's (never printed: a key's name only)
  if (path === '' && (want as any)?.stringData && typeof (want as any).stringData === 'object') {
    const data = ((live as any).data ?? {}) as Record<string, string>
    for (const [key2, v] of Object.entries((want as any).stringData as Record<string, string>)) if (Buffer.from(String(v)).toString('base64') !== data[key2]) out.push(`.data.${key2}: its value would change`)
  }
  return out
}
