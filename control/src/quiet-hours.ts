// Quiet hours: the windows an install declares in its Platform object (`spec.upgrades.quietHours`) in which no upgrade step and no drill
// starts, so that the install's own scheduled work (its jobs, its backups) never meets one. Restart hours (`spec.upgrades.restartHours`)
// have the same shape: the windows in which an awake database may be restarted onto a new compute image.
//
// A window is a start and an end on the clock of a time zone (IANA name, UTC by default), on the days listed (every day when none are).
// An end at or before the start runs past midnight: { start: '23:30', end: '00:30' } is an hour. The day is the start's day.

export const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const
export type Day = (typeof DAYS)[number]

export interface Window {
  /** `HH:MM`, 24-hour. */
  start: string
  /** `HH:MM`, 24-hour; at or before `start` means the window runs past midnight. */
  end: string
  /** An IANA time zone; UTC when absent. */
  timeZone?: string
  /** The days the window starts on; every day when absent or empty. */
  days?: Day[]
}

const HHMM = /^([01]\d|2[0-3]):([0-5]\d)$/
const minutesOf = (t: string): number => {
  const m = HHMM.exec(t)
  if (!m) throw new Error(`a time of day is HH:MM, not ${JSON.stringify(t)}`)
  return Number(m[1]) * 60 + Number(m[2])
}

/** A window as declared, checked: every fault is named, none is guessed at. */
export function checkWindow(w: unknown): Window {
  if (!w || typeof w !== 'object') throw new Error('a window is an object with start and end')
  const o = w as Record<string, unknown>
  if (typeof o['start'] !== 'string' || typeof o['end'] !== 'string') throw new Error('a window has a start and an end, each HH:MM')
  minutesOf(o['start'])
  minutesOf(o['end'])
  const timeZone = o['timeZone'] === undefined ? undefined : String(o['timeZone'])
  if (timeZone !== undefined) {
    try { new Intl.DateTimeFormat('en-US', { timeZone }) } catch { throw new Error(`${JSON.stringify(timeZone)} is not a time zone`) }
  }
  const days = o['days'] === undefined ? undefined : (o['days'] as unknown[])
  if (days !== undefined && (!Array.isArray(days) || days.some((d) => !(DAYS as readonly string[]).includes(String(d))))) throw new Error(`days are some of ${DAYS.join(', ')}`)
  return { start: o['start'], end: o['end'], ...(timeZone ? { timeZone } : {}), ...(days ? { days: days as Day[] } : {}) }
}

/** The windows of an environment value (a JSON array, as the operator writes it); empty or unset is none. */
export function parseWindows(text: string | undefined): Window[] {
  if (!text) return []
  const v = JSON.parse(text)
  if (!Array.isArray(v)) throw new Error('quiet hours are a JSON array of windows')
  return v.map(checkWindow)
}

/** The day of the week and the minute of the day of a moment, on a zone's clock. */
function localTime(at: Date, timeZone: string): { day: Day; minute: number } {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone, weekday: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).formatToParts(at)
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? ''
  return { day: get('weekday') as Day, minute: Number(get('hour')) * 60 + Number(get('minute')) }
}

/** Whether a moment is inside a window. */
export function inWindow(w: Window, at: Date): boolean {
  const { day, minute } = localTime(at, w.timeZone ?? 'UTC')
  const start = minutesOf(w.start), end = minutesOf(w.end)
  const onDay = (d: Day) => !w.days || w.days.length === 0 || w.days.includes(d)
  if (end > start) return onDay(day) && minute >= start && minute < end
  // past midnight: the evening part is on its own day, the morning part belongs to the day before
  const yesterday = DAYS[(DAYS.indexOf(day) + 6) % 7]!
  return (onDay(day) && minute >= start) || (onDay(yesterday) && minute < end)
}

/** The first window a moment is inside, or null. */
export const windowAt = (windows: Window[], at: Date): Window | null => windows.find((w) => inWindow(w, at)) ?? null

/** Whether work that starts at `from` and lasts `minutes` meets any window: the first window it meets and when, or null. Read minute by minute. */
export function meets(windows: Window[], from: Date, minutes: number): { window: Window; at: Date } | null {
  for (let m = 0; m <= Math.max(0, minutes); m++) {
    const at = new Date(from.getTime() + m * 60_000)
    const w = windowAt(windows, at)
    if (w) return { window: w, at }
  }
  return null
}

export const describeWindow = (w: Window): string => `${w.start} to ${w.end} ${w.timeZone ?? 'UTC'}${w.days?.length ? ` on ${w.days.join(', ')}` : ''}`
