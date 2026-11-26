import { describe, expect, test } from 'bun:test'
import { checkWindow, describeWindow, inWindow, meets, parseWindows } from '../src/quiet-hours'

describe('quiet hours', () => {
  test('a window on the clock of its zone: inside from its start, outside at its end', () => {
    const w = { start: '02:00', end: '03:00', timeZone: 'America/New_York' }
    // 2026-12-01 is a Tuesday; New York is UTC-5 in December
    expect(inWindow(w, new Date('2026-12-01T07:00:00Z'))).toBe(true)
    expect(inWindow(w, new Date('2026-12-01T07:59:00Z'))).toBe(true)
    expect(inWindow(w, new Date('2026-12-01T08:00:00Z'))).toBe(false)
    expect(inWindow(w, new Date('2026-12-01T06:59:00Z'))).toBe(false)
  })
  test('UTC when no zone is named', () => {
    expect(inWindow({ start: '05:00', end: '05:30' }, new Date('2026-12-01T05:10:00Z'))).toBe(true)
  })
  test('an end at or before the start runs past midnight, and the morning part belongs to the day before', () => {
    const w = { start: '23:30', end: '00:30', days: ['Tue' as const] }
    expect(inWindow(w, new Date('2026-12-01T23:45:00Z'))).toBe(true) // Tuesday evening
    expect(inWindow(w, new Date('2026-12-02T00:15:00Z'))).toBe(true) // Wednesday morning, Tuesday's window
    expect(inWindow(w, new Date('2026-12-03T00:15:00Z'))).toBe(false) // Thursday morning: Wednesday has no window
  })
  test('days: only the days listed', () => {
    const w = { start: '00:00', end: '23:59', days: ['Sat' as const, 'Sun' as const] }
    expect(inWindow(w, new Date('2026-12-05T12:00:00Z'))).toBe(true) // Saturday
    expect(inWindow(w, new Date('2026-12-01T12:00:00Z'))).toBe(false) // Tuesday
  })
  test('work that would meet a window is found, with when; work that ends before it is not', () => {
    const ws = [{ start: '05:00', end: '06:00' }]
    expect(meets(ws, new Date('2026-12-01T04:00:00Z'), 30)).toBeNull()
    expect(meets(ws, new Date('2026-12-01T04:45:00Z'), 30)?.at.toISOString()).toBe('2026-12-01T05:00:00.000Z')
    expect(meets([], new Date('2026-12-01T05:10:00Z'), 30)).toBeNull()
  })
  test('a window is checked: a bad time, a zone that is not one and a day that is not one are refused; the environment form is a JSON list', () => {
    expect(() => checkWindow({ start: '5:00', end: '06:00' })).toThrow('HH:MM')
    expect(() => checkWindow({ start: '05:00', end: '06:00', timeZone: 'Mars/Olympus' })).toThrow('not a time zone')
    expect(() => checkWindow({ start: '05:00', end: '06:00', days: ['Funday'] })).toThrow('days are')
    expect(parseWindows(undefined)).toEqual([])
    expect(parseWindows('[{"start":"05:00","end":"06:00","timeZone":"UTC"}]')).toEqual([{ start: '05:00', end: '06:00', timeZone: 'UTC' }])
    expect(() => parseWindows('{}')).toThrow('JSON array')
    expect(describeWindow({ start: '05:00', end: '06:00', days: ['Mon'] })).toBe('05:00 to 06:00 UTC on Mon')
  })
})
