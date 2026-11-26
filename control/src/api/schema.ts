// A small OpenAPI v3 schema, as a CustomResourceDefinition carries one, and a checker of a value against it. The API server checks an
// object against its kind's schema before the operator sees it; the operator and the command check too, so a value made in code (a default
// filled in, a request built by the command) is held to the same rules, and the tests can say what each rule refuses.

export interface Schema {
  type?: 'object' | 'array' | 'string' | 'integer' | 'number' | 'boolean'
  description?: string
  properties?: Record<string, Schema>
  required?: string[]
  items?: Schema
  additionalProperties?: Schema | boolean
  enum?: (string | number)[]
  pattern?: string
  minimum?: number
  maximum?: number
  minItems?: number
  maxItems?: number
  default?: unknown
  nullable?: boolean
  format?: string
  'x-kubernetes-preserve-unknown-fields'?: boolean
  'x-kubernetes-int-or-string'?: boolean
}

/** Every way `value` breaks `schema`, as `path: why`; empty when it fits. Unknown fields are findings unless the schema keeps them. */
export function check(schema: Schema, value: unknown, path = ''): string[] {
  const at = path || '(root)'
  if (value === null || value === undefined) return schema.nullable || value === undefined ? [] : [`${at}: may not be null`]
  if (schema['x-kubernetes-int-or-string']) {
    if (typeof value !== 'string' && !Number.isInteger(value)) return [`${at}: is an integer or a string`]
    return []
  }
  const errors: string[] = []
  switch (schema.type) {
    case 'object': {
      if (typeof value !== 'object' || Array.isArray(value)) return [`${at}: is an object`]
      const o = value as Record<string, unknown>
      for (const k of schema.required ?? []) if (o[k] === undefined) errors.push(`${path ? `${path}.` : ''}${k}: is required`)
      for (const [k, v] of Object.entries(o)) {
        const sub = schema.properties?.[k]
        if (sub) errors.push(...check(sub, v, path ? `${path}.${k}` : k))
        else if (typeof schema.additionalProperties === 'object') errors.push(...check(schema.additionalProperties, v, path ? `${path}.${k}` : k))
        else if (!schema.additionalProperties && !schema['x-kubernetes-preserve-unknown-fields']) errors.push(`${path ? `${path}.` : ''}${k}: is not a field`)
      }
      return errors
    }
    case 'array': {
      if (!Array.isArray(value)) return [`${at}: is a list`]
      if (schema.minItems !== undefined && value.length < schema.minItems) errors.push(`${at}: has at least ${schema.minItems} items`)
      if (schema.maxItems !== undefined && value.length > schema.maxItems) errors.push(`${at}: has at most ${schema.maxItems} items`)
      value.forEach((v, i) => { if (schema.items) errors.push(...check(schema.items, v, `${path}[${i}]`)) })
      return errors
    }
    case 'string':
      if (typeof value !== 'string') return [`${at}: is a string`]
      if (schema.pattern && !new RegExp(schema.pattern).test(value)) errors.push(`${at}: ${JSON.stringify(value)} does not match ${schema.pattern}`)
      break
    case 'integer':
      if (!Number.isInteger(value)) return [`${at}: is a whole number`]
      break
    case 'number':
      if (typeof value !== 'number') return [`${at}: is a number`]
      break
    case 'boolean':
      if (typeof value !== 'boolean') return [`${at}: is true or false`]
      break
  }
  if (schema.enum && !schema.enum.includes(value as string | number)) errors.push(`${at}: is one of ${schema.enum.join(', ')}`)
  if (typeof value === 'number' && schema.minimum !== undefined && value < schema.minimum) errors.push(`${at}: is at least ${schema.minimum}`)
  if (typeof value === 'number' && schema.maximum !== undefined && value > schema.maximum) errors.push(`${at}: is at most ${schema.maximum}`)
  return errors
}

/** The value with every default of the schema filled in where the value has none (objects only; a list's items are filled each). */
export function withDefaults<T>(schema: Schema, value: T): T {
  if (value === undefined && schema.default !== undefined) value = structuredClone(schema.default) as T
  if (schema.type === 'object' && value && typeof value === 'object' && !Array.isArray(value)) {
    const o = { ...(value as Record<string, unknown>) }
    for (const [k, sub] of Object.entries(schema.properties ?? {})) {
      const filled = withDefaults(sub, o[k])
      if (filled !== undefined) o[k] = filled
    }
    return o as T
  }
  if (schema.type === 'array' && Array.isArray(value) && schema.items) return value.map((v) => withDefaults(schema.items!, v)) as T
  return value
}

// ---- builders, so the kinds read as their fields
export const str = (description: string, more: Partial<Schema> = {}): Schema => ({ type: 'string', description, ...more })
export const int = (description: string, more: Partial<Schema> = {}): Schema => ({ type: 'integer', description, ...more })
export const bool = (description: string, more: Partial<Schema> = {}): Schema => ({ type: 'boolean', description, ...more })
export const list = (description: string, items: Schema, more: Partial<Schema> = {}): Schema => ({ type: 'array', description, items, ...more })
export const obj = (description: string, properties: Record<string, Schema>, more: Partial<Schema> = {}): Schema => ({ type: 'object', description, properties, ...more })
export const map = (description: string, values: Schema = { type: 'string' }): Schema => ({ type: 'object', description, additionalProperties: values })
