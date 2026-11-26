// The CustomResourceDefinitions of Peat's two kinds, rendered from v1alpha1.ts: what the install file applies first.
import { GROUP, KINDS, VERSION } from './v1alpha1'
import type { Schema } from './schema'

/** The schema as the API server takes it: the checker's own markers left out. */
const structural = (s: Schema): Schema => {
  const out: Schema = { ...s }
  if (s.properties) out.properties = Object.fromEntries(Object.entries(s.properties).map(([k, v]) => [k, structural(v)]))
  if (s.items) out.items = structural(s.items)
  if (typeof s.additionalProperties === 'object') out.additionalProperties = structural(s.additionalProperties)
  if (s.additionalProperties === false || s.additionalProperties === true) delete out.additionalProperties
  return out
}

const COLUMNS = {
  Platform: [
    { name: 'Release', type: 'string', jsonPath: '.spec.release' },
    { name: 'Running', type: 'string', jsonPath: '.status.release' },
    { name: 'Phase', type: 'string', jsonPath: '.status.phase' },
    { name: 'Message', type: 'string', jsonPath: '.status.message', priority: 1 },
    { name: 'Age', type: 'date', jsonPath: '.metadata.creationTimestamp' },
  ],
  Database: [
    { name: 'Postgres', type: 'integer', jsonPath: '.status.pgVersion' },
    { name: 'Phase', type: 'string', jsonPath: '.status.phase' },
    { name: 'Awake', type: 'boolean', jsonPath: '.status.awake' },
    { name: 'Host', type: 'string', jsonPath: '.status.host' },
    { name: 'Parent', type: 'string', jsonPath: '.spec.parent', priority: 1 },
    { name: 'Age', type: 'date', jsonPath: '.metadata.creationTimestamp' },
  ],
}

export function crds(): Record<string, unknown>[] {
  return (Object.entries(KINDS) as [keyof typeof KINDS, (typeof KINDS)[keyof typeof KINDS]][]).map(([kind, k]) => ({
    apiVersion: 'apiextensions.k8s.io/v1',
    kind: 'CustomResourceDefinition',
    metadata: { name: `${k.plural}.${GROUP}` },
    spec: {
      group: GROUP,
      scope: k.scope,
      names: { kind, plural: k.plural, singular: k.singular, shortNames: k.shortNames, categories: ['peat'] },
      versions: [{
        name: VERSION,
        served: true,
        storage: true,
        subresources: { status: {} },
        additionalPrinterColumns: COLUMNS[kind],
        schema: { openAPIV3Schema: { type: 'object', description: k.spec.description, required: ['spec'], properties: { apiVersion: { type: 'string' }, kind: { type: 'string' }, metadata: { type: 'object' }, spec: structural(k.spec), status: structural(k.status) } } },
      }],
    },
  }))
}
