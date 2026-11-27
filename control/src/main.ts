// The control image's entry: one program, one role per pod, named by the first argument.
//   operator   installs and keeps a platform from its Platform object (src/operator/)
//   control    the control layer (src/control.ts)
//   gateway    a gateway pod, behind the one front door (src/gateway/)
const roles: Record<string, string> = { operator: './operator/main.ts', control: './control.ts', gateway: './gateway/main.ts' }
const role = process.argv[2] ?? ''
if (!roles[role]) {
  console.error(`usage: main.ts <${Object.keys(roles).join('|')}>`)
  process.exit(2)
}
await import(roles[role]!)
export {}
