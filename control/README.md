# control: Peat's control layer, gateways, drills and operator

The program that runs a Peat platform on Kubernetes, beside the storage code it runs: one TypeScript program, run by Bun, in one image with
an entry per role. It is the program that has run a production Peat platform since 2026-11-20, brought here with every name of that install
made a setting, and the operator that installs and keeps a whole platform from one object.

| Part | What it is | Where |
|---|---|---|
| The control layer | Turns "a database" into Neon's terms (tenant, timeline, compute); starts and stops computes; replaces a compute or a safekeeper that is lost; repoints computes when the storage controller moves a tenant; trims old WAL from the store. Two pods, one acting at a time. | `src/control.ts`, with its pure decisions in `src/storage-spec.ts` |
| The gateways | The one front door: a client connects to `<database>.<domain>`, the gateway reads the name from TLS (without decrypting), wakes the database if it sleeps, and passes the bytes through. Puts a database to sleep after its idle time. | `src/gateway/` |
| The drills | Failure drills runnable against an install: pods cut, disks wiped, machines cut (by a command the installer supplies), the store unreachable, the controller's records restored. Every cutting step goes through one guard. | `src/drills/` |
| The operator | Reads the one Platform object and makes every piece of it: the namespaces, the keys and a certificate authority when none are given, the storage (pinned to machines by the spread rules), the control layer, the gateways and the admission rules that hold the control layer's account. Keeps each as declared, removes what it made and no longer renders, never touches what it did not make, and reports each piece and its release in the Platform's status. | `src/operator/` |
| The install file | The kinds, the operator's namespace and account, and its Deployment. | `src/install/install.ts` |
| The object kinds | `Platform` and `Database` in `peat.zabaca.com/v1alpha1`. | `src/api/v1alpha1.ts`, examples in `examples/` |
| The storage's checks | What an upgrade reads between steps (every node active, every shard attached, every secondary warm) and a pageserver's drain and fill. | `src/storage/` |
| Shared names | Namespaces, the machine label, the dead taint, the API group: neutral defaults, each a setting. | `src/names.ts` |

## Upgrades

Changing the Platform's `release` (and its `images`, for an install that pins them) moves the platform one piece at a time: the storage
controller and the broker; each pageserver, drained first (its shards moved to the others), restarted and filled again; each safekeeper; the
gateways; the control layer. The whole platform is checked before and after every step (every piece ready, every pageserver Active, every
shard attached, nothing reconciling, every secondary copy warm, every safekeeper answering). A check that does not pass within
`upgrades.checkTimeout` (a duration, `10m` by default) halts the upgrade with the step and the reason in the status (`peat status`); it stays halted until `peat retry`
(it goes on from that step) or until the earlier release is set back (only what had moved goes back, last first, with the same checks,
except that the check before each step back leaves out that step's own piece and its own storage readings: it is the broken piece the way
back mends). With two pieces broken at once the way back halts at the first it reaches: for the keepers this is the safe side (a write needs
two of three); the way out is `suspend: true` on the Platform and a fix by hand. The storage readings of every check, and a pageserver's
drain and fill, go through the storage controller, and the way back reaches the controller last: a controller that breaks after later
pieces moved halts the way back at the first of those it takes back. The way out is `suspend: true` on the Platform and the controller put
back on its earlier image by hand. Changing the target's images while an upgrade is under way
(the release kept, its images pinned anew to mend a bad one) takes back what moved and then runs the upgrade again to the new images. No
upgrade starts from pieces that do not all run the same images (one changed by hand while no release is recorded), nor from images no
release records (pieces without the operator's release stamp): the status says which, and nothing is recorded or started until they
agree (pinning `spec.images` to what runs records the release under the spec's name). A drain asked again when its node reads Active also
restarts one a person cancelled, until `upgrades.checkTimeout` halts it.
No step starts inside `upgrades.quietHours`: an upgrade under way waits at the next step boundary. A compute takes the new image at its
next start, or, awake, inside `upgrades.restartHours`, one at a time. The state is in `status.upgrade`, so a restarted operator goes on.

## What the operator does not take over

Adoption, taking over a platform installed another way, is not in this release: a Platform that names `adopt` or `requests.applyPending`
is refused. An existing object of a piece's name that the operator did not make is refused and left alone (the Platform reads Installing,
naming it); the operator's record of what it made, in its own namespace, decides what is its own, and it never applies over, nor deletes,
an object outside it. A lost record (its ConfigMap deleted, a cluster restored with new UIDs) has no way back in this release: every piece
is then refused and left alone, the status says the record was made new, and the recovery is by hand (the record rebuilt from the objects'
UIDs, or the objects removed so the operator makes them again). Once any piece runs, a changed release goes through the ordered steps,
from the images the live pieces run when no release has been recorded yet.

## Databases

A database is a Database object, in any namespace; its name is unique across the platform. The control layer makes the storage behind
it (a new database, a branch of `parent`, a restore of `parent` to the moment `at`, a replica of `replicaOf`) and keeps its record in
the object's status, with a copy in the platform namespace (a ConfigMap `db-<name>`) from which a lost status is written again (so a
Database object deleted without its finalizer and made again, while that ConfigMap is there, attaches to the same data); the
operator publishes, in the object's namespace, the Secret `<name>-login` (username, password, host, URL) and the ConfigMap `peat-ca`
(the authority's certificate, for `sslmode=verify-full`; not published when `certificates.issuer` is set and `keys.caSecret` is not,
since the platform's own authority then signs nothing), and the status names both. `spec.closed: true` refuses every connection until it
is false again; `spec.requests.stop` puts it to sleep now. A deleted database's compute goes at once and its data after the Platform's
`databases.deleteHold` (`never` keeps it; a hold is fixed when the database is deleted); a database made from another waits for its
branches to be deleted first, and the name can be used again at once. The control layer's HTTP API (`POST /db`, `/branch`, `/restore`,
`/replica`, `DELETE /db/<name>`) writes the same objects. `POST /records/copy` makes a Database object for every record that has none
(an install whose records are the control layer's older ConfigMaps, or a cluster whose objects were lost) and compares the two
(`?dry=1` compares only); `?held=<name>` first brings a deleted database back from its held data while the hold lasts.

When every safekeeper is lost (every member's disk, so no member holds a timeline's write-ahead log), no database can start: what
each one wrote and the pageservers uploaded to the store is kept; what was written and not yet uploaded is gone. The way on:

1. The control layer's replacement job makes a new member for each lost one (it does so before anything else), so three new members
   usually exist by the time a person looks; it then keeps the lost members and says "done with unknowns": it never accepts a total
   loss by itself.
2. A person checks that the loss is real before retiring anything: every lost member's machine has its data disk mounted where the
   Platform's safekeeper `hostPath` points (on Talos, `talosctl -n <machine> mounts` or `talosctl -n <machine> get mountstatus`;
   elsewhere `findmnt <path>` or `df <path>` on the machine). A member that restarted on an empty folder because its disk was not
   mounted is not lost: mount the disk and restart its pod, and it starts on its data (the replacement made meanwhile is then finished
   by the control layer, its timelines moved to the new member, as for any lost member that answers again).
3. The person retires each lost member, `POST /sk/<id>/decommission`. A timeline no member answers about counts as answered when
   every member its set lists carries a lost-disk mark or is already retired; one member alive, or only silent with no mark, keeps
   it unknown and refuses (`force=1` retires anyway). A retirement removes the member's StatefulSet and marks it retired in the
   storage controller; its folder on the disk stays, but no command brings that member back.
4. If every listed member is retired and no new one exists, the control layer seeds three.
5. Each database is recovered under its own name, `POST /db/<name>/recover` (`?dry=1` first says what it would do): a new timeline
   from the store's last upload, the same name, address and login.

## Installing

The control image is `ghcr.io/zabaca/peat/control`, published with the storage and compute images on every merge to main, tagged with
the commit (`.github/peat/control.Dockerfile`; one program, the role named by its first argument: `operator`, `control` or `gateway`).

A tag can be moved on the registry; a digest names one image. Each publish therefore also makes the release's image map (`images.json`:
the storage, control and both compute images, each `<image>:<tag>@sha256:<digest>`, the digest of what was just pushed and read back
under the tag) and its install file (`peat.yaml`) from it, kept as the run's `peat-install` artifact and attached to a `release-*` tag's
GitHub release (`.github/peat/image-map.sh`). That install file runs the operator by digest and hands it the map (`PEAT_RELEASE_IMAGES`):
a Platform of that release and registry then runs every image it does not pin by the map's digest, and its status records the digests.
An image the Platform pins in `images` comes first, and a Platform of another release or registry runs the tags (its status says the
map is of another release). A tag and its digest are one image: a platform already on the release by tags stays on them (no piece
restarts) and goes to digests at its next upgrade, and an upgrade by tags under way when the install file lands goes on, never back.
The usual order, the new release's install file first and then its `release` in the Platform, makes one upgrade, to the digests.

1. Apply the install file: `kubectl apply --server-side -f peat.yaml` with a release's own, or make one:
   `bun src/install/install.ts --image ghcr.io/zabaca/peat/control:<tag> | kubectl apply --server-side -f -` (add `--images images.json`
   to make it from an image map; `--image` may then be left out).
   It makes the platform's four namespaces and binds the operator's narrower rights in them: every right on Secrets there (elsewhere
   only read, made and patched: each database's login), exec in the compute namespace only (to grant it to a move); it deletes no
   namespace and changes no admission policy but its own. A Platform that names its own namespaces (`spec.namespaces`) needs the same
   names here: `--namespaces <platform>,<computes>,<gateways>,<databases>` (a release's own install file is made for the default ones);
   a Platform whose namespaces the install does not cover is Failed, its status naming them and the `--namespaces` to install with.
2. Label the machines the platform may use: `kubectl label node <node> peat.zabaca.com/machine=true` (three for the default spread; one
   for a trial with `spread: none`).
3. Put the store's Secret in the platform namespace (`peat` by default, made by the install file): `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY`.
4. Apply a Platform (`examples/platform.yaml`, or `examples/trial.yaml` for one machine) and read `kubectl get platform` until it says
   Ready.

`kubectl get platform` says Installing as soon as the Platform is taken up, and `kubectl get platform -o wide` (the Message column)
says what it waits for. What it needs from outside is checked first, and each is Failed with what to do, before any piece is made: no
machine labelled (the `kubectl label node` command); the store's Secret missing or without `AWS_ACCESS_KEY_ID` or
`AWS_SECRET_ACCESS_KEY` (the `kubectl create secret` command); with `records: postgres`, the records' Secret; with
`certificates.issuer`, cert-manager not installed or the issuer not there. While the platform is not Ready, the operator also makes one
request to the store for each bucket: a bucket the store does not have, or credentials it refuses, fail the Platform with the endpoint,
the bucket and the store's own error (never the key); a store that does not answer is only logged. A piece that is not ready says why
its pods are not: an image that cannot be pulled (the image and the pull's error, as for a `release` whose images are not published), a
container that cannot start or keeps failing, a pod that cannot be placed. `kubectl get database` shows each database's Host: the name
inside the cluster, or `<name>.<domain>` with `gateways.domain`.

`examples/try-it.sh` does the four steps above for a person trying Peat, on a throwaway kind cluster with a MinIO store (about two minutes), then makes a
database, writes and reads it with `psql`, puts it to sleep and wakes it by connecting. `.github/peat/install-test.sh` does it for CI, on one too (a one-machine trial with a MinIO store), then makes a
database, writes and reads it through the front door with `sslmode=verify-full`, puts it to sleep and wakes it by connecting, and has
the cluster's API server judge the admission rules as the control layer's account. CI runs it on every pull request (this change's
control image against main's published storage images) and on every merge (that run's images), and publishes nothing a failed run
built. It needs kind, kubectl, docker, bun and psql; locally, `KIND=<path to kind> .github/peat/install-test.sh <storage> <compute 18>
<compute 19> <control>` with the control image built into the local Docker.

On Talos the kubelet runs in a container of its own, with its own view of the machine's folders: another folder under `/var/lib`, and
the machine's own under `/var/mnt`, read-only. So a pageserver and a safekeeper mount their host directory as itself, never through
`subPath` (which the kubelet resolves in its view), and address their own folder inside it (`neon`, `sk-<id>`); with `create: true` the
directory is mounted unchecked, so the container runtime makes a missing one on the machine itself. Both the default paths under
`/var/lib/peat` and paths under `/var/mnt` (a disk Talos mounts there) then work with `create: true`, and an existing directory with
`create: false`.

A pageserver or a safekeeper whose folder is wiped while it runs is restarted by its liveness probe: a look every 10 s, 5 s for each,
three missed in a row, so 30 to 35 s (a stall of the disk shorter than that is not taken for a wipe). A pageserver then makes its folder
again and re-attaches its tenants from the controller; a safekeeper finds its folder empty, and as a registered member is refused by
the empty-disk guard, treated as lost and replaced. The first start's own probe of a safekeeper waits 90 s (its start may wait up to
60 s for the control layer's answer). A pageserver's and a safekeeper's start make the volume's root theirs (uid 1000), so a restart
can make the folder again.

The storage controller names itself to its peers by its pod's address (`http://$(POD_IP):1234`, from the downward API). On a cluster
whose pods have only IPv6 addresses that is not a valid URL (it needs brackets, which the downward API cannot add, and the controller
takes no address but a URL); such a cluster is not supported yet: it needs a wrapper that writes the address before the controller
starts.

Each piece's network rules let it reach only what it uses; the pieces that use the store (the pageservers, the safekeepers, the
storage controller with its records' copy, the control layer) reach any address on the store's port, less
`objectStore.egressExcept`. By default that is the private ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16,
100.64.0.0/10) when the endpoint is a public host, so a piece on a machine of a home or office network cannot reach that network's
devices on that port; and nothing when the endpoint is a cluster Service, a single-label or private name, or a private address, so an
in-cluster or private-network store keeps working. Set it for a public name that leads to a private address (`[]`), or to ranges of
your own. What it bounds: the pageservers and the safekeepers. The storage controller and the control layer also reach the API server
on 443 and 6443 at any address (a rule of their own, which a policy's rules add to), so with a store on 443 the private ranges stay
reachable for those two on that port until that rule is narrowed.

On upgrade an install with a public store has its store rule narrowed by these ranges; that breaks it only where a public name leads
to a private address (an S3 interface endpoint with private DNS, a split-horizon MinIO, a private gateway): set
`objectStore.egressExcept: []` there before upgrading.

## The objects

A whole install is one `Platform` (cluster-scoped, one per cluster) and its databases are `Database` objects (namespaced). Every field, its
meaning and its default is in `src/api/v1alpha1.ts`, which the CustomResourceDefinitions are rendered from, so `kubectl explain
platform.spec` and `kubectl explain database.spec` print the same text. In short:

- **Platform** `spec`: `release` (every piece runs it; changing it upgrades), `images`, `namespaces`, `machines` (the label that marks the
  platform's machines, their taint), `spread` (`machines`, or `none` for a one-machine trial), `objectStore` (one bucket and Secret, or one
  per piece), `pageservers`, `safekeepers` (three or five, their zone label, `outside` machines that may hold one member), `controller`
  (`records: kubernetes` or `postgres`), `gateways` (count, `domain`, how the front door is exposed), `certificates` (a cert-manager issuer,
  or the platform's own authority), `computes` (default Postgres version, `idleAfter`, memory), `databases.deleteHold`, `upgrades`
  (`quietHours`, `restartHours`, `checkTimeout`), `keys` (given, or made), `control`, `suspend`, `requests.retry` (`adopt` and
  `requests.applyPending` are refused: not in this release). Every duration is a number and a unit (`30m`, `7d`). Rules across fields (a namespace never changes; a
  version never lowered; `parent`, `at` and `replicaOf` fixed once made) are the API server's, in the CustomResourceDefinitions.
- **Database** `spec`: `pgVersion` (raising it moves the database to the newer major under its own name), `parent` and `at` (a branch, or
  a restore to a moment as a new database), `replicaOf`, `idleAfter`, `memory`, `closed`, `keepOldFor`, `requests.stop`,
  `requests.rehearseMove`. Its `status` says where to connect (`host`, `serviceHost`, `port`), the login Secret and the authority's
  certificate in the database's own namespace.

## Moving a database to a newer Postgres version

Raising a Database's `spec.pgVersion` (or `peat move <db> --pg 19`) moves it to the newer version under its own name, address, port and
login. Neon has no in-place major upgrade, so a move is a copy, in the control layer's background, with its state in `status.move`:

- A new timeline of the newer version is made in the database's own tenant, and a compute of its own (an internal name, `mv-...`) is
  started on it.
- The door is shut (`phase: Moving`: new connections are refused, open sessions ended) for the copy's length. The control layer waits
  12 s (two of a gateway's reads of its routes) before it stops the old compute and starts it again for the copy. A gateway whose read failed
  keeps the old route, so a client may still get in. Before the comparison every session on the old compute that is not the compute's own
  is ended (what it wrote is compared), and the old compute's counts of rows written are read (`pg_stat_database`). After it, a session
  still open, or a count that grew (a client that came, wrote and left), fails the move, and the old compute is stopped at once. What
  remains: a write that lands between that last look and the old compute's shutdown (the stop's calls to the API server, tens of
  milliseconds in the install test, then the kubelet's signal to the pod and Postgres's fast shutdown, not measured), and one by a session
  that ends at the very moment of the look, between leaving the list of sessions and its counts reaching the totals (microseconds). A
  control layer that restarts during the comparison starts the copy again; once `switching` is recorded the old compute was already
  stopped after the last look, and nothing starts it again while the door is shut.
- A temporary superuser, `peat_move_<hex>`, is made on the old compute. The new compute reads the old one over TLS as that role,
  verified against the platform's authority:
  - every role (its password's verifier, its attributes, its memberships, its settings), then every database (each object made by
    its own owner, and its settings and its roles' settings in it);
  - and loads it all locally.
- The copy stops at its first error. Then both sides are compared: the database list, the roles, the settings (of each database, each
  role, and each role in a database), every database's tables, views, sequences, functions, triggers, types, indexes, constraints,
  privileges and default privileges, and every table's row count. A database holding what the comparison cannot read (a foreign
  table) fails the move, and the message names it. That is a real move's check (`moveVerify: counts`, the default). With
  `moveVerify: full` (`peat move <db> --pg 19 --verify full`) it also compares every row, as a rehearsal always does: each row's hash,
  summed per table, so order does not matter and no table is too large for it. A row count costs a read of the table; a hash of every
  row costs that read and a hash of each row (on a Postgres 17, 3 million rows in memory: 30 ms counted, 2.4 s hashed).
- What a real move does not catch: a copy wrong in a way no count shows (a value changed in the copy, with the same number of rows) on a
  move made without a rehearsal or `--verify full`. The old copy and the way back remain for that. Rehearse first
  (`peat move <db> --pg 19 --rehearse`): it copies and compares every row with the door open, and switches nothing.
- Equal, the database's record is pointed at the new timeline and version, and the door opens. The old timeline is kept as a stopped
  database `<name>-pg<old>` for `keepOldFor` (30 days by default), then deleted, its data then held as any deleted database's.

A failed move switches nothing, opens the door and waits for `requests.retryMove` (`peat move <db> --retry`). A rehearsal
(`requests.rehearseMove: 19@<anything>`, `peat move <db> --pg 19 --rehearse`) copies and compares with the door open and switches
nothing; `status.move.seconds` is how long the copy took.

While the old copy is kept, `requests.moveBack` (`peat move <db> --back`) goes back: a return to the frozen copy the move left, which
nothing prevents. The door is shut and the database's compute stopped (its sessions ended); the version being left is kept as a
database of its own, `<name>-pg<newer>`, stopped, for `keepOldFor` from then; and the name is pointed at the old copy as it was at the
move. Nothing is read or compared, so it takes seconds whatever the size. It writes what it needs to finish (`state: going-back`)
before its first step, so a control layer that restarts in the middle goes on with it, each step safe to repeat; the door stays shut
until it is back. It costs what was written since the move: that is in
`<name>-pg<newer>` until its keep period ends (connect to it by its own name, with the database's login, to read it or copy from it by
hand). The status says so: back on Postgres N as of the move; what was written since is in `<name>-pg<newer>` until a time. It is
refused only when it cannot be done: no move to go back from (or one under way), the old copy or its timeline gone, or every name for
the newer copy taken. A second way back (moved, back, forward again, back again) keeps the version it leaves under the next free name,
`<name>-pg<newer>-2` (then `-3`, up to `-9`; a name any Database holds, made or not, is passed over), beside the earlier copy; each copy carries its own delete time
(`peat.zabaca.com/delete-after`), so the earlier one is still deleted when its keep period ends. A refusal is answered at once:
`peat move <db> --back` prints its reason and exits 1 (it is also the status's message, `status.move.backRefused`, and the control
layer's log). Asked while a move is under way, it says so and waits: the move runs to its end first. After a way back `spec.pgVersion` still names the
newer version (the API server refuses to lower it, the guard against an accidental downgrade) while `status.pgVersion` says what runs.
Moving forward again is a new move from what the database holds now, asked with `requests.retryMove` (`peat move <db> --retry`, or
`peat move <db> --pg <newer>`, which asks the same when the spec already names that version, as it does after a way back): it
copies the old version's database into a new timeline of the newer one, and does not take up `<name>-pg<newer>`, which stays, with its
own keep period, beside it. A
restore of a moved database to a moment before its move is refused, and names the old copy, which holds that history. A replica does
not move, nor a database that has replicas (delete them, move, make them again); branches stay on the old version.

What a move leaves out of the copy and the comparison, matched by exact name, never a prefix:
- the compute's own roles (`cloud_admin`, `neon_superuser`, the platform's login `app`, which each side's compute makes, and the move's
  own `peat_move_*`);
- the compute's own schemas `neon` and `neon_migration`, its extension `neon`, and its table `public.health_check` in the database
  `postgres`.

A person's role granted to `app`, and a person's role's membership in `neon_superuser`, are copied.

What guards it:
- The control layer's account holds no exec of its own. For a step of a move that needs one (the copy, the comparison, the switch; a
  way back needs none), it names the pods in the Database's `status.move.pods`; the operator, woken by its watch of the Databases, grants exec on
  exactly those of them that are the database's own, by a Role `peat-move-<database>` in the compute namespace naming them, and takes
  it back when the step ends, the move fails, or the Database goes. The control layer waits for the grant at most 60 s, then the move
  fails and goes on when asked again. A pod is the database's own when it exists, is not on its way out, is a compute, and is the
  database's compute or its move's target (a name `mv-<hex>` with the target label). A stale name (a compute restarted mid-move) names no pod and is never granted.
- The grant is taken back by the operator: at the end of each pass, and also by a pass that stops early (a Platform not valid, the
  keys, the placements). With the operator down (or the Platform suspended), a grant outlives its move until the operator's next pass;
  after a failed move the source compute runs on under the same name, so the control layer can still run psql there until then.
- A move under way on the old control layer while the platform is upgraded to this release fails safely: the new operator takes exec
  out of the control layer's Role before the move asks the new way. Ask again with `requests.retryMove` once the upgrade is done.
- What the control layer could still widen: it writes `status.move` (and so the target's name), and it makes the
  computes' Deployments (and so their labels). A control layer that misbehaves could name, or relabel, another database's compute as
  a move's target and be granted it. The check bounds a fault in the control layer (a pod of another database named by mistake is
  refused), not one that sets out to reach another database.
- One admission rule holds the shape, with no parameter: an exec, not an attach, only into a compute's container in the compute
  namespace, only `psql` or `bash /move/copy.sh` (the script the operator renders into the compute namespace, which the control layer
  cannot write). It judges no account but the control layer's. (An earlier release also read a list of pods from a ConfigMap as the
  rule's parameter; an API server's copy of such an object can go stale until it restarts, kubernetes#141015, so no rule reads one now.
  The operator removes that rule, its binding and its `peat-move-pods`; a `peat-move-execs` an earlier control layer or a person made is
  left behind, read by nothing, and may be deleted.)
- An upgrade from a release that had that rule removes its binding. On Kubernetes versions with kubernetes#141015 (before the v1.38
  fix), a policy left without a binding freezes the API server's copy of every ConfigMap used as a policy parameter, for every policy in
  the cluster, until the API server restarts. Affected: another policy of the cluster whose `paramKind` is a ConfigMap (or another
  built-in kind) and that reads a parameter changed after the upgrade. To tell: such a policy goes on judging by an older value of its
  ConfigMap, or answers "no params found" for one that exists. The policies it can touch are those with a `paramKind` (in
  `kubectl get validatingadmissionpolicies -o yaml`).
  Restarting the API server clears it. Peat itself reads no parameter, so it is not affected.
- The rule does not bound `psql`'s arguments, so in a granted pod the control layer may run any SQL as the local superuser, and so any
  shell command in that container (`\!`, `-o '|command'`, `COPY ... TO PROGRAM`).
- The rule is an admission policy and its binding: if either is missing (deleted, or a cluster without admission policies), RBAC alone
  bounds the control layer's exec, to the pods granted.
- The operator holds `pods/exec` in every namespace, only to grant it to the control layer (Kubernetes refuses a grant of a right the
  granter does not hold, and its check does not look at admission rules). A rule of the install file, `peat-operator-runs-no-exec`,
  refuses every exec and attach by the operator's account. Its reach: the operator may also change admission policies (it makes the
  guards), so the rule bounds a fault in the operator, not an operator that sets out to remove it.
- The temporary role is reachable through the gateways like any login (the front door passes TLS through and never sees a user's name).
  Its protection is its 256-bit random password, a validity of 10 minutes renewed every 3, a limit of four connections, and its drop on
  every exit: success, failure, and a control layer that restarted (the next attempt drops every `peat_move_` role it finds).
- The copy's password and the roles' verifiers are held in memory only, never logged or stored.
- Only a move's target compute may reach another compute's port.

## The command

`peat` (`src/cli/peat.ts`: `bun src/cli/peat.ts`, or the one-file program for Linux and macOS that CI builds on main and attaches,
with its SHA256SUMS, to each `release-*` tag), working through the user's own Kubernetes login on Database objects (so the cluster's
permissions decide who may do what; a token, a client certificate or an exec credential plugin, and a KUBECONFIG of several files, as
kubectl reads them), each change read back until the control layer has answered it:
`peat list`, `peat show <db>`, `peat make <db> [--pg 18|19]`, `peat branch <from> <db>`, `peat restore <from> <db> --at <time>`,
`peat replica <of> <db>`, `peat stop <db>`, `peat close <db>`, `peat open <db>`, `peat delete <db> --yes`, `peat url <db>`,
`peat ca [<db>]`, and for the platform `peat status` and `peat retry [<platform>]`. A database is looked up in the kubeconfig context's
namespace, or `-n`'s; `-A` looks in every namespace and refuses a name more than one holds. A delete and a close name the context and
server they act on.

## Settings

Each piece reads its settings from its environment, which the operator writes from the Platform; unset, each is the neutral default.
The names every piece shares are `NAMESPACE`, `COMPUTE_NAMESPACE`, `GATEWAY_NAMESPACE`, `DB_NAMESPACE`, `MACHINE_LABEL` (`key=value`),
`MACHINE_TAINT` and `DEAD_TAINT` (`key=value:Effect`), read by `src/names.ts`. The rest are read where they are used, at the top of each
program: `src/control.ts`, `src/gateway/settings.ts`, `src/drills/guard.ts` (the drills also take `--kubeconfig`, which `run` and `recover`
require), and the storage's scripts, `src/storage/api.ts` (`KEY_DIR`, the signing key's folder, `/keys` by default) and
`src/storage/roll.ts` (`ROLL_ACTION`, `ROLL_NODE`, `ROLL_WAIT_S`).

## Working here

```sh
cd control
bun install
bun test            # every test, the public-names check among them
bun run typecheck
```

Peat's CI runs the same on every pull request and every merge (the `control` job of `.github/workflows/peat-ci.yml`).

**Everything here is public.** Code, tests, comments and examples name no real person, company, host, address, bucket or record: hosts are
`example.com` and its kind, addresses are in the documentation ranges (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24), and machines are
`machine-1` and the like. `test/public-names.test.ts` fails on anything else it can recognise, in this folder and in the repository's
`.github` (`src/public-names.ts` says what it looks for and what it allows); the maintainer also runs a check with a private list of names before a change here merges. No em dashes or en dashes.
