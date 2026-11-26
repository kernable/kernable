# How it works

kernable Cloud is not a database you send your data to. **Your kernel runs in your own network**,
next to your code. kernable Cloud keeps **every other instance of it in sync**, wherever it runs,
and **sees only sealed data** on the way.

<figure class="kn-how" role="img" aria-label="Your code and your kernel run in your network. Every change leaves it sealed, pushed out to a copy at kernable Cloud in the EU, which keeps backups and cannot open it. Kernels at your other sites, at the edge and in other regions follow that copy within seconds.">
<svg viewBox="0 0 800 300" aria-hidden="true">
  <defs>
    <marker id="kn-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" class="kn-fill-accent"/></marker>
  </defs>
  <!-- your network -->
  <rect x="10" y="40" width="230" height="220" rx="10" class="kn-zone kn-zone-you"/>
  <text x="26" y="66" class="kn-label">YOUR NETWORK</text>
  <rect x="34" y="90" width="182" height="44" rx="6" class="kn-box"/>
  <text x="125" y="117" class="kn-text" text-anchor="middle">your code</text>
  <rect x="34" y="166" width="182" height="54" rx="6" class="kn-box kn-box-strong"/>
  <path d="M112 136V163" class="kn-line" marker-end="url(#kn-arrow)"/>
  <path d="M138 164V137" class="kn-line" marker-end="url(#kn-arrow)"/>
  <text x="125" y="190" class="kn-text kn-bold" text-anchor="middle">your kernel</text>
  <text x="125" y="208" class="kn-small" text-anchor="middle">the source of your data</text>
  <text x="125" y="246" class="kn-small" text-anchor="middle">local reads and writes · fast</text>

  <!-- push -->
  <path d="M240 193H318" class="kn-line kn-line-accent" marker-end="url(#kn-arrow)"/>
  <text x="279" y="182" class="kn-small" text-anchor="middle">sealed</text>
  <text x="279" y="214" class="kn-small" text-anchor="middle">pushed out</text>

  <!-- kernable cloud -->
  <rect x="320" y="40" width="200" height="220" rx="10" class="kn-zone kn-zone-cloud"/>
  <text x="336" y="66" class="kn-label">KERNABLE CLOUD · EU</text>
  <g transform="translate(404 92)">
    <rect x="0" y="14" width="32" height="24" rx="4" class="kn-fill-accent"/>
    <path d="M7 14v-5a9 9 0 0 1 18 0v5" class="kn-line kn-line-accent" fill="none"/>
  </g>
  <rect x="344" y="150" width="152" height="40" rx="6" class="kn-box kn-box-sealed"/>
  <text x="420" y="175" class="kn-text kn-bold" text-anchor="middle">sealed copy</text>
  <rect x="344" y="198" width="152" height="30" rx="6" class="kn-box kn-box-sealed"/>
  <text x="420" y="218" class="kn-small" text-anchor="middle">backups every 5 s</text>
  <text x="420" y="248" class="kn-small" text-anchor="middle">no key to open it</text>

  <!-- follow -->
  <path d="M520 170C560 170 560 100 598 100" class="kn-line kn-line-accent" marker-end="url(#kn-arrow)"/>
  <path d="M520 180H598" class="kn-line kn-line-accent" marker-end="url(#kn-arrow)"/>
  <path d="M520 190C560 190 560 260 598 260" class="kn-line kn-line-accent" marker-end="url(#kn-arrow)"/>

  <!-- everywhere else -->
  <text x="600" y="66" class="kn-label">ANYWHERE · IN SECONDS</text>
  <rect x="600" y="78" width="190" height="44" rx="6" class="kn-box"/>
  <text x="695" y="98" class="kn-text" text-anchor="middle">your other site</text>
  <text x="695" y="114" class="kn-small" text-anchor="middle">a kernel next to its code</text>
  <rect x="600" y="158" width="190" height="44" rx="6" class="kn-box"/>
  <text x="695" y="178" class="kn-text" text-anchor="middle">the edge, a branch</text>
  <text x="695" y="194" class="kn-small" text-anchor="middle">reads locally</text>
  <rect x="600" y="238" width="190" height="44" rx="6" class="kn-box"/>
  <text x="695" y="258" class="kn-text" text-anchor="middle">another region</text>
  <text x="695" y="274" class="kn-small" text-anchor="middle">listeners, reports, search</text>
</svg>
</figure>

## Three things to know

### 1. Your kernel runs in your network

In your data center, your own cloud account, at the edge or on a laptop: wherever your code runs.
Your services read and write it locally, so both are fast, and your application keeps working
when the internet does not. Your kernel is the source of your data. It accepts no connection from
outside: it sends its changes out over a connection it opens itself.

### 2. kernable Cloud sees only sealed data

Every change leaves your network encrypted with a **copy key** that only your kernels hold. We
store it in the EU, back it up every five seconds and pass it on, without the key to open it.
Your **root key**, which decides who may do what, stays offline with you.

| kernable sees | kernable never sees |
|---|---|
| that a record arrived, its position and size | your records, events and their contents |
| hashes, to check that nothing was changed | personal data, files and secrets |
| how much you store and send, for the invoice | the copy key or your root key |
| your accounts, teams and projects | your code and your services |

### 3. Every other instance stays in sync, worldwide

A kernel at your other site, at the edge, in another region, or a listener that feeds reports or
search: each follows the sealed copy within seconds, by itself, and checks every record against
your kernel's signature. Writes sent to one of them travel back to your kernel. You write no sync
code and run no message broker or replication setup.

The writer never waits for the cloud: if kernable Cloud is unreachable, your application goes on,
and the copy catches up afterwards.

## Under the hood

### The parts

- **A writer per partition.** Each partition of your data has one kernel that writes, in your
  network. With a standby and a witness beside it, it fails over within its region by itself.
- **The pusher**, `kernable push`, runs beside the writer (a [node](/how-to/node)
  runs it for you with a `[push]` section). It follows the writer and sends every record out of
  your network to your copy at kernable Cloud, over a connection it opens itself.
- **The copy**, at a relay of kernable Cloud, one per writer, named `<team>.<project>.<partition>`
  (your team's slug: another team's project of the same name has copies of its own).
- **Readers**, read-only kernels anywhere, follow the copy, or the writer directly where they
  reach it. Writes sent to a reader travel back through the copy to the writer, and the reader
  answers once its own copy holds them.

### Sealing in detail

Everything that reaches kernable Cloud is encrypted with the copy key, which your pusher and your
readers hold and kernable does not: every record, every personal value, every file. kernable sees
positions, hashes and sizes, nothing else.

A relay cannot change a record: every reader checks each one against the writer's signed
checkpoints. It could only hold records back, and a reader that falls behind its maximum lag
refuses queries rather than answer from old data.

The copy key never passes through kernable Cloud: you give it to your instances yourself. The
relay lets in only the keys you name for each partition, while they are active, and kernable's
list of them opens nothing: a key it let in without the copy key would get sealed bytes, and
could not make a record your readers accept.

### Backups

kernable Cloud backs up each sealed copy in its own storage in the EU: the new parts every few
seconds and the whole copy daily, kept seven days. A restore is a new copy from them; your
readers follow it as before. The backups are sealed like the copy.

### Limits

A copy keeps everything its writer wrote; it is not compacted, since kernable cannot read it. Its
size is capped by your plan. At the cap, the copy stops following and its readers report
themselves not ready, until the plan or the cap changes; the writer is not affected.

### What stays yours

Your root key stays offline, with you. Your code, your services and your kernels' own stores are
in your infrastructure. kernable Cloud holds your accounts, teams and projects, the sealed copies
and their backups, and the usage it bills.

More on the open side: [relays](/how-to/relays),
[readers](/how-to/readers),
[what's open, what's kernable Cloud](/concepts/open-and-cloud).
