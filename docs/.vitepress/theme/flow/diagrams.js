/* Animated figure definitions for the kernable docs.
   Each entry: { w, h, intro, legend, build(g), steps(g) }
   Coordinates target the docs content column (~740 units wide);
   the command overview is wider and meant for the home page.
   Every caption restates what the concept pages say; keep them in step. */

import { icons } from './icons.js'

export const diagrams = {}

/* ------------------------------------------------------------------
   hero — home page: kernable in one loop
------------------------------------------------------------------ */
diagrams.hero = {
  w: 760, h: 300,
  intro: 'Your code decides, in any language. kernable checks, orders and stores — and every read is up to date.',
  legend: ['cmd', 'evt', 'ok', 'kern'],
  build (g) {
    // Three rows; each of your processes faces the part of kernable it talks to, so every wire
    // is one straight line.
    const row = (i) => 66 + i * 76 // top of row i; rows are 56 high, centers at row(i) + 28
    g.zone(10, 14, 300, 272, 'Your code · any language')
    // For example; any language with gRPC can take part.
    g.icon(28, 37, 18, icons.typescript)
    g.icon(56, 37, 18, icons.python)
    g.icon(84, 37, 18, icons.rust)
    g.node('client', { x: 28, y: row(0), w: 264, h: 56, label: 'Client', sub: 'submits commands, asks queries', kind: 'cmd' })
    g.node('worker', { x: 28, y: row(1), w: 264, h: 56, label: 'Worker', sub: 'runs your aggregates: decides', kind: 'cmd' })
    g.node('views', { x: 28, y: row(2), w: 264, h: 56, label: 'Read models · reactors', sub: 'answer queries, react to events', kind: 'qry' })
    // The logo names this side, in place of a heading.
    g.zone(410, 14, 340, 272, null, 'var(--vp-c-brand-1)')
    g.image(426, 26, 99, 25, 'logo-nav.png')
    g.node('checks', { x: 428, y: row(0), w: 304, h: 56, label: 'Checks', sub: 'signature · grant · middleware', kind: 'kern' })
    g.node('queue', { x: 428, y: row(1), w: 304, h: 56, label: 'Execute', sub: 'on a worker · one per aggregate at a time', kind: 'kern' })
    // The log: four events, each chained to the one before by its hash.
    for (let i = 0; i < 4; i++) {
      const x = 428 + i * 80
      g.node(`e${i}`, { x, y: row(2), w: 64, h: 56, label: `#${39 + i}`, sub: i === 3 ? 'new' : 'event', kind: 'evt', shape: 'store', hidden: i === 3 })
      if (i > 0) g.wire(`link${i}`, [[x - 16, row(2) + 28], [x, row(2) + 28]], { arrow: false, dashed: true, hidden: i === 3 })
    }
    g.wire('in', [g.port('client', 'right'), g.port('checks', 'left')])
    g.wire('worker', [g.port('queue', 'left'), g.port('worker', 'right')])
    g.wire('views', [g.port('e0', 'left'), g.port('views', 'right')])
    g.wire('queued', [g.port('checks', 'bottom'), g.port('queue', 'top')])
    g.wire('commit', [g.port('queue', 'bottom', 120), g.port('e3', 'top')])
  },
  steps () {
    const check = (text) => ({ say: 'kernable checks it before anything happens.', hot: ['checks'], run: (g) => g.setSub('checks', text), pause: 600, after: (g) => g.setSub('checks', text) })
    return [
      { say: 'Your code sends a command — from any language, signed with its own key.',
        hot: ['client', 'checks'], packets: [['in', { kind: 'cmd', label: 'PlaceOrder', dur: 900 }]] },
      check('✓ signature'),
      check('✓ signature  ✓ grant'),
      check('✓ signature  ✓ grant  ✓ middleware'),
      { say: 'It is stored, then executed on a worker: one command per aggregate at a time, so nothing overtakes.',
        hot: ['checks', 'queue'], packets: [['queued', { kind: 'cmd', dur: 500 }]] },
      { say: 'A worker decides: your business logic returns the events.',
        hot: ['queue', 'worker'],
        packets: [['worker', { kind: 'cmd', label: 'PlaceOrder', dur: 700 }], ['worker', { kind: 'evt', label: 'OrderPlaced', dur: 700, reverse: true, delay: 850 }]] },
      { say: 'The event joins one ordered log, chained to the one before and signed.',
        show: ['e3', 'link3'], hot: ['queue', 'e3'], packets: [['commit', { kind: 'evt', dur: 600 }]],
        after: (g) => g.setSub('e3', 'Placed') },
      { say: 'Read models and reactors get it in order, and confirm.',
        hot: ['e0', 'e1', 'e2', 'e3', 'views'],
        packets: [['views', { kind: 'evt', label: '#42', dur: 700 }], ['views', { kind: 'ok', label: 'ack', dur: 600, reverse: true, delay: 800 }]] },
      { say: 'Then the submit returns: every read is already up to date. No polling, no "eventually".',
        hot: ['checks', 'client'], packets: [['in', { kind: 'ok', label: 'settled ✓', dur: 900, reverse: true }]] }
    ]
  }
}

/* ------------------------------------------------------------------
   architecture — a still picture: your processes, the kernel, its stores
------------------------------------------------------------------ */
diagrams.architecture = {
  w: 800, h: 372,
  intro: 'Your processes hold the business logic, in any language. The kernel checks, executes, orders and stores, and keeps every store.',
  legend: [],
  build (g) {
    const mid = (i) => 76 + i * 62 // the rows' centers; each side lines up with the other
    g.zone(10, 14, 200, 222, 'Your code')
    g.icon(118, 22, 16, icons.typescript)
    g.icon(142, 22, 16, icons.python)
    g.icon(166, 22, 16, icons.rust)
    const yours = [
      ['clients', 'Clients', 'commands, queries', 'cmd'],
      ['workers', 'Workers', 'run aggregates: decide', 'cmd'],
      ['listeners', 'Listeners', 'views · reactors · checks', 'qry']
    ]
    yours.forEach(([id, label, sub, kind], i) => g.node(id, { x: 22, y: mid(i) - 27, w: 176, h: 54, label, sub, kind }))

    g.zone(300, 14, 260, 344, null, 'var(--vp-c-brand-1)')
    g.image(314, 22, 80, 20, 'logo-nav.png')
    const kernel = [
      ['checks', 'Checks', 'signatures · grants · middleware'],
      ['execute', 'Execute', 'one per aggregate, on a worker'],
      ['log', 'Log', 'commit · hash chain · order'],
      ['services', 'Services', 'memberships · readiness'],
      ['ui', 'UI', 'map · connections · history']
    ]
    kernel.forEach(([id, label, sub], i) => g.node(id, { x: 314, y: mid(i) - 25, w: 232, h: 50, label, sub, kind: 'kern' }))

    g.zone(610, 14, 180, 344, 'Stores')
    const stores = [
      ['events', 'Events · commands', 'the signed log', 'evt'],
      ['snapshots', 'Snapshots', 'state cache', 'kern'],
      ['vault', 'Vault', 'personal, encrypted', 'pii'],
      ['files', 'Files', 'PDFs, images', 'mut'],
      ['cache', 'Cache', 'shared, expiring', 'mut']
    ]
    stores.forEach(([id, label, sub, kind], i) => {
      g.node(id, { x: 622, y: mid(i) - 25, w: 156, h: 50, label, sub, kind, shape: 'store' })
      g.wire(`to-${id}`, [[588, mid(i)], g.port(id, 'left')])
    })
    // One bus from the kernel to its stores.
    g.wire('bus', [[588, mid(0)], [588, mid(4)]], { arrow: false })
    g.wire('trunk', [[560, mid(2)], [588, mid(2)]], { arrow: false })

    const label = (y, text) => g.text(255, y, text, { size: 9.5 })
    g.wire('requests', [g.port('clients', 'right'), g.port('checks', 'left')])
    label(mid(0) - 7, 'commands, queries')
    g.wire('execute', [g.port('execute', 'left', -6), g.port('workers', 'right', -6)])
    label(mid(1) - 13, 'commands, history')
    g.wire('results', [g.port('workers', 'right', 6), g.port('execute', 'left', 6)])
    label(mid(1) + 22, 'events, signed')
    g.wire('deliver', [g.port('log', 'left'), g.port('listeners', 'right')])
    label(mid(2) - 7, 'events, queries, checks')
  },
  steps () {
    return []
  }
}

/* ------------------------------------------------------------------
   query — architecture page: one query's way, the counterpart of `hero`
------------------------------------------------------------------ */
diagrams.query = {
  w: 760, h: 300,
  intro: 'A query goes through the kernel too: checked, answered by a read model in order with its events, checked again.',
  legend: ['qry', 'evt', 'ok', 'kern'],
  build (g) {
    const row = (i) => 66 + i * 76 // as in `hero`: rows are 56 high
    g.zone(10, 14, 300, 272, 'Your code · any language')
    g.icon(28, 37, 18, icons.typescript)
    g.icon(56, 37, 18, icons.python)
    g.icon(84, 37, 18, icons.rust)
    g.node('client', { x: 28, y: row(0), w: 264, h: 56, label: 'Client', sub: 'asks a query', kind: 'qry' })
    // One read model faces both the router and the log: it gets queries and events on one stream.
    g.node('rm', { x: 28, y: row(1), w: 264, h: row(2) + 56 - row(1), label: 'Read model', sub: 'answers from its own state', kind: 'qry' })
    g.zone(410, 14, 340, 272, null, 'var(--vp-c-brand-1)')
    g.image(426, 26, 99, 25, 'logo-nav.png')
    g.node('checks', { x: 428, y: row(0), w: 304, h: 56, label: 'Checks', sub: 'signature · grant · middleware', kind: 'kern' })
    g.node('route', { x: 428, y: row(1), w: 304, h: 56, label: 'Route', sub: 'to a read model that answers it', kind: 'kern' })
    for (let i = 0; i < 4; i++) {
      const x = 428 + i * 80
      g.node(`e${i}`, { x, y: row(2), w: 64, h: 56, label: `#${39 + i}`, sub: 'event', kind: 'evt', shape: 'store' })
      if (i > 0) g.wire(`link${i}`, [[x - 16, row(2) + 28], [x, row(2) + 28]], { arrow: false, dashed: true })
    }
    g.wire('in', [g.port('client', 'right'), g.port('checks', 'left')])
    g.wire('routed', [g.port('checks', 'bottom'), g.port('route', 'top')])
    g.wire('ask', [g.port('route', 'left'), [292, row(1) + 28]])
    g.wire('feed', [g.port('e0', 'left'), [292, row(2) + 28]])
  },
  steps () {
    const check = (text) => ({ say: 'kernable checks it, as it checks a command.', hot: ['checks'], run: (g) => g.setSub('checks', text), pause: 600, after: (g) => g.setSub('checks', text) })
    return [
      { say: 'A client asks a query, signed with its own key.',
        hot: ['client', 'checks'], packets: [['in', { kind: 'qry', label: 'GetCart', dur: 900 }]] },
      check('✓ signature'),
      check('✓ signature  ✓ grant'),
      check('✓ signature  ✓ grant  ✓ middleware'),
      { say: 'It is routed to a read model that answers GetCart.',
        hot: ['checks', 'route'], packets: [['routed', { kind: 'qry', dur: 500 }]] },
      { say: 'The read model gets the query in order with its events: first everything committed before it…',
        hot: ['e3', 'rm'], packets: [['feed', { kind: 'evt', label: '#42', dur: 700 }]],
        after: (g) => g.setSub('rm', 'has processed #42') },
      { say: '…then the query. So its answer reflects every event it was sent before.',
        hot: ['route', 'rm'],
        packets: [['ask', { kind: 'qry', label: 'GetCart', dur: 700 }], ['ask', { kind: 'ok', label: 'answer', dur: 700, reverse: true, delay: 850 }]] },
      { say: 'A middleware after the query may still refuse the answer, e.g. another customer\'s cart.',
        ...check('✓ after: may see it') },
      { say: 'The client gets the answer, up to date with every settled submit.',
        hot: ['checks', 'client'], packets: [['in', { kind: 'ok', label: 'answer', dur: 900, reverse: true }]] }
    ]
  }
}

/* ------------------------------------------------------------------
   decide — event sourcing in one loop: a command is decided into events, events fold into state
------------------------------------------------------------------ */
diagrams.decide = {
  w: 760, h: 310,
  intro: 'Decide: state + command → events. Fold: events → state. That is all of event sourcing.',
  legend: ['cmd', 'evt', 'err'],
  build (g) {
    g.node('cmd', { x: 20, y: 50, w: 210, h: 56, label: 'Command', sub: 'AddItem · qty 1', kind: 'cmd' })
    g.node('state', { x: 20, y: 194, w: 210, h: 56, label: 'State', sub: 'items 2', kind: 'kern' })
    g.node('decide', { x: 290, y: 122, w: 160, h: 76, label: 'Command handler', sub: 'decides', kind: 'ok' })
    g.zone(515, 14, 235, 250, 'Stream shop/cart/c1')
    const events = [['v1', 'v1 · ItemAdded', 'qty 2'], ['v2', 'v2 · ItemAdded', 'qty 1'], ['v3', 'v3 · CheckedOut', 'order o-7']]
    events.forEach(([id, label, sub], i) => g.node(id, { x: 532, y: 44 + i * 72, w: 200, h: 54, label, sub, kind: 'evt', hidden: i > 0 }))
    g.wire('in', [g.port('cmd', 'right'), [258, 78], [258, 145], g.port('decide', 'left', -15)])
    g.wire('read', [g.port('state', 'right'), [258, 222], [258, 175], g.port('decide', 'left', 15)])
    g.wire('emit2', [g.port('decide', 'right', -10), [490, 150], [490, 143], g.port('v2', 'left')], { hidden: true })
    g.wire('emit3', [g.port('decide', 'right', 10), [490, 170], [490, 215], g.port('v3', 'left')], { hidden: true })
    // Fold: the events, in order, through `apply` back into the state.
    g.wire('fold', [[632, 264], [632, 290], [125, 290], g.port('state', 'bottom')])
    g.text(380, 284, 'apply: fold the events, in order, into the state')
  },
  steps () {
    const ask = (label) => ({ hot: ['cmd', 'state', 'decide'], run: (g) => g.setSub('cmd', label), after: (g) => g.setSub('cmd', label),
      packets: [['in', { kind: 'cmd', label, dur: 800 }], ['read', { kind: 'kern', label: 'state', dur: 800, delay: 0 }]] })
    return [
      { say: 'A command arrives; the handler looks at the current state.', ...ask('AddItem · qty 1') },
      { say: 'It decides: the cart is open, so it returns an event.',
        show: ['v2', 'emit2'], hot: ['decide', 'v2'], packets: [['emit2', { kind: 'evt', label: 'ItemAdded', dur: 800 }]] },
      { say: 'The new event is folded into the state.',
        hot: ['v2', 'state'], packets: [['fold', { kind: 'evt', dur: 1000 }]], after: (g) => g.setSub('state', 'items 3') },
      { say: 'Checking out works the same way.', ...ask('CheckOut') },
      { say: 'Another event, the next version.',
        show: ['v3', 'emit3'], hot: ['decide', 'v3'], packets: [['emit3', { kind: 'evt', label: 'CheckedOut', dur: 800 }]] },
      { say: 'Folded too: now the cart is checked out.',
        hot: ['v3', 'state'], packets: [['fold', { kind: 'evt', dur: 1000 }]], after: (g) => g.setSub('state', 'items 3 · checked out') },
      { say: 'A late AddItem is decided against that state…', ...ask('AddItem · qty 1') },
      { say: '…and rejected: no event, the state stays as it is.',
        hot: ['decide', 'cmd'], packets: [['in', { kind: 'err', label: 'Rejected', dur: 900, reverse: true }]] }
    ]
  }
}

/* ------------------------------------------------------------------
   streams — one stream per aggregate, all of them in one log
------------------------------------------------------------------ */
diagrams.streams = {
  w: 760, h: 270,
  intro: 'Every aggregate has its own stream, numbered by version. All streams interleave in one log, numbered by position.',
  legend: [],
  build (g) {
    const lanes = { c1: 40, c2: 112 }
    const events = [['c1', 1, 'ItemAdded'], ['c2', 1, 'ItemAdded'], ['c1', 2, 'ItemAdded'], ['c1', 3, 'CheckedOut'], ['c2', 2, 'ItemAdded']]
    g.text(14, lanes.c1 + 32, 'cart c1', { anchor: 'start', size: 11 })
    g.text(14, lanes.c2 + 32, 'cart c2', { anchor: 'start', size: 11 })
    g.text(14, 222, 'the log', { anchor: 'start', size: 11 })
    events.forEach(([cart, version, type], i) => {
      const x = 96 + i * 132
      g.node(`s${i}`, { x, y: lanes[cart], w: 116, h: 50, label: `${cart} · v${version}`, sub: type, kind: 'evt' })
      g.node(`p${i}`, { x, y: 196, w: 116, h: 50, label: `#${41 + i}`, sub: `${cart} v${version}`, kind: 'kern', shape: 'store' })
      g.wire(`w${i}`, [g.port(`s${i}`, 'bottom'), g.port(`p${i}`, 'top')], { dashed: true })
    })
  },
  steps () {
    return []
  }
}

/* ------------------------------------------------------------------
   saga — a rule across two aggregates, carried by a reactor
------------------------------------------------------------------ */
diagrams.saga = {
  w: 760, h: 250,
  intro: 'Two aggregates, one rule: a reactor turns one aggregate\'s event into a command for the other, and back.',
  legend: ['cmd', 'evt', 'err'],
  build (g) {
    g.node('order', { x: 30, y: 30, w: 230, h: 62, label: 'Order', sub: 'o-7 · new', kind: 'cmd' })
    g.node('stock', { x: 30, y: 168, w: 230, h: 62, label: 'Stock', sub: 'apple · 5 left', kind: 'cmd' })
    g.node('reactor', { x: 500, y: 99, w: 230, h: 62, label: 'Reactor', sub: 'reservations', kind: 'evt' })
    // Wires leave and enter on separate tracks, so none crosses another.
    g.wire('placed', [g.port('order', 'right', 14), [400, 75], [400, 115], g.port('reactor', 'left', -15)])
    g.wire('reserve', [g.port('reactor', 'left', 15), [400, 145], [400, 199], g.port('stock', 'right')])
    g.wire('answer', [g.port('order', 'right', -14), [615, 47], g.port('reactor', 'top')], { arrow: false })
  },
  steps () {
    const round = (order, qty, left, ok) => [
      { say: `Order ${order} is placed: an event of the order alone.`, hot: ['order'],
        run: (g) => g.setSub('order', `${order} · placed · ${qty} apples`), after: (g) => g.setSub('order', `${order} · placed · ${qty} apples`),
        packets: [['placed', { kind: 'evt', label: 'OrderPlaced', dur: 900 }]] },
      { say: 'The reactor reacts with a command for the stock, with a fixed ID.', hot: ['reactor', 'stock'],
        packets: [['reserve', { kind: 'cmd', label: `Reserve ${qty}`, dur: 900 }]] },
      ok
        ? { say: 'The stock decides: enough left, reserved.', hot: ['stock', 'reactor'],
            packets: [['reserve', { kind: 'evt', label: 'StockReserved', dur: 900, reverse: true }]],
            after: (g) => g.setSub('stock', `apple · ${left} left`) }
        : { say: 'The stock decides: not enough left, rejected.', hot: ['stock', 'reactor'],
            packets: [['reserve', { kind: 'err', label: 'Rejected', dur: 900, reverse: true }]] },
      { say: ok ? 'The reactor confirms the order.' : 'The reactor cancels the order.', hot: ['reactor', 'order'],
        packets: [['answer', { kind: 'cmd', label: ok ? 'Confirm' : 'Cancel', dur: 900, reverse: true }]],
        after: (g) => g.setSub('order', `${order} · ${ok ? 'confirmed' : 'cancelled'}`) }
    ]
    return [
      ...round('o-7', 2, 3, true),
      { say: 'Next order.', hot: [], run: (g) => g.setSub('order', 'o-8 · new'), pause: 900, after: (g) => g.setSub('order', 'o-8 · new') },
      ...round('o-8', 9, 3, false)
    ]
  }
}

/* ------------------------------------------------------------------
   lanes — commands: one at a time per aggregate, aggregates in parallel
------------------------------------------------------------------ */
diagrams.lanes = {
  w: 740, h: 210,
  intro: 'Commands of one aggregate run one after another; different aggregates run at the same time.',
  legend: [],
  build (g) {
    const lane = (y, name) => g.text(14, y + 31, name, { anchor: 'start', size: 11 })
    lane(30, 'cart c1')
    lane(110, 'cart c2')
    const box = (id, x, y, label, sub) => g.node(id, { x, y, w: 150, h: 52, label, sub, kind: 'cmd' })
    box('a1', 100, 30, 'AddItem', 'decided on v0 → v1')
    box('a2', 260, 30, 'AddItem', 'on v1 → v2')
    box('a3', 420, 30, 'CheckOut', 'on v2 → v3')
    box('b1', 170, 110, 'AddItem', 'decided on v0 → v1')
    box('b2', 330, 110, 'CheckOut', 'on v1 → v2')
    g.wire('time', [[100, 186], [730, 186]])
    g.text(715, 202, 'time', { anchor: 'end', size: 10.5 })
  },
  steps () {
    return []
  }
}

/* ------------------------------------------------------------------
   layers — who checks what, from the connection to the stored log
------------------------------------------------------------------ */
diagrams.layers = {
  w: 760, h: 120,
  intro: 'Five checks, from the connection to the stored log: each answers its own question.',
  legend: [],
  build (g) {
    const layers = [
      ['tls', 'TLS', 'may it connect?', 'mut'],
      ['sig', 'Signature', 'which key sent it?', 'sig'],
      ['grant', 'Grant', 'may that key do it?', 'kern'],
      ['mw', 'Middleware', 'may this person?', 'ok'],
      ['chain', 'Hash chain', 'unchanged since?', 'evt']
    ]
    layers.forEach(([id, label, sub, kind], i) => {
      g.node(id, { x: 10 + i * 152, y: 30, w: 132, h: 60, label, sub, kind })
      if (i > 0) g.wire(`to-${id}`, [[10 + i * 152 - 20, 60], [10 + i * 152, 60]])
    })
  },
  steps () {
    return []
  }
}

/* ------------------------------------------------------------------
   chain — the middleware chain: by order, and the first rejection refuses
------------------------------------------------------------------ */
diagrams.chain = {
  w: 760, h: 140,
  intro: 'Middlewares check a request in order; the first rejection refuses it, and it is never stored.',
  legend: ['cmd', 'ok', 'err'],
  build (g) {
    g.node('req', { x: 10, y: 40, w: 150, h: 60, label: 'Request', sub: 'AddItem · alice', kind: 'cmd' })
    g.node('rbac', { x: 205, y: 40, w: 150, h: 60, label: 'rbac', sub: 'order 0', kind: 'ok' })
    g.node('quota', { x: 400, y: 40, w: 150, h: 60, label: 'quota', sub: 'order 5', kind: 'ok' })
    g.node('done', { x: 595, y: 40, w: 155, h: 60, label: 'Accepted', sub: 'stored and queued', kind: 'kern' })
    g.wire('a', [g.port('req', 'right'), g.port('rbac', 'left')])
    g.wire('b', [g.port('rbac', 'right'), g.port('quota', 'left')])
    g.wire('c', [g.port('quota', 'right'), g.port('done', 'left')])
  },
  steps () {
    const reset = (g, item) => { g.setSub('req', `AddItem · alice, ${item}`); g.setSub('rbac', 'order 0'); g.setSub('quota', 'order 5') }
    return [
      { say: 'A request reaches the chain.', hot: ['req', 'rbac'], run: (g) => reset(g, '3rd item'), after: (g) => reset(g, '3rd item'),
        packets: [['a', { kind: 'cmd', dur: 700 }]] },
      { say: 'rbac: alice is a customer.', hot: ['rbac', 'quota'], after: (g) => g.setSub('rbac', '✓ a customer'),
        packets: [['b', { kind: 'cmd', dur: 700 }]] },
      { say: 'quota: her cart has room.', hot: ['quota', 'done'], after: (g) => g.setSub('quota', '✓ 3 of 3'),
        packets: [['c', { kind: 'cmd', dur: 700 }]] },
      { say: 'Accepted.', hot: ['done'], pause: 900 },
      { say: 'The next request…', hot: ['req', 'rbac'], run: (g) => reset(g, '4th item'), after: (g) => reset(g, '4th item'),
        packets: [['a', { kind: 'cmd', dur: 700 }]] },
      { say: '…passes rbac…', hot: ['rbac', 'quota'], after: (g) => g.setSub('rbac', '✓ a customer'),
        packets: [['b', { kind: 'cmd', dur: 700 }]] },
      { say: '…and quota refuses it: no further check runs, nothing is stored.', hot: ['quota', 'rbac', 'req'],
        after: (g) => g.setSub('quota', '✗ cart full'),
        packets: [['b', { kind: 'err', label: 'quota', dur: 700, reverse: true }], ['a', { kind: 'err', label: 'refused', dur: 700, reverse: true, delay: 750 }]] }
    ]
  }
}

/* ------------------------------------------------------------------
   ordering — listener order and settling
------------------------------------------------------------------ */
diagrams.ordering = {
  w: 740, h: 340,
  intro: 'How the kernel hands one event to its listeners: lower orders first, and a submit settles when all are done.',
  legend: ['evt', 'qry', 'ok'],
  build (g) {
    g.zone(190, 24, 165, 300, 'Order 0', 'var(--vp-c-indigo-1)')
    g.zone(380, 24, 165, 300, 'Order 10', 'var(--vp-c-purple-1)')
    g.zone(570, 24, 155, 300, 'Order 20', 'var(--vp-c-purple-1)')
    g.node('kernel', { x: 20, y: 140, w: 130, h: 64, label: 'Kernel', sub: 'event at position 42', kind: 'kern' })
    g.node('orders', { x: 205, y: 70, w: 135, h: 52, label: 'Orders view', sub: 'read model', kind: 'qry' })
    g.node('stock', { x: 205, y: 220, w: 135, h: 52, label: 'Stock view', sub: 'read model', kind: 'qry' })
    g.node('mailer', { x: 395, y: 145, w: 135, h: 52, label: 'Mailer', sub: 'reactor', kind: 'evt' })
    g.node('invoice', { x: 582, y: 145, w: 130, h: 52, label: 'Invoicing', sub: 'later reactor', kind: 'evt' })
    g.wire('orders', [g.port('kernel', 'right', -12), [170, 160], [170, 96], g.port('orders', 'left')])
    g.wire('stock', [g.port('kernel', 'right', 12), [170, 184], [170, 246], g.port('stock', 'left')])
    g.wire('mailer', [g.port('kernel', 'bottom', 16), [101, 306], [462, 306], g.port('mailer', 'bottom')])
    g.wire('invoice', [g.port('kernel', 'bottom'), [85, 318], [647, 318], g.port('invoice', 'bottom')])
    g.wire('query', [g.port('mailer', 'top'), [462, 96], g.port('orders', 'right')], { dashed: true })
  },
  steps () {
    return [
      { say: 'Every listener of order 0 gets the event, in global order. Listeners of the same order run in parallel.',
        hot: ['kernel', 'orders', 'stock'],
        packets: [['orders', { kind: 'evt', label: '#42', dur: 800 }], ['stock', { kind: 'evt', label: '#42', dur: 800, delay: 0 }]] },
      { say: 'Each one acknowledges what it has processed.',
        hot: ['orders', 'stock'],
        packets: [['orders', { kind: 'ok', label: 'ack', dur: 650, reverse: true }], ['stock', { kind: 'ok', label: 'ack', dur: 650, reverse: true, delay: 200 }]],
        after: (g) => { g.setSub('orders', 'at position 42'); g.setSub('stock', 'at position 42') } },
      { say: 'Only now does order 10 get the event: a lower order must have acknowledged it first.',
        hot: ['kernel', 'mailer'], packets: [['mailer', { kind: 'evt', label: '#42', dur: 1000 }]] },
      { say: 'So when the reactor queries a read model, the read model already reflects the event it reacts to.',
        hot: ['mailer', 'orders'],
        packets: [['query', { kind: 'qry', label: 'query', dur: 700 }], ['query', { kind: 'ok', label: 'answer', dur: 600, reverse: true, delay: 850 }]] },
      { say: 'The reactor acknowledges, and order 20 follows the same way.',
        hot: ['mailer', 'kernel', 'invoice'],
        packets: [['mailer', { kind: 'ok', label: 'ack', dur: 700, reverse: true }], ['invoice', { kind: 'evt', label: '#42', dur: 1000, delay: 800 }]],
        after: (g) => g.setSub('mailer', 'at position 42') },
      { say: 'When the last listener acknowledges, the submit that wrote the event settles and returns.',
        hot: ['invoice', 'kernel'], packets: [['invoice', { kind: 'ok', label: 'ack', dur: 1000, reverse: true }]],
        after: (g) => { g.setSub('invoice', 'at position 42'); g.setSub('kernel', 'position 42 settled') } },
      { say: 'After a crash, a listener gets everything after its last acknowledgement again — so handlers must be idempotent.',
        hot: [], pause: 1600 }
    ]
  }
}

/* ------------------------------------------------------------------
   trust — signatures, hash chain, checkpoint, verify
------------------------------------------------------------------ */
diagrams.trust = {
  w: 740, h: 300,
  intro: 'Every claim is signed, every event is chained to the one before, and kernable verify checks it all.',
  legend: ['cmd', 'evt', 'sig', 'err'],
  build (g) {
    g.node('client', { x: 20, y: 24, w: 150, h: 52, label: 'Client', sub: 'own Ed25519 key', kind: 'cmd' })
    g.node('kernel', { x: 295, y: 20, w: 150, h: 60, label: 'Kernel', sub: 'trusts root keys', kind: 'kern' })
    g.node('worker', { x: 570, y: 24, w: 150, h: 52, label: 'Worker', sub: 'own Ed25519 key', kind: 'cmd' })
    g.zone(10, 150, 720, 130, 'Hash chain', 'var(--vp-c-purple-1)')
    g.node('genesis', { x: 25, y: 190, w: 100, h: 56, label: 'genesis', sub: 'start' })
    g.node('e1', { x: 160, y: 190, w: 100, h: 56, label: 'event 1', sub: 'hash(…, genesis)', kind: 'evt' })
    g.node('e2', { x: 295, y: 190, w: 100, h: 56, label: 'event 2', sub: 'hash(…, event 1)', kind: 'evt' })
    g.node('e3', { x: 430, y: 190, w: 100, h: 56, label: 'event 3', sub: 'not stored yet' })
    g.node('cp', { x: 570, y: 190, w: 145, h: 56, label: 'Checkpoint', sub: 'at event 2', kind: 'sig' })
    g.wire('submit', [g.port('client', 'right'), g.port('kernel', 'left', 0)])
    g.wire('result', [g.port('worker', 'left'), g.port('kernel', 'right', 0)])
    g.wire('commit', [g.port('kernel', 'bottom'), [370, 120], [480, 120], g.port('e3', 'top')])
    g.wire('c1', [g.port('genesis', 'right'), g.port('e1', 'left')])
    g.wire('c2', [g.port('e1', 'right'), g.port('e2', 'left')])
    g.wire('c3', [g.port('e2', 'right'), g.port('e3', 'left')])
    g.wire('c4', [g.port('e3', 'right'), g.port('cp', 'left')])
  },
  steps () {
    const verify = (kind) => [
      ['c1', { kind: 'sig', dur: 450 }], ['c2', { kind: 'sig', dur: 450, delay: 500 }],
      ['c3', { kind, dur: 450, delay: 1000 }], ['c4', { kind, dur: 450, delay: 1500 }]
    ]
    return [
      { say: 'A client signs its command. The signature proves who asked for exactly this, in which context.',
        hot: ['client', 'kernel'], packets: [['submit', { kind: 'cmd', label: 'signed command', dur: 900 }]] },
      { say: 'The kernel checks the signature and whether the key\'s grant, signed by a root key, permits this command type.',
        hot: ['kernel'], pause: 1100, after: (g) => g.setSub('kernel', 'signature ok, allowed') },
      { say: 'The worker signs its result: which worker decided, based on which version.',
        hot: ['worker', 'kernel'], packets: [['result', { kind: 'evt', label: 'signed events', dur: 900 }]] },
      { say: 'The stored event\'s hash covers its content, the worker\'s signature, the request context and the previous hash.',
        hot: ['kernel', 'e3', 'e2'], packets: [['commit', { kind: 'evt', dur: 900 }]],
        after: (g) => g.setSub('e3', 'hash(…, event 2)') },
      { say: 'Regularly, the kernel signs a checkpoint: the state of the whole log at a position.',
        hot: ['e3', 'cp'], packets: [['c4', { kind: 'sig', label: 'sign', dur: 700 }]],
        after: (g) => g.setSub('cp', 'at event 3, signed') },
      { say: 'kernable verify recomputes the chain from the start and checks every signature.',
        hot: ['genesis', 'e1', 'e2', 'e3', 'cp'], packets: verify('sig'),
        after: (g) => g.setSub('cp', 'verified ✓') },
      { say: 'Now someone changes event 2 in the store…',
        hot: ['e2'], pause: 1100, after: (g) => { g.setSub('e2', 'changed!'); g.setSub('cp', 'at event 3, signed') } },
      { say: '…and verify shows it: event 3\'s hash no longer matches. Changed, removed or reordered events all show.',
        hot: ['e2', 'e3'], packets: verify('err'),
        after: (g) => { g.setSub('e3', 'hash mismatch ✗'); g.setSub('cp', 'verify failed ✗') } },
      { say: 'Events after the last checkpoint are protected by the chain only.', hot: [], pause: 1400 }
    ]
  }
}

/* ------------------------------------------------------------------
   data — personal data in the vault, and an erasing event
------------------------------------------------------------------ */
diagrams.vault = {
  w: 740, h: 330,
  intro: 'Events cannot be deleted, but personal data can: it goes to the vault, and the log keeps only a reference.',
  legend: ['evt', 'pii', 'err'],
  build (g) {
    g.node('worker', { x: 20, y: 135, w: 130, h: 56, label: 'Worker', sub: 'aggregate', kind: 'cmd' })
    g.node('kernel', { x: 190, y: 133, w: 130, h: 60, label: 'Kernel', sub: 'routes fields', kind: 'kern' })
    g.node('log', { x: 360, y: 30, w: 160, h: 70, label: 'Log', sub: 'signed, forever', kind: 'evt', shape: 'store' })
    g.node('vault', { x: 360, y: 230, w: 160, h: 70, label: 'Vault', sub: 'encrypted, deletable', kind: 'pii', shape: 'store' })
    g.node('crm', { x: 575, y: 170, w: 150, h: 56, label: 'CRM view', sub: 'vault reader', kind: 'qry' })
    g.node('stats', { x: 575, y: 20, w: 150, h: 52, label: 'Analytics', sub: 'no vault access', kind: 'qry' })
    g.wire('emit', [g.port('worker', 'right'), g.port('kernel', 'left')])
    g.wire('log', [g.port('kernel', 'top'), [255, 65], g.port('log', 'left')])
    g.wire('vault', [g.port('kernel', 'bottom'), [255, 265], g.port('vault', 'left')])
    g.wire('crm', [g.port('log', 'right', 10), [536, 75], [536, 190], g.port('crm', 'left', -8)])
    g.wire('crmv', [g.port('vault', 'right'), [556, 265], [556, 206], g.port('crm', 'left', 8)], { dashed: true })
    g.wire('stats', [g.port('log', 'right', -10), [548, 55], [548, 46], g.port('stats', 'left')])
  },
  steps () {
    return [
      { say: 'A worker returns an event with a field declared personal: the customer\'s email.',
        hot: ['worker', 'kernel'], packets: [['emit', { kind: 'pii', label: 'email: max@…', dur: 900 }]] },
      { say: 'The kernel splits it: the value goes to the vault, the log keeps a salted reference.',
        hot: ['kernel', 'log', 'vault'],
        packets: [['log', { kind: 'evt', label: '{$vault: 3fa9…}', dur: 900 }], ['vault', { kind: 'pii', label: 'max@example.com', dur: 900, delay: 0 }]],
        after: (g) => { g.setSub('log', 'email → 3fa9…'); g.setSub('vault', '1 value') } },
      { say: 'Listeners granted vault_read get the value; everyone else gets null.',
        hot: ['log', 'vault', 'crm', 'stats'],
        packets: [
          ['crm', { kind: 'evt', dur: 800 }], ['crmv', { kind: 'pii', label: 'max@…', dur: 800, delay: 0 }],
          ['stats', { kind: 'evt', label: 'email: null', dur: 800, delay: 0 }]
        ],
        after: (g) => { g.setSub('crm', 'shows max@…'); g.setSub('stats', 'email: null') } },
      { say: 'Later the person asks to be forgotten, and the worker returns an erasing event.',
        hot: ['worker', 'kernel'], packets: [['emit', { kind: 'evt', label: 'CustomerErased', dur: 900 }]] },
      { say: 'The kernel deletes the person\'s values in every stream and domain, and the snapshots that may hold them.',
        hot: ['kernel', 'vault'], packets: [['vault', { kind: 'err', label: 'delete', dur: 900 }]],
        after: (g) => { g.setSub('vault', 'value deleted'); g.setSub('log', 'unchanged, still verifiable') } },
      { say: 'Read models forget the person when they see the erasing event; rebuilt, they read null.',
        hot: ['log', 'crm'], packets: [['crm', { kind: 'evt', label: 'CustomerErased', dur: 900 }]],
        after: (g) => g.setSub('crm', 'person forgotten') },
      { say: 'The log stays complete and signed; the person\'s data is gone.', hot: [], pause: 1400 }
    ]
  }
}

/* ------------------------------------------------------------------
   shop — the shop tutorial's overview: what each part adds
------------------------------------------------------------------ */
diagrams.shop = {
  w: 740, h: 350,
  intro: 'The shop you build in eight parts. Each step adds what one part builds and shows it at work.',
  legend: ['cmd', 'evt', 'qry', 'ok', 'pii', 'err'],
  build (g) {
    g.zone(10, 12, 400, 326, 'Yours', null, { id: 'yours', hidden: true })
    g.node('client', { x: 40, y: 40, w: 340, h: 46, label: 'Your scripts', sub: 'client', kind: 'cmd', hidden: true })
    g.node('guard', { x: 40, y: 100, w: 340, h: 46, label: 'Guard', sub: 'middleware', kind: 'ok', hidden: true })
    g.node('worker', { x: 40, y: 160, w: 340, h: 46, label: 'Worker', sub: 'customer, order', kind: 'cmd', hidden: true })
    g.node('rm', { x: 40, y: 220, w: 340, h: 46, label: 'Read models', sub: 'customers, orders', kind: 'qry', hidden: true })
    g.node('reactor', { x: 40, y: 280, w: 340, h: 46, label: 'Reactor', sub: 'confirmations', kind: 'evt', hidden: true })
    g.node('kernel', { x: 510, y: 140, w: 190, h: 76, label: 'Kernel', sub: 'checks, orders, stores', kind: 'kern' })
    g.node('commands', { x: 495, y: 268, w: 76, h: 62, label: 'Commands', sub: 'queued', kind: 'cmd', shape: 'store' })
    g.node('events', { x: 577, y: 268, w: 76, h: 62, label: 'Events', sub: 'signed log', kind: 'evt', shape: 'store' })
    g.node('vault', { x: 659, y: 268, w: 76, h: 62, label: 'Vault', sub: 'personal', kind: 'pii', shape: 'store', hidden: true })
    g.wire('in', [g.port('client', 'right'), [470, 63], [470, 150], g.port('kernel', 'left', -28)], { hidden: true })
    g.wire('guard', [g.port('kernel', 'left', -14), [450, 164], [450, 123], g.port('guard', 'right')], { hidden: true })
    g.wire('worker', [g.port('kernel', 'left'), [430, 178], [430, 183], g.port('worker', 'right')], { hidden: true })
    g.wire('rm', [g.port('kernel', 'left', 14), [450, 192], [450, 243], g.port('rm', 'right')], { hidden: true })
    g.wire('reactor', [g.port('kernel', 'left', 28), [470, 206], [470, 303], g.port('reactor', 'right')], { hidden: true })
    g.wire('commands', [[533, 216], g.port('commands', 'top')])
    g.wire('events', [[615, 216], g.port('events', 'top')])
    g.wire('vault', [[697, 216], g.port('vault', 'top')], { hidden: true })
  },
  steps () {
    // Persistent changes go in `after`, which navigation replays; `run` shows them right away.
    const languages = (g) => {
      g.setSub('worker', 'customer, order · TypeScript')
      g.setSub('rm', 'customers, orders · Python')
      g.setSub('reactor', 'confirmations · Rust')
    }
    const production = (g) => {
      g.setSub('kernel', 'root key offline · metrics')
      g.setSub('worker', 'TypeScript · own key, mutual TLS')
      g.setSub('rm', 'Python · own key, survives restarts')
      g.setSub('reactor', 'Rust · own key, mutual TLS')
    }
    return [
      { say: 'Part 1, run a kernel. It checks, orders and stores commands and events; your processes connect with a key and a grant.',
        show: ['yours', 'client', 'in'], hot: ['client', 'kernel'],
        packets: [['in', { kind: 'cmd', label: 'connect', dur: 800 }], ['in', { kind: 'ok', label: 'ok', dur: 700, reverse: true, delay: 950 }]] },
      { say: 'Part 2, aggregates and commands. The kernel stores and queues each command; the worker runs customer and order and returns events, which the kernel appends to the log.',
        show: ['worker'], hot: ['client', 'kernel', 'worker', 'commands', 'events'],
        packets: [
          ['in', { kind: 'cmd', label: 'Place', dur: 800 }],
          ['commands', { kind: 'cmd', dur: 500, delay: 900 }],
          ['worker', { kind: 'cmd', label: 'Place', dur: 700, delay: 1500 }],
          ['worker', { kind: 'evt', label: 'OrderPlaced', dur: 700, reverse: true, delay: 2300 }],
          ['events', { kind: 'evt', dur: 500, delay: 3100 }],
          ['in', { kind: 'ok', label: 'committed', dur: 800, reverse: true, delay: 3700 }]
        ] },
      { say: 'Part 3, read models. customers and orders build their views from the events and answer queries; a submit returns once they are up to date.',
        show: ['rm'], hot: ['kernel', 'rm', 'client'],
        packets: [
          ['rm', { kind: 'evt', label: 'OrderPlaced', dur: 700 }],
          ['rm', { kind: 'ok', label: 'ack', dur: 600, reverse: true, delay: 800 }],
          ['in', { kind: 'qry', label: 'GetOrder', dur: 800, delay: 1500 }],
          ['rm', { kind: 'qry', label: 'GetOrder', dur: 600, delay: 2350 }],
          ['rm', { kind: 'ok', label: 'answer', dur: 600, reverse: true, delay: 3000 }],
          ['in', { kind: 'ok', label: 'answer', dur: 800, reverse: true, delay: 3650 }]
        ] },
      { say: 'Part 4, a reactor. confirmations gets each submitted order after the read models and sends a follow-up command.',
        show: ['reactor'], hot: ['kernel', 'reactor'],
        packets: [
          ['reactor', { kind: 'evt', label: 'OrderSubmitted', dur: 800 }],
          ['reactor', { kind: 'cmd', label: 'Confirm', dur: 800, reverse: true, delay: 1000 }]
        ] },
      { say: 'Part 5, a second language. Nothing new to build: each process can be TypeScript, Python or Rust, and they work together.',
        hot: ['worker', 'rm', 'reactor'], run: languages, pause: 1800, after: languages },
      { say: 'Part 6, authorization. The guard checks every command and query against the sender: customers act only on their own data.',
        show: ['guard'], hot: ['client', 'kernel', 'guard'],
        packets: [
          ['in', { kind: 'cmd', label: 'GetOrder as bob', dur: 800 }],
          ['guard', { kind: 'cmd', label: 'check', dur: 700, delay: 900 }],
          ['guard', { kind: 'err', label: 'deny', dur: 700, reverse: true, delay: 1700 }],
          ['in', { kind: 'err', label: 'rejected', dur: 800, reverse: true, delay: 2500 }]
        ] },
      { say: 'Part 7, personal data. Email and name go to the vault, the signed event log keeps references; erasing a customer deletes them from the vault.',
        show: ['vault'], hot: ['worker', 'kernel', 'events', 'vault'],
        packets: [
          ['worker', { kind: 'pii', label: 'CustomerRegistered', dur: 800, reverse: true }],
          ['events', { kind: 'evt', dur: 600, delay: 900 }],
          ['vault', { kind: 'pii', dur: 600, delay: 900 }],
          ['worker', { kind: 'evt', label: 'CustomerErased', dur: 800, reverse: true, delay: 1800 }],
          ['vault', { kind: 'err', dur: 600, delay: 2700 }]
        ],
        after: (g) => g.setSub('vault', 'erased') },
      { say: 'Part 8, operate. Each service has its own key, the root key stays offline, connections use mutual TLS, read models survive restarts.',
        hot: ['kernel', 'worker', 'rm', 'reactor'], run: production, pause: 2200, after: production },
      { say: 'That is the whole shop: commands in, events out, every read up to date.', hot: [], pause: 1400 }
    ]
  }
}
