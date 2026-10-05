# PBS Monitor Visual Alignment for the Node Monitor Dashboard

## Status

Approved visual direction. This specification defines a presentation-only alignment of the node-monitor dashboard with the established PBS Monitor web UI. It does not change telemetry meaning, API contracts, database behavior, or deployment behavior.

## Goal

Make node-monitor and PBS Monitor visibly read as members of the same operational-dashboard family while retaining node-monitor's distinct information architecture and telemetry semantics.

The approved direction is the right-hand concept shown in the visual comparison reviewed on 2026-10-04: PBS Monitor's dark visual system and component language applied to node-monitor's existing controls, status states, and charts.

## Source of truth

The implementation must derive its visual vocabulary from PBS Monitor's current main-page assets:

- `pbs_monitor/web/static/css/dashboard.css`
- `pbs_monitor/web/static/index.html`

The relevant vocabulary is:

- dark navy page and panel surfaces;
- muted slate secondary text;
- blue primary actions and selected controls;
- green, amber, and red operational state accents;
- compact system-font typography;
- sticky, bordered, shadowed header hierarchy;
- uppercase, letter-spaced panel and metric labels;
- rounded panels with restrained one-pixel borders;
- compact segmented controls and form fields;
- dense operational layout without decorative gradients or external assets.

The implementation must not copy PBS-specific data concepts, Vue structure, job controls, node maps, or queue views.

## Scope

### In scope

1. Restyle and reorganize the existing node-monitor static dashboard markup and stylesheet.
2. Add semantic CSS classes needed for the shared visual language.
3. Add or adjust frontend rendering classes or attributes only where needed to represent existing operational states visually.
4. Preserve all existing `data-testid` values used by browser acceptance tests.
5. Add browser acceptance tests for visual-system invariants, responsive layout, visible focus, and status-state styling.
6. Capture and inspect desktop and 400-pixel-wide screenshots from production static bytes.

### Out of scope

- API or database changes.
- New telemetry, derived diagnosis, or causal claims.
- Vue, React, or another frontend framework.
- CDN, remote font, remote JavaScript, or remote CSS dependencies.
- Exact page-layout duplication of PBS Monitor.
- Changes to chart mathematics, aggregation, attribution, gaps, modes, or units.
- Changes to refresh cadence, request behavior, or data-retention behavior.
- A general shared CSS package between repositories. The repositories remain independently deployable.

## Information architecture

### Sticky operational header

The header establishes the same hierarchy as PBS Monitor while showing node-monitor concepts:

- Left: dominant system name and selected node.
- Beneath the identity: browser-to-web-server connection state, absolute newest-data timestamp, and locally advancing human-readable data age.
- Center: CPU busy p50 as the primary current metric.
- Right: running-process and total-process summaries.

Connectivity and telemetry freshness remain independent. A successful HTTP poll must not imply current telemetry. A failed poll after prior success must retain the last complete snapshot and visibly show `Web server disconnected`.

### Control panel

Place the global controls directly below the header in one bordered panel:

- segmented `1h / 3h / 6h / 12h / 24h` selector;
- node input and switch action;
- username input and filter action.

The selected range uses the PBS blue active treatment. Inputs use dark inset surfaces and clear labels. Existing forms, keyboard operation, and request behavior remain unchanged.

### Status summary

Present four compact status cards:

1. Counter freshness and quality.
2. Usage freshness and username result.
3. Physical-memory use.
4. Poll failures and breaker state.

Current, partial/stale, and failed/disconnected states use green, amber, and red accents respectively. Empty remains neutral rather than appearing successful or failed. Text remains the authoritative state indicator; color is supplementary.

CPU, load, and process current values used in the header remain available to assistive technology and existing tests. Any values retained outside the visible header should be structured without duplicating confusing visible summaries.

### Metric panels

Retain the four current plot groups:

- CPU / Load;
- Memory (physical);
- Process activity;
- Network / Lustre.

Each becomes a PBS-style panel with an uppercase muted panel header, dark chart well, bordered explanatory content, and compact local mode controls. Chart canvases remain in explicitly height-bounded positioned wrappers to prevent Chart.js resize feedback.

Technical explanations and fallback tables remain present but visually subordinate. Dense explanatory tables may use collapsible disclosure where that does not hide required state or alter accessibility.

### Detailed operational context

Retain D-state/interactivity hotspot attribution, hardware context, selected node/range/username, absolute timestamps, and failure details. Present these as subordinate panels rather than generic cards. Do not remove or simplify wording that protects the mathematical interpretation of the data.

## Visual tokens

Use local CSS custom properties with values aligned to PBS Monitor:

- page background: `#1a1a2e`;
- panel background: `#16213e`;
- chart/input inset: `#0f172a`;
- primary text: `#e0e0e0`;
- muted text: `#94a3b8`;
- border: `#2d3748`;
- blue/action: `#3b82f6`;
- green/current: `#4ade80`;
- amber/partial-or-stale: `#f59e0b`;
- red/disconnected-or-failed: `#ef4444`.

Use the same platform system-font stack as PBS Monitor. Avoid importing fonts. Base spacing should follow a compact quarter-rem rhythm. Panels use an 8-pixel radius and one-pixel border. Shadows are reserved for the sticky header and overlays, not every card.

## State mapping

State classes must be derived from existing response semantics, not guessed from displayed strings:

- connected plus current data: green connection/freshness treatment;
- connected plus stale or partial data: amber telemetry treatment while connection remains connected;
- disconnected after a successful snapshot: red connection treatment, retained data still shown and aging locally;
- first-load connection failure: red full-page error presentation with retry action;
- empty: neutral muted treatment and dashes/nulls, never invented zeros;
- complete/current versus partial/stale must remain independently visible for counter and usage sections.

Future timestamps are not current; existing API semantics remain authoritative.

## Accessibility and responsive behavior

- Preserve semantic headings, landmarks, labels, tables, `aria-live`, and `aria-pressed` behavior.
- Never encode operational state with color alone.
- All controls require a visible `:focus-visible` outline with adequate contrast.
- At desktop widths, status cards form a four-column grid and metric panels form a two-column grid.
- At intermediate widths, header sections wrap without overlap and controls remain usable.
- At 400 pixels, all content uses a single column, no horizontal page overflow occurs, all controls remain keyboard accessible, and chart wrappers retain bounded dimensions.
- Respect `prefers-reduced-motion`; the disconnected pulse must be disabled or reduced when requested.

## Security and dependency boundary

The dashboard remains self-contained and private-first:

- production HTML must load only allowlisted local static resources;
- no remote network requests, analytics, fonts, icons, or images;
- no new dynamic HTML injection;
- existing static-resource route protections and CSP-compatible behavior remain intact;
- CSS and markup changes must not introduce client-controlled resource names or handler parameters.

## Testing and acceptance

Implementation follows strict test-driven development. Each behavior-bearing change begins with an observed failing test.

### Automated acceptance

1. Existing `tests/web` and `tests/browser` suites remain green.
2. Browser tests assert the production static bytes use the approved token values and structural classes.
3. Header acceptance verifies system/node identity, connection state, data age, CPU p50, running processes, and total processes are visible after a complete response.
4. State tests verify distinct current, stale, partial, empty, first-load failure, and later-disconnect treatments without relying on color alone.
5. Controls retain all five ranges, node switching, username filtering, `aria-pressed`, keyboard focus, and exact request parameters.
6. Chart acceptance retains exact transformed arrays, null gaps, modes, attribution, units, lifecycle counts, and drawn pixels.
7. Responsive acceptance covers a desktop viewport and a 400-pixel viewport, including no horizontal overflow and bounded chart wrappers.
8. An external-request guard continues to prove that no production interaction contacts a non-local host.
9. Focus and state colors are checked for sufficient contrast against their actual surfaces.

### Visual acceptance

Capture production dashboard screenshots using the existing mutable production-shaped browser fixture:

- desktop complete/current;
- desktop disconnected with retained data;
- 400-pixel complete/current;
- 400-pixel partial or stale.

Inspect both pixels and dimensions. Acceptance requires:

- clear correspondence with PBS Monitor's visual family;
- no clipped text, overlap, unreadable chart labels, excessive whitespace, or horizontal overflow;
- visible distinction among connectivity and freshness states;
- bounded chart dimensions;
- all primary controls visible and discoverable.

### Repository gate

Before integration:

- run focused web and browser suites through the project venv;
- run the full repository suite;
- compare any failure only against the accepted base `be0bb68ee7133c07acbfad31cb6a640472ef1e0a`;
- obtain an independent exact-head review;
- block integration on every Critical or Important finding.

## Implementation boundaries

Expected production files are limited to:

- `node_monitor/web/static/index.html`;
- `node_monitor/web/static/styles.css`;
- `node_monitor/web/static/app.js` only if semantic state classes cannot be expressed from existing markup and state updates;
- browser tests and narrowly relevant web packaging/static tests.

No collector, database, SQL, daemon, TCP listener, or deployment file should change for this feature.

## Rollback

This is a static frontend increment with no persistent-state migration. Rollback is a source revert to the prior static assets. The API and database remain compatible throughout.
