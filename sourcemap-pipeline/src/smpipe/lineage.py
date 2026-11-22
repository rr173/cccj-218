"""Trigger-operation lineage export from a sealed enrichment database.

The enrichment database answers *where in source code* an error frame lands.
It cannot say which user action started the asynchronous work whose
confluence eventually reported the error.  This module joins two offline
inputs into a per-event lineage table:

* a **sealed enrichment database** (the ``enrich`` SQLite output).  It is
  opened immutable and only its ``frames`` table is read, solely to derive
  the universe of known ``event_id`` values.  Source maps, exposure tables
  and reverse indexes are never touched, and the database is never written;
* an unordered JSONL stream of browser async telemetry with three record
  types:

  - ``action``: an actual user operation, carrying ``run_id`` + ``node_id``;
  - ``work``: an asynchronous job carrying ``run_id`` + ``node_id`` and a
    non-empty ``parents`` list of upstream node ids that started it;
  - ``error``: a reported error carrying ``event_id``, ``run_id`` and the
    reporting ``node_id``.

A node's full identity is ``(run_id, node_id)``: parent edges only resolve
inside the same run; a same-named node in another run is never borrowed.
Parent lists are interpreted as sets, so reordered lists and equivalent
re-delivered records fold together.  Conflicting definitions (one node with
two kinds or two parent sets), one event bound to two nodes, or an
``error`` whose ``event_id`` is absent from the frame universe are input
conflicts and reject the whole export before the output file is touched.
Missing parents or missing reporting nodes are sampling gaps: they are
flagged on the affected rows instead of rejecting the telemetry.

The table describes the *observed trigger chain*; it never diagnoses code
errors.

Scaling rules
-------------

One export may cover many independent browser runs.  Only one run's graph
is held in memory at a time; the parsed telemetry, the event universe and
the per-event results are all spooled in a temporary SQLite database that
is deleted when the export finishes.  Many errors inside one run often
share the same upstream subgraph, so per-node lineage is computed once:

* strongly connected components (iterative Tarjan, O(V+E)) locate every
  directed cycle;
* the condensation DAG is propagated in emission (reverse-topological)
  order with every reachable-action set capped at the two smallest ids, so
  node/edge work grows with the graph size and never with the error count,
  the number of reachable actions or the number of path combinations;
* only the final per-result review paths are reconstructed on demand,
  memoised per distinct reporting anchor.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import Any

from .errors import EnrichDatabaseError, LineageConflict, TelemetryError

ACTION = "action"
WORK = "work"
ERROR = "error"
RECORD_TYPES = (ACTION, WORK, ERROR)

NO_ANCHOR = "no_anchor"
AMBIGUOUS = "ambiguous"
ATTRIBUTED = "attributed"
UNRESOLVED = "unresolved"

WAL_SUFFIX = "-wal"
INGEST_BATCH = 2000
TOP_N_ACTIONS = 2


# --------------------------------------------------------------------------
# Pure graph analysis (no SQLite; unit tested directly)
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class NodeLineage:
    """Lineage facts reachable from one node along parent edges."""

    reaches_missing: bool
    reaches_cycle: bool
    # The two smallest action node ids reachable (the node itself counts),
    # capped so propagation cost stays independent of reachable set size.
    top_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunGraphAnalysis:
    """Whole-run result of the O(V+E) lineage computation."""

    lineage: dict[str, NodeLineage]
    adjacency: dict[str, list[str]]
    cyclic_components: int


def merge_top_actions(*groups: tuple[str, ...]) -> tuple[str, ...]:
    """Union several capped action-id groups, keeping the two smallest."""
    merged: set[str] = set()
    for group in groups:
        merged.update(group)
    return tuple(sorted(merged)[:TOP_N_ACTIONS])


def strongly_connected_components(
    node_ids: list[str], adj: dict[str, list[str]]
) -> list[list[str]]:
    """Iterative Tarjan SCC over sorted node ids and sorted adjacency.

    Components are returned in Tarjan emission order: sink SCCs first,
    i.e. reverse topological order of the condensation DAG.  Iteration is
    explicit so a long chain cannot exhaust the Python recursion limit.
    """
    index_of: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    stack: list[str] = []
    components: list[list[str]] = []
    counter = 0

    for root in node_ids:
        if root in index_of:
            continue
        index_of[root] = lowlink[root] = counter
        counter += 1
        stack.append(root)
        on_stack[root] = True
        call_stack: list[list[Any]] = [[root, 0]]
        while call_stack:
            frame = call_stack[-1]
            node = frame[0]
            edge_pos = frame[1]
            neighbours = adj[node]
            if edge_pos < len(neighbours):
                nxt = neighbours[edge_pos]
                frame[1] = edge_pos + 1
                if nxt not in index_of:
                    index_of[nxt] = lowlink[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack[nxt] = True
                    call_stack.append([nxt, 0])
                elif on_stack.get(nxt, False):
                    if index_of[nxt] < lowlink[node]:
                        lowlink[node] = index_of[nxt]
            else:
                call_stack.pop()
                if call_stack:
                    parent = call_stack[-1][0]
                    if lowlink[node] < lowlink[parent]:
                        lowlink[parent] = lowlink[node]
                if lowlink[node] == index_of[node]:
                    component: list[str] = []
                    while True:
                        member = stack.pop()
                        on_stack[member] = False
                        component.append(member)
                        if member == node:
                            break
                    components.append(component)
    return components


def analyze_run_graph(
    kinds: dict[str, str],
    parents: dict[str, list[str]],
) -> RunGraphAnalysis:
    """Compute gap/cycle flags and the top-2 reachable actions per node.

    ``kinds`` maps every defined node id to ``action``/``work`` and
    ``parents`` maps node ids to the parent ids they declared.  Parent ids
    absent from ``kinds`` are sampling gaps: they are detected here, never
    treated as conflicts.  The work is O(V+E): one Tarjan pass plus one
    capped propagation over the condensation DAG, independent of how many
    error anchors will later query the result.
    """
    node_ids = sorted(kinds)
    adj: dict[str, list[str]] = {}
    self_loops: set[str] = set()
    missing_ref_nodes: set[str] = set()
    for node in node_ids:
        defined_neighbours: set[str] = set()
        for parent in parents.get(node, ()):
            if parent in kinds:
                defined_neighbours.add(parent)
                if parent == node:
                    self_loops.add(node)
            else:
                missing_ref_nodes.add(node)
        adj[node] = sorted(defined_neighbours)

    components = strongly_connected_components(node_ids, adj)
    scc_count = len(components)
    scc_of: dict[str, int] = {}
    own_actions: list[tuple[str, ...]] = []
    cyclic: list[bool] = []
    scc_missing_ref: list[bool] = []
    for index, component in enumerate(components):
        for member in component:
            scc_of[member] = index
        own_actions.append(tuple(sorted(
            member for member in component if kinds[member] == ACTION
        )))
        is_cyclic = len(component) > 1 or any(
            member in self_loops for member in component
        )
        cyclic.append(is_cyclic)
        scc_missing_ref.append(
            any(member in missing_ref_nodes for member in component)
        )

    successors: list[set[int]] = [set() for _ in range(scc_count)]
    for node in node_ids:
        source = scc_of[node]
        for neighbour in adj[node]:
            target = scc_of[neighbour]
            if target != source:
                successors[source].add(target)

    # Components were emitted sink-first, so every successor SCC already
    # has its final values when the current one is processed.
    top_actions: list[tuple[str, ...]] = [()] * scc_count
    reaches_missing: list[bool] = [False] * scc_count
    reaches_cycle: list[bool] = [False] * scc_count
    for index in range(scc_count):
        top = own_actions[index][:TOP_N_ACTIONS]
        gap = scc_missing_ref[index]
        cycle = cyclic[index]
        for successor in sorted(successors[index]):
            top = merge_top_actions(top, top_actions[successor])
            gap = gap or reaches_missing[successor]
            cycle = cycle or reaches_cycle[successor]
        top_actions[index] = top
        reaches_missing[index] = gap
        reaches_cycle[index] = cycle

    lineage = {
        node: NodeLineage(
            reaches_missing=reaches_missing[scc_of[node]],
            reaches_cycle=reaches_cycle[scc_of[node]],
            top_actions=top_actions[scc_of[node]],
        )
        for node in node_ids
    }
    return RunGraphAnalysis(
        lineage=lineage,
        adjacency=adj,
        cyclic_components=sum(1 for flag in cyclic if flag),
    )


def shortest_lineage_path(
    adj: dict[str, list[str]], anchor: str, target: str
) -> list[str] | None:
    """Shortest anchor -> target path along parent edges.

    Ties are broken by the lexicographically smallest node-id sequence.
    ``adj`` lists must be sorted; BFS then discovers same-distance nodes in
    lexicographic path order, so the first discovered predecessor of every
    node already yields its lexicographically smallest shortest path.
    Both endpoints are included.  Returns None when unreachable.
    """
    if anchor == target:
        return [anchor]
    predecessor: dict[str, str] = {anchor: anchor}
    frontier = [anchor]
    while frontier:
        next_frontier: list[str] = []
        for node in frontier:
            for neighbour in adj[node]:
                if neighbour not in predecessor:
                    predecessor[neighbour] = node
                    if neighbour == target:
                        path = [target]
                        while path[-1] != anchor:
                            path.append(predecessor[path[-1]])
                        path.reverse()
                        return path
                    next_frontier.append(neighbour)
        frontier = next_frontier
    return None


def classify(lineage: NodeLineage | None) -> str:
    """Status of a bound event given its anchor's lineage.

    ``None`` stands for a bound anchor that no telemetry record defines:
    a sampling gap with no graph to traverse, never an input conflict.
    """
    if lineage is None:
        return UNRESOLVED
    if len(lineage.top_actions) >= TOP_N_ACTIONS:
        return AMBIGUOUS
    if (len(lineage.top_actions) == 1
            and not lineage.reaches_missing
            and not lineage.reaches_cycle):
        return ATTRIBUTED
    return UNRESOLVED


# --------------------------------------------------------------------------
# Telemetry record validation (pure; raises with 1-based line numbers)
# --------------------------------------------------------------------------
def _require_nonempty_str(value: Any, field: str, lineno: int) -> str:
    if not isinstance(value, str) or value == "":
        raise TelemetryError(
            f"line {lineno}: field {field!r} must be a non-empty string"
        )
    return value


def parse_telemetry_record(obj: Any, lineno: int) -> dict[str, Any]:
    """Validate one decoded JSONL record and return its normalized fields.

    Equivalent re-deliveries normalize to identical content so the caller
    can fold them by primary key.
    """
    if not isinstance(obj, dict):
        raise TelemetryError(f"line {lineno}: telemetry record must be an object")
    kind = obj.get("type")
    if kind not in RECORD_TYPES:
        raise TelemetryError(
            f"line {lineno}: field 'type' must be one of "
            + "/".join(RECORD_TYPES)
        )
    if kind == ACTION:
        unknown = set(obj) - {"type", "run_id", "node_id"}
        if unknown:
            raise TelemetryError(
                f"line {lineno}: action record has unknown fields: "
                f"{sorted(unknown)}"
            )
        run_id = _require_nonempty_str(obj.get("run_id"), "run_id", lineno)
        node_id = _require_nonempty_str(obj.get("node_id"), "node_id", lineno)
        return {"type": ACTION, "run_id": run_id, "node_id": node_id}

    if kind == WORK:
        unknown = set(obj) - {"type", "run_id", "node_id", "parents"}
        if unknown:
            raise TelemetryError(
                f"line {lineno}: work record has unknown fields: "
                f"{sorted(unknown)}"
            )
        run_id = _require_nonempty_str(obj.get("run_id"), "run_id", lineno)
        node_id = _require_nonempty_str(obj.get("node_id"), "node_id", lineno)
        parents = obj.get("parents")
        if not isinstance(parents, list) or not parents:
            raise TelemetryError(
                f"line {lineno}: field 'parents' must be a non-empty array"
            )
        parent_ids = [
            _require_nonempty_str(parent, "parents[]", lineno)
            for parent in parents
        ]
        # Parent lists are sets: dedupe and canonicalize the order.
        return {
            "type": WORK, "run_id": run_id, "node_id": node_id,
            "parents": sorted(set(parent_ids)),
        }

    unknown = set(obj) - {"type", "event_id", "run_id", "node_id"}
    if unknown:
        raise TelemetryError(
            f"line {lineno}: error record has unknown fields: {sorted(unknown)}"
        )
    event_id = _require_nonempty_str(obj.get("event_id"), "event_id", lineno)
    run_id = _require_nonempty_str(obj.get("run_id"), "run_id", lineno)
    node_id = _require_nonempty_str(obj.get("node_id"), "node_id", lineno)
    return {"type": ERROR, "event_id": event_id,
            "run_id": run_id, "node_id": node_id}


# --------------------------------------------------------------------------
# Sealed enrichment database (read-only event universe)
# --------------------------------------------------------------------------
def _open_sealed_enrich(path: str) -> sqlite3.Connection:
    if not os.path.isfile(path):
        raise EnrichDatabaseError(f"enrichment database not found: {path}")
    wal = path + WAL_SUFFIX
    if os.path.exists(wal) and os.path.getsize(wal) > 0:
        raise EnrichDatabaseError(
            f"{path!r} still has a non-empty WAL; checkpoint and seal it "
            "before use"
        )
    try:
        conn = sqlite3.connect(
            f"file:{os.path.abspath(path)}?immutable=1",
            uri=True, check_same_thread=False,
        )
    except sqlite3.DatabaseError as exc:
        raise EnrichDatabaseError(f"cannot open {path!r}: {exc}") from exc
    try:
        conn.execute("SELECT 1 FROM frames LIMIT 1")
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise EnrichDatabaseError(
            f"{path!r} is not a sealed enrichment database: {exc}"
        ) from exc
    return conn


# --------------------------------------------------------------------------
# Disk-spooled export
# --------------------------------------------------------------------------
class _PathFinder:
    """Memoised shortest-path reconstruction for one run's sorted graph."""

    def __init__(self, adj: dict[str, list[str]]) -> None:
        self._adj = adj
        self._tables: dict[str, dict[str, list[str] | None]] = {}
        self.bfs_runs = 0
        self.path_lookups = 0
        self.path_cache_hits = 0

    def shortest(self, anchor: str, target: str) -> list[str] | None:
        self.path_lookups += 1
        table = self._tables.get(anchor)
        if table is None:
            table = {}
            self._tables[anchor] = table
            self.bfs_runs += 1
        if target in table:
            self.path_cache_hits += 1
            return table[target]
        path = shortest_lineage_path(self._adj, anchor, target)
        table[target] = path
        return path


class LineageExporter:
    """Stream telemetry into scratch SQLite and emit the lineage JSONL.

    All intermediate state lives in a temporary directory that is removed
    on :meth:`close`; the sealed enrichment database is attached immutable
    and never written.  The destination file is replaced only after every
    input record has been validated and every run graph analysed, so a bad
    record discovered on the last line preserves any previous export.
    """

    def __init__(self, enrich_path: str) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="smpipe-lineage-")
        self._enrich: sqlite3.Connection | None = None
        try:
            self.scratch_path = os.path.join(
                self._tmpdir, "lineage-scratch.db"
            )
            self._conn = sqlite3.connect(self.scratch_path)
            self._conn.execute("PRAGMA journal_mode=DELETE")
            self._conn.execute("PRAGMA synchronous=OFF")
            self._conn.execute("PRAGMA temp_store=MEMORY")
            self._init_schema()
            self._enrich = _open_sealed_enrich(enrich_path)
            self.metrics = {
                "telemetry_records": 0,
                "records_folded": 0,
                "runs_processed": 0,
                "nodes_defined": 0,
                "parent_edges_stored": 0,
                "missing_parent_refs": 0,
                "cyclic_components": 0,
                "path_bfs_runs": 0,
                "path_lookups": 0,
                "path_cache_hits": 0,
                "rows_by_status": {
                    NO_ANCHOR: 0, AMBIGUOUS: 0, ATTRIBUTED: 0,
                    UNRESOLVED: 0,
                },
            }
            self._pending = 0
            self._load_universe()
        except BaseException:
            # A failed constructor still owns its scratch directory.
            self.close()
            raise

    # ------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE universe(
                event_id TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE runs(
                run_id TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE nodes(
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                PRIMARY KEY(run_id, node_id)
            ) WITHOUT ROWID;
            CREATE TABLE parent_sigs(
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                sig TEXT NOT NULL,
                PRIMARY KEY(run_id, node_id)
            ) WITHOUT ROWID;
            CREATE TABLE parents(
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                PRIMARY KEY(run_id, node_id, parent_id)
            ) WITHOUT ROWID;
            CREATE TABLE bindings(
                event_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE results(
                event_id TEXT PRIMARY KEY,
                run_id TEXT,
                node_id TEXT,
                reaches_missing INTEGER NOT NULL,
                reaches_cycle INTEGER NOT NULL,
                status TEXT NOT NULL,
                candidates_json TEXT NOT NULL
            ) WITHOUT ROWID;
            """
        )

    def close(self) -> None:
        conn = getattr(self, "_conn", None)
        if conn is not None:
            conn.close()
        if self._enrich is not None:
            self._enrich.close()
        # Scratch data must never survive the export.
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def __enter__(self) -> "LineageExporter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------- universe from enrich
    def _load_universe(self) -> None:
        """Copy distinct event ids of attributed frames into scratch.

        Only frames that actually have an enrichment result define the
        event universe; an event with several frames still appears once.
        A frame that failed attribution keeps its event included -- the
        lineage table is about the trigger chain, not frame status.
        """
        rows = self._enrich.execute(
            "SELECT DISTINCT event_id FROM frames "
            "WHERE result_json IS NOT NULL ORDER BY event_id"
        )
        batch: list[tuple[str]] = []
        total = 0
        for (event_id,) in rows:
            batch.append((event_id,))
            if len(batch) >= INGEST_BATCH:
                self._conn.executemany(
                    "INSERT INTO universe(event_id) VALUES(?)", batch)
                total += len(batch)
                batch.clear()
        if batch:
            self._conn.executemany(
                "INSERT INTO universe(event_id) VALUES(?)", batch)
            total += len(batch)
        self._conn.commit()
        self.metrics["events_in_universe"] = total

    # ----------------------------------------------------------- ingestion
    def _flush(self) -> None:
        if self._pending >= INGEST_BATCH:
            self._conn.commit()
            self._pending = 0

    def ingest_telemetry(self, telemetry_path: str) -> None:
        try:
            handle = open(telemetry_path, "rb")
        except OSError as exc:
            raise TelemetryError(
                f"telemetry file not readable: {telemetry_path}: {exc}"
            ) from exc
        with handle:
            raw = handle.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise TelemetryError(
                f"telemetry file is not valid UTF-8: {exc}"
            ) from exc

        for lineno, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except ValueError as exc:
                raise TelemetryError(
                    f"line {lineno}: invalid JSON: {exc}"
                ) from exc
            record = parse_telemetry_record(obj, lineno)
            self._ingest_record(record, lineno)
            self.metrics["telemetry_records"] += 1
            self._flush()
        self._conn.commit()

    def _ingest_record(self, record: dict[str, Any], lineno: int) -> None:
        conn = self._conn
        kind = record["type"]
        if kind == ERROR:
            event_id = record["event_id"]
            known = conn.execute(
                "SELECT 1 FROM universe WHERE event_id=?", (event_id,)
            ).fetchone()
            if known is None:
                raise LineageConflict(
                    f"line {lineno}: error event_id {event_id!r} is not part "
                    "of the enrichment frame universe"
                )
            existing = conn.execute(
                "SELECT run_id, node_id FROM bindings WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if existing is not None:
                if tuple(existing) == (record["run_id"], record["node_id"]):
                    self.metrics["records_folded"] += 1
                    return
                raise LineageConflict(
                    f"line {lineno}: event_id {event_id!r} already bound to "
                    f"node ({existing[0]!r}, {existing[1]!r}), cannot rebind "
                    f"to ({record['run_id']!r}, {record['node_id']!r})"
                )
            conn.execute(
                "INSERT INTO bindings(event_id, run_id, node_id) "
                "VALUES(?, ?, ?)",
                (event_id, record["run_id"], record["node_id"]),
            )
            self._pending += 1
            return

        run_id = record["run_id"]
        node_id = record["node_id"]
        conn.execute("INSERT OR IGNORE INTO runs(run_id) VALUES(?)", (run_id,))
        existing = conn.execute(
            "SELECT kind FROM nodes WHERE run_id=? AND node_id=?",
            (run_id, node_id),
        ).fetchone()
        if existing is not None:
            if existing[0] != kind:
                raise LineageConflict(
                    f"line {lineno}: node ({run_id!r}, {node_id!r}) is "
                    f"already defined as {existing[0]!r}, cannot redefine it "
                    f"as {kind!r}"
                )
            if kind == ACTION:
                self.metrics["records_folded"] += 1
                return
        else:
            conn.execute(
                "INSERT INTO nodes(run_id, node_id, kind) VALUES(?, ?, ?)",
                (run_id, node_id, kind),
            )
            self.metrics["nodes_defined"] += 1
            self._pending += 1

        if kind == WORK:
            parent_ids = record["parents"]
            sig = json.dumps(parent_ids, ensure_ascii=False, separators=(",", ":"))
            existing_sig = conn.execute(
                "SELECT sig FROM parent_sigs WHERE run_id=? AND node_id=?",
                (run_id, node_id),
            ).fetchone()
            if existing_sig is not None:
                if existing_sig[0] == sig:
                    self.metrics["records_folded"] += 1
                    return
                raise LineageConflict(
                    f"line {lineno}: work node ({run_id!r}, {node_id!r}) "
                    "redefined with a different parent set"
                )
            conn.execute(
                "INSERT INTO parent_sigs(run_id, node_id, sig) "
                "VALUES(?, ?, ?)",
                (run_id, node_id, sig),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO parents(run_id, node_id, parent_id) "
                "VALUES(?, ?, ?)",
                [(run_id, node_id, parent) for parent in parent_ids],
            )
            self.metrics["parent_edges_stored"] += len(parent_ids)
            self._pending += 1

    # --------------------------------------------------------- per-run work
    def _load_run_graph(
        self, run_id: str
    ) -> tuple[dict[str, str], dict[str, list[str]], set[str]]:
        """Load one run's graph; only this run's nodes stay in memory."""
        conn = self._conn
        kinds = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT node_id, kind FROM nodes WHERE run_id=? ORDER BY node_id",
                (run_id,),
            )
        }
        parents: dict[str, list[str]] = {node: [] for node in kinds}
        missing: set[str] = set()
        for node_id, parent_id in conn.execute(
            "SELECT node_id, parent_id FROM parents "
            "WHERE run_id=? ORDER BY node_id, parent_id",
            (run_id,),
        ):
            parents.setdefault(node_id, []).append(parent_id)
            if parent_id not in kinds:
                missing.add(parent_id)
        return kinds, parents, missing

    def _all_run_ids(self) -> list[str]:
        """Runs that define nodes *or* only carry error bindings.

        A run consisting solely of error records pointing at undefined
        anchors must still produce unresolved rows; it never appears in
        ``runs`` because that table is fed by node definitions.
        """
        rows = self._conn.execute(
            "SELECT run_id FROM runs "
            "UNION SELECT DISTINCT run_id FROM bindings "
            "ORDER BY run_id"
        ).fetchall()
        return [row[0] for row in rows]

    def build_results(self) -> None:
        conn = self._conn
        for run_id in self._all_run_ids():
            self._build_run(run_id)
        self._build_unbound_rows()
        conn.commit()

    def _build_run(self, run_id: str) -> None:
        conn = self._conn
        kinds, parents, missing = self._load_run_graph(run_id)
        self.metrics["runs_processed"] += 1
        self.metrics["missing_parent_refs"] += len(missing)
        analysis = analyze_run_graph(kinds, parents)
        self.metrics["cyclic_components"] += analysis.cyclic_components
        finder = _PathFinder(analysis.adjacency)

        bound = conn.execute(
            "SELECT event_id, node_id FROM bindings WHERE run_id=? "
            "ORDER BY event_id",
            (run_id,),
        ).fetchall()
        for event_id, node_id in bound:
            node_lineage = analysis.lineage.get(node_id)
            status = classify(node_lineage)
            candidates: list[dict[str, Any]] = []
            if node_lineage is not None:
                for action_id in node_lineage.top_actions:
                    path = finder.shortest(node_id, action_id)
                    # A top action is reachable by construction; keep the
                    # guard explicit rather than asserting in production.
                    if path is not None:
                        candidates.append(
                            {"action_id": action_id, "path": path}
                        )
            if node_lineage is None:
                # The bound reporting node has no defining record: this is
                # a sampling gap, not an input conflict.
                reaches_missing, reaches_cycle = True, False
            else:
                reaches_missing = node_lineage.reaches_missing
                reaches_cycle = node_lineage.reaches_cycle
            self._insert_result(
                event_id, run_id, node_id,
                reaches_missing=reaches_missing,
                reaches_cycle=reaches_cycle,
                status=status,
                candidates=candidates,
            )
        self.metrics["path_bfs_runs"] += finder.bfs_runs
        self.metrics["path_lookups"] += finder.path_lookups
        self.metrics["path_cache_hits"] += finder.path_cache_hits

    def _insert_result(
        self, event_id: str, run_id: str | None, node_id: str | None,
        *, reaches_missing: bool, reaches_cycle: bool, status: str,
        candidates: list[dict[str, Any]],
    ) -> None:
        self._conn.execute(
            "INSERT INTO results(event_id, run_id, node_id, reaches_missing, "
            "reaches_cycle, status, candidates_json) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                event_id, run_id, node_id,
                1 if reaches_missing else 0,
                1 if reaches_cycle else 0,
                status,
                json.dumps(candidates, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.metrics["rows_by_status"][status] += 1

    def _build_unbound_rows(self) -> None:
        """Every universe event without an error record gets no_anchor."""
        rows = self._conn.execute(
            "SELECT u.event_id FROM universe u "
            "LEFT JOIN bindings b ON u.event_id = b.event_id "
            "WHERE b.event_id IS NULL ORDER BY u.event_id"
        ).fetchall()
        for (event_id,) in rows:
            self._insert_result(
                event_id, None, None,
                reaches_missing=False, reaches_cycle=False,
                status=NO_ANCHOR, candidates=[],
            )

    # -------------------------------------------------------------- output
    def write_output(self, out_path: str) -> int:
        out_dir = os.path.dirname(os.path.abspath(out_path))
        if not os.path.isdir(out_dir):
            raise TelemetryError(
                f"output directory does not exist: {out_dir}"
            )
        tmp_path = os.path.join(
            out_dir, f".{os.path.basename(out_path)}.tmp.{os.getpid()}"
        )
        rows = self._conn.execute(
            "SELECT event_id, run_id, node_id, reaches_missing, "
            "reaches_cycle, status, candidates_json FROM results "
            "ORDER BY event_id"
        )
        written = 0
        try:
            with open(tmp_path, "w", encoding="utf-8") as handle:
                for row in rows:
                    payload = {
                        "event_id": row[0],
                        "run_id": row[1],
                        "node_id": row[2],
                        "reaches_missing": bool(row[3]),
                        "reaches_cycle": bool(row[4]),
                        "status": row[5],
                        "candidates": json.loads(row[6]),
                    }
                    handle.write(
                        json.dumps(payload, ensure_ascii=False,
                                   sort_keys=True) + "\n"
                    )
                    written += 1
            os.replace(tmp_path, out_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        self.metrics["rows_written"] = written
        return written


def export_lineage(
    enrich_path: str, telemetry_path: str, out_path: str
) -> dict[str, Any]:
    """Run the full offline lineage export and return a counter report."""
    with LineageExporter(enrich_path) as exporter:
        exporter.ingest_telemetry(telemetry_path)
        exporter.build_results()
        exporter.write_output(out_path)
        report = dict(exporter.metrics)
    return report
