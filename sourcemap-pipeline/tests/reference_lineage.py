"""Independent from-scratch reference for trigger-operation lineage.

The production implementation in :mod:`smpipe.lineage` uses iterative
Tarjan SCCs plus capped condensation propagation.  This reference shares no
code with it and deliberately uses the simplest obvious algorithm instead:

* reachability is recomputed per node by a plain depth-first walk that
  keeps the *full* reachable sets (no two-item cap);
* shortest paths are found by an independent BFS implementation;
* records are folded with plain dictionaries keyed by full node identity.

Tests only compare final values, so any shared blind spot in one style is
unlikely to hide in the other.
"""

from __future__ import annotations

from typing import Any

ACTION = "action"
WORK = "work"
ERROR = "error"


class ReferenceLineage:
    def __init__(self) -> None:
        # run_id -> node_id -> kind
        self.kinds: dict[str, dict[str, str]] = {}
        # run_id -> node_id -> set(parent_id)
        self.parents: dict[str, dict[str, set[str]]] = {}
        # event_id -> (run_id, node_id)
        self.bindings: dict[str, tuple[str, str]] = {}

    def add_node(self, run_id: str, node_id: str, kind: str) -> None:
        table = self.kinds.setdefault(run_id, {})
        if node_id in table:
            if table[node_id] != kind:
                raise ValueError(
                    f"node conflict for ({run_id!r}, {node_id!r})"
                )
            return
        table[node_id] = kind
        self.parents.setdefault(run_id, {}).setdefault(node_id, set())

    def add_work(
        self, run_id: str, node_id: str, parent_ids: list[str]
    ) -> None:
        self.add_node(run_id, node_id, WORK)
        table = self.parents[run_id]
        new_parents = set(parent_ids)
        existing = table[node_id]
        if existing and existing != new_parents:
            raise ValueError(
                f"parent-set conflict for ({run_id!r}, {node_id!r})"
            )
        table[node_id] = new_parents

    def bind_error(
        self, event_id: str, run_id: str, node_id: str
    ) -> None:
        if event_id in self.bindings:
            if self.bindings[event_id] != (run_id, node_id):
                raise ValueError(f"binding conflict for {event_id!r}")
            return
        self.bindings[event_id] = (run_id, node_id)

    # --------------------------------------------------------- graph facts
    def missing_parents(self, run_id: str) -> set[str]:
        defined = self.kinds.get(run_id, {})
        gap: set[str] = set()
        for upstream in self.parents.get(run_id, {}).values():
            for parent in upstream:
                if parent not in defined:
                    gap.add(parent)
        return gap

    def _reachable(self, run_id: str, start: str) -> set[str]:
        """Full reachable node-id set by plain DFS (cycles tolerated)."""
        defined = self.kinds[run_id]
        seen: set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in seen or node not in defined:
                continue
            seen.add(node)
            for parent in self.parents[run_id].get(node, set()):
                if parent not in seen:
                    stack.append(parent)
        return seen

    def _reaches_missing(self, run_id: str, start: str) -> bool:
        defined = self.kinds[run_id]
        seen: set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            for parent in self.parents[run_id].get(node, set()):
                if parent not in defined:
                    return True
                if parent not in seen:
                    stack.append(parent)
        return False

    def _reaches_cycle(self, run_id: str, start: str) -> bool:
        """Cycle reachability via a coloring DFS (white/grey/black)."""
        defined = self.kinds[run_id]
        color: dict[str, int] = {}

        def visit(node: str) -> bool:
            color[node] = 1
            for parent in self.parents[run_id].get(node, set()):
                if parent not in defined:
                    continue
                state = color.get(parent, 0)
                if state == 1:
                    return True
                if state == 0 and visit(parent):
                    return True
            color[node] = 2
            return False

        return visit(start)

    def reachable_actions(self, run_id: str, start: str) -> list[str]:
        defined = self.kinds[run_id]
        return sorted(
            node for node in self._reachable(run_id, start)
            if defined[node] == ACTION
        )

    def shortest_path(
        self, run_id: str, start: str, target: str
    ) -> list[str] | None:
        """Shortest path with lexicographic sequence tie-break (BFS)."""
        defined = self.kinds[run_id]
        if start not in defined or target not in defined:
            return None
        if start == target:
            return [start]
        predecessor = {start: start}
        frontier = [start]
        while frontier:
            next_frontier: list[str] = []
            for node in frontier:
                for neighbour in sorted(self.parents[run_id].get(node, set())):
                    if neighbour not in defined or neighbour in predecessor:
                        continue
                    predecessor[neighbour] = node
                    if neighbour == target:
                        path = [target]
                        while path[-1] != start:
                            path.append(predecessor[path[-1]])
                        path.reverse()
                        return path
                    next_frontier.append(neighbour)
            frontier = next_frontier
        return None

    # ------------------------------------------------------------- rows
    def row_for(
        self, event_id: str, universe: set[str]
    ) -> dict[str, Any] | None:
        if event_id not in universe:
            return None
        if event_id not in self.bindings:
            return {
                "event_id": event_id,
                "run_id": None,
                "node_id": None,
                "reaches_missing": False,
                "reaches_cycle": False,
                "status": "no_anchor",
                "candidates": [],
            }
        run_id, node_id = self.bindings[event_id]
        defined = self.kinds.get(run_id, {})
        if node_id not in defined:
            return {
                "event_id": event_id,
                "run_id": run_id,
                "node_id": node_id,
                "reaches_missing": True,
                "reaches_cycle": False,
                "status": "unresolved",
                "candidates": [],
            }
        actions = self.reachable_actions(run_id, node_id)
        gap = self._reaches_missing(run_id, node_id)
        cycle = self._reaches_cycle(run_id, node_id)
        top_two = actions[:2]
        if len(actions) >= 2:
            status = "ambiguous"
        elif len(actions) == 1 and not gap and not cycle:
            status = "attributed"
        else:
            status = "unresolved"
        candidates = [
            {"action_id": action,
             "path": self.shortest_path(run_id, node_id, action)}
            for action in top_two
        ]
        return {
            "event_id": event_id,
            "run_id": run_id,
            "node_id": node_id,
            "reaches_missing": gap,
            "reaches_cycle": cycle,
            "status": status,
            "candidates": candidates,
        }

    def all_rows(self, universe: set[str]) -> list[dict[str, Any]]:
        rows = [
            row for event_id in sorted(universe)
            if (row := self.row_for(event_id, universe)) is not None
        ]
        return rows
