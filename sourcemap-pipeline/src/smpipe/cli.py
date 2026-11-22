"""Command line interface: ingest-build / enrich / explain-frame / groups.

All commands work against a local on-disk store and never touch the network.
The exposure first-error fact table adds ``import-facts`` / ``export-facts``
(plus ``facts-metrics``) backed by a separate working SQLite database that
reads a sealed enrichment database strictly read-only.  The source-range
reverse index adds ``build-reverse-index`` / ``query-reverse-index``: the
builder reads a sealed build database read-only, and the query side needs
only the self-contained index file.  ``export-lineage`` joins a sealed
enrichment database with unordered async telemetry into a per-event
trigger-operation lineage table; the enrichment database stays immutable and
all intermediate state is spooled in a throwaway scratch database.
"""

from __future__ import annotations

import argparse
import json
import sys

from .errors import SmPipeError
from .facts import FactStore
from .lineage import export_lineage
from .pipeline import (
    compute_groups,
    ingest_frames,
    ingest_manifest,
)
from .reverse import ReverseIndex, ReverseIndexBuilder
from .store import Store


def _print(value) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smpipe",
        description="Offline source-map enrichment pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser(
        "ingest-build", help="ingest a release manifest and its build material"
    )
    p_ingest.add_argument("--store", required=True)
    p_ingest.add_argument("--manifest", required=True)

    p_enrich = sub.add_parser(
        "enrich", help="enrich a JSONL file of normalized frames"
    )
    p_enrich.add_argument("--store", required=True)
    p_enrich.add_argument("--frames", required=True)
    p_enrich.add_argument("--out", help="optional JSONL result path")

    p_explain = sub.add_parser(
        "explain-frame",
        help="show the per-layer trace for one stored frame",
    )
    p_explain.add_argument("--store", required=True)
    p_explain.add_argument("--event-id", required=True)
    p_explain.add_argument("--frame-index", required=True, type=int)

    p_groups = sub.add_parser("groups", help="compute reviewable error groups")
    p_groups.add_argument("--store", required=True)
    p_groups.add_argument("--out", help="optional JSON output path")

    p_metrics = sub.add_parser(
        "metrics", help="dump store access counters (cache/map access proof)"
    )
    p_metrics.add_argument("--store", required=True)

    p_reset = sub.add_parser("reset-metrics", help="zero the access counters")
    p_reset.add_argument("--store", required=True)

    p_import_facts = sub.add_parser(
        "import-facts",
        help="import one telemetry change batch into the exposure fact "
             "working database",
    )
    p_import_facts.add_argument("--work-db", required=True)
    p_import_facts.add_argument(
        "--enrich-db",
        help="sealed enrichment SQLite database (required when creating the "
             "working database)",
    )
    p_import_facts.add_argument("--batch", required=True)

    p_export_facts = sub.add_parser(
        "export-facts",
        help="export the current exposure first-error facts as JSONL",
    )
    p_export_facts.add_argument("--work-db", required=True)
    p_export_facts.add_argument(
        "--enrich-db",
        help="optional sealed enrichment database to rebind after reopening",
    )
    p_export_facts.add_argument("--out", help="optional JSONL output path")

    p_fact_metrics = sub.add_parser(
        "facts-metrics",
        help="dump fact-store access/maintenance counters and query plans",
    )
    p_fact_metrics.add_argument("--work-db", required=True)
    p_fact_metrics.add_argument(
        "--enrich-db",
        help="optional sealed enrichment database to rebind after reopening",
    )
    p_fact_metrics.add_argument(
        "--plan-event", help="event_id for an enrichment frame query plan"
    )
    p_fact_metrics.add_argument(
        "--plan-release", help="release_id paired with --plan-event"
    )

    p_rev_build = sub.add_parser(
        "build-reverse-index",
        help="build the source-range reverse index from a sealed build "
             "database (read-only; frames not required)",
    )
    p_rev_build.add_argument("--store", required=True,
                             help="sealed build SQLite database")
    p_rev_build.add_argument("--index", required=True,
                             help="reverse index file to create")

    p_rev_query = sub.add_parser(
        "query-reverse-index",
        help="find generated ranges for a source range (index file only)",
    )
    p_rev_query.add_argument("--index", required=True)
    p_rev_query.add_argument("--source", required=True,
                             help="exact source_identity from the index")
    p_rev_query.add_argument("--start-line", required=True, type=int,
                             help="zero-based start line (inclusive)")
    p_rev_query.add_argument("--start-col", required=True, type=int,
                             help="zero-based UTF-16 start column (inclusive)")
    p_rev_query.add_argument("--end-line", required=True, type=int,
                             help="zero-based end line (exclusive)")
    p_rev_query.add_argument("--end-col", required=True, type=int,
                             help="zero-based UTF-16 end column (exclusive)")
    p_rev_query.add_argument("--out", help="optional JSONL output path")
    p_rev_query.add_argument(
        "--explain", action="store_true",
        help="also print the query plan and index counters to stderr",
    )

    p_lineage = sub.add_parser(
        "export-lineage",
        help="export per-event trigger-operation lineage by joining a sealed "
             "enrichment database with async browser telemetry (read-only)",
    )
    p_lineage.add_argument(
        "--enrich-db", required=True,
        help="sealed enrichment SQLite database (opened immutable)",
    )
    p_lineage.add_argument(
        "--telemetry", required=True,
        help="unordered JSONL of action/work/error telemetry records",
    )
    p_lineage.add_argument(
        "--out", required=True,
        help="JSONL output path; replaced atomically only after the whole "
             "input has validated",
    )
    return parser


def _run_facts(args: argparse.Namespace) -> int:
    if args.command == "import-facts":
        with open(args.batch, "rb") as handle:
            raw = handle.read()
        facts = FactStore(args.work_db, enrich_path=args.enrich_db)
        try:
            _print(facts.import_batch(raw))
        finally:
            facts.close()
        return 0

    facts = FactStore(args.work_db, enrich_path=args.enrich_db)
    try:
        if args.command == "export-facts":
            rows = facts.export_facts()
            if args.out:
                with open(args.out, "w", encoding="utf-8") as handle:
                    for row in rows:
                        handle.write(
                            json.dumps(row, ensure_ascii=False,
                                       sort_keys=True) + "\n"
                        )
            else:
                for row in rows:
                    sys.stdout.write(
                        json.dumps(row, ensure_ascii=False,
                                   sort_keys=True) + "\n"
                    )
            _print({"fact_rows": len(rows), "out": args.out or "stdout"})
        elif args.command == "facts-metrics":
            payload = {
                "last_seq": facts.last_seq(),
                "metrics": facts.metrics_snapshot(),
            }
            if args.plan_event and args.plan_release:
                payload["enrich_frame_query_plan"] = facts.explain_enrich_plan(
                    args.plan_event, args.plan_release
                )
            _print(payload)
    finally:
        facts.close()
    return 0


def _run_reverse(args: argparse.Namespace) -> int:
    if args.command == "build-reverse-index":
        with ReverseIndexBuilder(args.store, args.index) as builder:
            report = builder.build()
        _print(report)
        return 0
    start = (args.start_line, args.start_col)
    end = (args.end_line, args.end_col)
    with ReverseIndex(args.index) as index:
        rows = index.query(args.source, start, end)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(
                        json.dumps(row, ensure_ascii=False, sort_keys=True)
                        + "\n"
                    )
        else:
            for row in rows:
                sys.stdout.write(
                    json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                )
        if args.explain:
            payload = {
                "rows": len(rows),
                "query_plan": index.explain_plan(args.source, start, end),
                "index": index.info(),
            }
            json.dump(payload, sys.stderr, ensure_ascii=False, indent=2,
                      sort_keys=True)
            sys.stderr.write("\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "export-lineage":
        try:
            report = export_lineage(args.enrich_db, args.telemetry, args.out)
        except SmPipeError as exc:
            print(f"{exc.reason}: {exc}", file=sys.stderr)
            return 2
        _print(report)
        return 0
    if args.command in ("import-facts", "export-facts", "facts-metrics"):
        try:
            return _run_facts(args)
        except SmPipeError as exc:
            print(f"{exc.reason}: {exc}", file=sys.stderr)
            return 2
    if args.command in ("build-reverse-index", "query-reverse-index"):
        try:
            return _run_reverse(args)
        except SmPipeError as exc:
            print(f"{exc.reason}: {exc}", file=sys.stderr)
            return 2

    store = None
    try:
        store = Store(args.store)
        if args.command == "ingest-build":
            report = ingest_manifest(store, args.manifest)
            _print(report.to_dict())
        elif args.command == "enrich":
            counts = ingest_frames(store, args.frames)
            if args.out:
                results = []
                for row in store.iter_frames():
                    if row["result_json"] is not None:
                        results.append(json.loads(row["result_json"]))
                with open(args.out, "w", encoding="utf-8") as handle:
                    for result in results:
                        handle.write(
                            json.dumps(result, ensure_ascii=False,
                                       sort_keys=True) + "\n"
                        )
            _print({k: v for k, v in counts.items() if k != "input_errors"})
            if counts["input_errors"]:
                _print({"input_errors": counts["input_errors"]})
        elif args.command == "explain-frame":
            row = store.get_frame(args.event_id, args.frame_index)
            if row is None:
                print(
                    f"frame ({args.event_id}, {args.frame_index}) not found",
                    file=sys.stderr,
                )
                return 2
            _print(json.loads(row["result_json"]))
        elif args.command == "groups":
            payload = compute_groups(store)
            if args.out:
                with open(args.out, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, ensure_ascii=False,
                              indent=2, sort_keys=True)
            _print(payload)
        elif args.command == "metrics":
            _print(store.metrics_snapshot())
        elif args.command == "reset-metrics":
            store.reset_metrics()
            _print({"reset": True})
        else:  # pragma: no cover - argparse enforces choices
            return 2
        return 0
    except SmPipeError as exc:
        print(f"{exc.reason}: {exc}", file=sys.stderr)
        return 2
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
