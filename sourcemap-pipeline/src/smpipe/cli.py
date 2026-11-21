"""Command line interface: ingest-build / enrich / explain-frame / groups.

All commands work against a local on-disk store and never touch the network.
"""

from __future__ import annotations

import argparse
import json
import sys

from .errors import SmPipeError
from .pipeline import (
    compute_groups,
    enrich_frame,
    ingest_frames,
    ingest_manifest,
)
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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
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
        try:
            store.close()
        except Exception:  # pragma: no cover
            pass


if __name__ == "__main__":
    raise SystemExit(main())
