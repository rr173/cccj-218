#!/usr/bin/env bash
# End-to-end, reproducible rebuild of the offline pipeline deliverables.
# Every interpreter invocation uses -I (isolated mode) so planted modules in
# the data directories cannot be imported.
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"
WORK="${ROOT}/run"
DATA="${WORK}/demo-data"
DB="${WORK}/store.db"

rm -rf "${WORK}"
mkdir -p "${WORK}"

echo "== [1/8] unit + behavior tests =="
python3 -I tests/run_tests.py

echo "== [2/8] generate hand-written walkthrough material =="
python3 -I tools/generate_fixtures.py --out "${DATA}" >"${WORK}/fixtures.json"

echo "== [3/8] ingest four releases =="
for r in r1 r2 r3 r4; do
  python3 -I smpipe.py ingest-build \
    --store "${DB}" \
    --manifest "${DATA}/${r}/manifest.json"
done | tee "${WORK}/ingest.jsonl"

echo "== [4/8] enrich frames + groups =="
python3 -I smpipe.py enrich \
  --store "${DB}" \
  --frames "${DATA}/frames/frames.jsonl" \
  --out "${WORK}/results.jsonl"
python3 -I smpipe.py groups \
  --store "${DB}" --out "${WORK}/groups.json" >"${WORK}/groups.stdout.json"
python3 -I smpipe.py metrics --store "${DB}" | tee "${WORK}/metrics.json"

echo "== [5/8] million-segment stress test under 256 MiB =="
python3 -I tools/stress_test.py --segments 1000000 \
  | tee "${WORK}/stress.json"

echo "== [6/8] exposure first-error fact table walkthrough =="
# Seal the enrichment database (checkpoint its WAL) before read-only binding.
python3 -I -c "import sqlite3;c=sqlite3.connect('${DB}');\
c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()"
python3 -I tools/facts_walkthrough.py --out "${WORK}/facts-demo" \
  | tee "${WORK}/facts-walkthrough.log"

echo "== [7/8] source-range reverse index walkthrough =="
python3 -I tools/reverse_walkthrough.py --out "${WORK}/reverse-demo" \
  | tee "${WORK}/reverse-walkthrough.log"

echo "== [8/8] trigger-operation lineage export walkthrough =="
python3 -I tools/lineage_walkthrough.py --out "${WORK}/lineage-demo" \
  | tee "${WORK}/lineage-walkthrough.log"

echo
echo "Rebuild complete. Artifacts in ${WORK}:"
ls -1 "${WORK}"
