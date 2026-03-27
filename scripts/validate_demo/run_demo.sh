#!/usr/bin/env bash
# Demo for the trama-validate CLI tool.
#
# trama-validate has two phases:
#
#   Phase 1 — Structural validation (always runs)
#     Parses the definition and checks all node references, required fields,
#     async callback config, etc. No orchestrator needed.
#
#   Phase 2 — Dry-run simulation (runs when a scenario file is provided)
#     Walks the node graph offline using the real engine components
#     (Mustache renderer, json-logic evaluator). Renders every template with
#     the scenario payload and shows exactly which path the saga would take.
#     Catches logical bugs that structural validation cannot see.
#
# Cases in this demo:
#   1. Structural check only       (--validate-only, no scenario)
#   2. Dry-run — PIX path          (sync flow, SUCCEEDED)
#   3. Dry-run — CARD path         (async flow, SUCCEEDED)
#   4. Structural errors           (typos caught before dry-run even starts)
#   5. Dry-run catches a logic bug (wrong variable name in switch condition —
#                                   passes structural check, fails dry-run)
#   6. Dry-run — step returns 500  (runtime failure mid-simulation)
#
# Usage (from anywhere):
#   bash scripts/validate_demo/run_demo.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEF="$SCRIPT_DIR/definition.json"
DEF_BROKEN="$SCRIPT_DIR/definition_broken.json"
DEF_LOGIC_BUG="$SCRIPT_DIR/definition_logic_bug.json"

# Compile once up front so the per-case output is clean.
echo
echo "Building …"
(cd "$ROOT" && ./gradlew classes -q)
echo "Done."

# ─── Helper ───────────────────────────────────────────────────────────────────

run_case() {
    local number="$1"
    local title="$2"
    local args="$3"

    echo
    echo "══════════════════════════════════════════════════════════════════"
    printf  "  Case %s — %s\n" "$number" "$title"
    echo "══════════════════════════════════════════════════════════════════"
    echo "  $ ./gradlew trama-validate --args=\"$args\""
    echo

    # -q suppresses Gradle lifecycle noise; 2>/dev/null drops Gradle's own
    # "BUILD FAILED" error block when the CLI exits non-zero.
    # The CLI's meaningful output all goes to stdout so nothing is lost.
    (cd "$ROOT" && ./gradlew trama-validate -q --args="$args" 2>/dev/null) || true
}

# ─── Cases ────────────────────────────────────────────────────────────────────

run_case 1 "Structural check only  (--validate-only)" \
    "$DEF --validate-only"

run_case 2 "Dry-run — PIX path  (validate → switch → pix-payment → notify)" \
    "$DEF $SCRIPT_DIR/scenario_pix.json"

run_case 3 "Dry-run — CARD path  (validate → switch → card-payment [async] → notify)" \
    "$DEF $SCRIPT_DIR/scenario_card.json"

run_case 4 "Structural errors  (typo in entrypoint + typo in next reference)" \
    "$DEF_BROKEN --validate-only"

run_case 5 "Dry-run catches a logic bug  (switch condition uses wrong variable name)" \
    "$DEF_LOGIC_BUG $SCRIPT_DIR/scenario_card.json"
# The definition uses input.payment_method (snake_case) but the payload key is
# paymentMethod (camelCase).  Structural validation passes — the json-logic
# expression is syntactically valid.  The dry-run reveals the bug: no case
# matches, the switch silently falls to default (pix-payment) even though the
# payload says "card".

run_case 6 "Dry-run — step returns HTTP 500  (simulation stops and reports failure)" \
    "$DEF $SCRIPT_DIR/scenario_failure.json"

echo
echo "══════════════════════════════════════════════════════════════════"
echo "  Demo complete."
echo "══════════════════════════════════════════════════════════════════"
echo
