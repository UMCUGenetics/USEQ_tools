"""
Looks up index & index2 sequences from Top_Unknown_Barcodes.csv in reagents.json,
trying all orientations (forward/reverse complement, swapped order).
"""

import json
import re
import csv
import sys
from pathlib import Path

COMPLEMENT = str.maketrans("ACGTacgt", "TGCAtgca")

def rev_comp(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    return seq.translate(COMPLEMENT)[::-1]


def build_lookup(reagents: dict) -> dict[tuple[str, str], list[str]]:
    """
    Parse reagents.json into a dict keyed by (index1, index2) tuples.
    Handles both 'illumina' and 'ont' top-level sections.

    Entry format examples:
      "A701-A501 (ATCACGAC-AAGGTTCA)"   → two indexes
      "BC01 (CACAAAGACACCGACAACTTTCTT)"  → single index (index2 = "")
    """
    lookup: dict[tuple[str, str], list[str]] = {}

    for _platform, kits in reagents.items():
        if not isinstance(kits, dict):
            continue
        for kit_name, entries in kits.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                # Extract sequences inside parentheses
                m = re.search(r'\(([^)]+)\)', entry)
                if not m:
                    continue
                seqs = m.group(1).split("-")
                if len(seqs) == 2:
                    i1, i2 = seqs[0].strip(), seqs[1].strip()
                elif len(seqs) == 1:
                    i1, i2 = seqs[0].strip(), ""
                else:
                    # More than one dash — join all but the last as i1
                    i1 = "-".join(seqs[:-1]).strip()
                    i2 = seqs[-1].strip()

                key = (i1.upper(), i2.upper())
                lookup.setdefault(key, []).append(f"{kit_name}: {entry}")

    return lookup


def search_all_orientations(
    index: str,
    index2: str,
    lookup: dict[tuple[str, str], list[str]],
) -> list[tuple[str, list[str]]]:
    """
    Try all 8 orientation combinations and return a list of
    (orientation_label, [match_strings]) for every hit found.
    """
    i  = index.upper()
    rc_i  = rev_comp(i)
    i2 = index2.upper() if index2 else ""
    rc_i2 = rev_comp(i2) if i2 else ""

    # Build candidate list: (label, key)
    candidates: list[tuple[str, tuple[str, str]]] = [
        ("index, index2",                          (i,    i2)),
        ("RC(index), index2",                      (rc_i, i2)),
        ("index, RC(index2)",                      (i,    rc_i2)),
        ("RC(index), RC(index2)",                  (rc_i, rc_i2)),
        ("index2, index",                          (i2,   i)),
        ("RC(index2), index",                      (rc_i2, i)),
        ("index2, RC(index)",                      (i2,   rc_i)),
        ("RC(index2), RC(index)",                  (rc_i2, rc_i)),
    ]

    # When index2 is empty, skip candidates that would use a non-empty i2/rc_i2
    # value in the key — i.e. swapped-order candidates where i2 appears first.
    if not i2:
        candidates = [c for c in candidates if c[1][0] != ""]

    # Deduplicate by key, keeping the first matching label for each unique key.
    seen_keys: set[tuple[str, str]] = set()
    hits = []
    for label, key in candidates:
        if key in seen_keys:
            continue
        seen_keys.add(key)
        if key in lookup:
            hits.append((label, lookup[key]))
    return hits


def match_barcodes(csv_path: str, json_path: str) -> None:
    # Load reagents
    with open(json_path) as fh:
        reagents = json.load(fh)
    lookup = build_lookup(reagents)
    print(f"Loaded {len(lookup):,} reagent combinations from {json_path}\n")

    # Process CSV
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    print(f"Processing {len(rows)} rows from {csv_path}\n")
    print("=" * 80)

    total_matched = 0

    for row in rows:
        index  = (row.get("index")  or "").strip()
        index2 = (row.get("index2") or "").strip()
        lane   = row.get("Lane", "?")
        reads  = row.get("# Reads", "?")

        if not index:
            continue  # nothing to look up

        hits = search_all_orientations(index, index2, lookup)

        if hits:
            total_matched += 1
            idx_display = index if not index2 else f"{index} / {index2}"
            print(f"Lane {lane} | {idx_display} | Reads: {reads}")
            for orientation, matches in hits:
                for match in matches:
                    print(f"  ✓ [{orientation}]  {match}")
            print()

    print("=" * 80)
    print(f"Summary: {total_matched}/{len(rows)} rows had at least one match.")

def run(csv_path: str, json_path: str) -> None:
    """Execute the match_barcodes process.

    Args:
        csv_path (str): Full path to the Top_Unknown_Barcodes.csv file
        json_path (str): Full path to the reagents.json file. Defaults to resources/reagents.json.

    Returns:
        None
    """
    match_barcodes(csv_path, json_path)
