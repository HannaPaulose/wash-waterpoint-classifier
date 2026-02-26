"""
Waterpoint Prioritisation Script
=================================
Joins classified waterpoint flood risk results with original WPdx+ data
and applies a tier-based prioritisation framework grounded in the
OCHA Bangladesh Anticipatory Action Framework for Monsoon Floods 2025.

Tier definitions:
  Tier 1 - Pre-season action (before May, before flood season begins)
           served_population > 2500
           OR served_population > 1500 AND install_year < 2000

  Tier 2 - Anticipatory action window (between readiness and action trigger)
           served_population 1000-2500 AND install_year >= 2000
           OR served_population < 1500 AND install_year < 2000

  Tier 3 - Monitor and post-shock assistance
           served_population < 1000 AND install_year >= 2000

  Unknown - served_population or install_year missing

Usage:
  python prioritise_waterpoints.py
  python prioritise_waterpoints.py --results my_results.csv --source eqje-vguj.csv
"""

import argparse
import os
import datetime
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RESULTS_FILE = "waterpoint_flood_vulnerability_bangladesh.csv"
SOURCE_FILE = "eqje-vguj.csv"
OUTPUT_FILE = "waterpoint_prioritised_bangladesh.csv"

# Join key between results and source data
JOIN_KEY = "wpdx_id"

# Variables needed from source data for prioritisation
PRIORITY_VARS = ["served_population", "install_year"]

# Additional useful context columns to carry through
CONTEXT_VARS = [
    "water_source_clean",
    "water_tech_clean",
    "status_clean",
    "local_population",
    "distance_to_primary",
    "clean_adm1",
    "clean_adm2",
    "clean_adm3",
    "subjective_quality",
    "facility_type",
    "is_urban",
]

# Tier thresholds
POP_TIER1_HIGH = 2500       # served_population above this → always Tier 1
POP_TIER1_MED = 1500        # served_population above this + old infra → Tier 1
POP_TIER2_LOW = 1000        # served_population above this → Tier 2 (if not Tier 1)
YEAR_OLD = 2000             # install_year before this → old infrastructure


# ---------------------------------------------------------------------------
# Tier assignment logic
# ---------------------------------------------------------------------------

def assign_tier(row):
    """
    Assign a priority tier to a waterpoint based on served_population
    and install_year. Returns tier (1, 2, 3) or 'Unknown'.
    """
    pop = row.get("served_population")
    year = row.get("install_year")

    # Flag as Unknown if either variable is missing
    try:
        pop = float(pop)
        if pd.isna(pop):
            return "Unknown"
    except (TypeError, ValueError):
        return "Unknown"

    try:
        year = int(float(year))
        if pd.isna(year):
            return "Unknown"
    except (TypeError, ValueError):
        return "Unknown"

    old_infra = year < YEAR_OLD

    # Tier 1: High population OR moderate population + old infrastructure
    if pop > POP_TIER1_HIGH:
        return "Tier 1"
    if pop > POP_TIER1_MED and old_infra:
        return "Tier 1"

    # Tier 2: Moderate population + recent infra OR lower population + old infra
    if POP_TIER2_LOW <= pop <= POP_TIER1_HIGH and not old_infra:
        return "Tier 2"
    if pop < POP_TIER1_MED and old_infra:
        return "Tier 2"

    # Tier 3: Lower population + recent infrastructure
    if pop < POP_TIER2_LOW and not old_infra:
        return "Tier 3"

    # Fallback for any edge cases
    return "Tier 2"


def tier_rationale(row):
    """Generate a brief human-readable rationale for the tier assignment."""
    tier = row.get("priority_tier")
    pop = row.get("served_population")
    year = row.get("install_year")

    if tier == "Unknown":
        missing = []
        try:
            if pd.isna(float(row.get("served_population"))):
                missing.append("served_population")
        except (TypeError, ValueError):
            missing.append("served_population")
        try:
            if pd.isna(float(row.get("install_year"))):
                missing.append("install_year")
        except (TypeError, ValueError):
            missing.append("install_year")
        return f"Cannot assign tier — missing data: {', '.join(missing)}"

    try:
        pop = int(float(pop))
        year = int(float(year))
    except (TypeError, ValueError):
        return "Cannot generate rationale — data conversion error"

    old_infra = year < YEAR_OLD
    age_note = f"installed {year} ({'old infrastructure, parts may be hard to source' if old_infra else 'relatively recent'})"

    if tier == "Tier 1":
        if pop > POP_TIER1_HIGH:
            return (f"Tier 1: Serves {pop:,} people — above {POP_TIER1_HIGH:,} threshold. "
                    f"Requires pre-season rehabilitation ({age_note}).")
        else:
            return (f"Tier 1: Serves {pop:,} people with {age_note}. "
                    f"Old infrastructure and moderate-high population require pre-season action.")

    if tier == "Tier 2":
        if POP_TIER2_LOW <= pop <= POP_TIER1_HIGH and not old_infra:
            return (f"Tier 2: Serves {pop:,} people ({age_note}). "
                    f"Accessible enough for AA window intervention — pre-position supplies.")
        else:
            return (f"Tier 2: Serves {pop:,} people with {age_note}. "
                    f"Lower population but old infrastructure warrants AA window monitoring.")

    if tier == "Tier 3":
        return (f"Tier 3: Serves {pop:,} people ({age_note}). "
                f"Monitor during season and include in post-flood recovery planning.")

    return ""


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_prioritisation(results_file, source_file, output_file):

    # 1. Load classified results
    print(f"\nLoading classified results from: {results_file}")
    if not os.path.exists(results_file):
        raise FileNotFoundError(f"Results file not found: {results_file}")
    results_df = pd.read_csv(results_file, low_memory=False)
    print(f"Loaded {len(results_df)} classified waterpoints.")

    # 2. Load original source data
    print(f"Loading original WPdx+ data from: {source_file}")
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"Source file not found: {source_file}")
    source_df = pd.read_csv(source_file, low_memory=False)
    print(f"Loaded {len(source_df)} waterpoints from source.")

    # 3. Select columns to bring in from source
    cols_to_join = [JOIN_KEY] + [
        c for c in PRIORITY_VARS + CONTEXT_VARS
        if c in source_df.columns
    ]
    source_subset = source_df[cols_to_join].copy()

    # 4. Join on wpdx_id
    print(f"\nJoining on '{JOIN_KEY}'...")
    merged_df = results_df.merge(source_subset, on=JOIN_KEY, how="left")
    print(f"After join: {len(merged_df)} waterpoints.")

    # Check join quality
    matched = merged_df["served_population"].notna().sum()
    print(f"Matched to source data: {matched}/{len(merged_df)} waterpoints.")

    # 5. Apply tier logic
    print("\nApplying tier classification...")
    merged_df["priority_tier"] = merged_df.apply(assign_tier, axis=1)
    merged_df["tier_rationale"] = merged_df.apply(tier_rationale, axis=1)

    # 6. Summary
    print("\n" + "="*60)
    print("PRIORITISATION SUMMARY")
    print("="*60)
    tier_counts = merged_df["priority_tier"].value_counts()
    total = len(merged_df)
    for tier in ["Tier 1", "Tier 2", "Tier 3", "Unknown"]:
        count = tier_counts.get(tier, 0)
        pct = count / total * 100
        print(f"  {tier:10}: {count:4} ({pct:.1f}%)")

    print(f"\nBy district:")
    if "clean_adm2" in merged_df.columns:
        for district, grp in merged_df.groupby("clean_adm2"):
            counts = grp["priority_tier"].value_counts()
            t1 = counts.get("Tier 1", 0)
            t2 = counts.get("Tier 2", 0)
            t3 = counts.get("Tier 3", 0)
            unk = counts.get("Unknown", 0)
            print(f"  {district}: Tier1={t1}, Tier2={t2}, Tier3={t3}, Unknown={unk}")

    # Cross-tabulation with flood risk
    if "flood_risk" in merged_df.columns:
        print(f"\nFlood risk × Priority tier:")
        cross = pd.crosstab(merged_df["flood_risk"], merged_df["priority_tier"])
        print(cross.to_string())

    # Population stats by tier
    if "served_population" in merged_df.columns:
        print(f"\nServed population by tier:")
        for tier in ["Tier 1", "Tier 2", "Tier 3"]:
            grp = merged_df[merged_df["priority_tier"] == tier]["served_population"].dropna()
            if len(grp) > 0:
                print(f"  {tier}: min={int(grp.min())}, "
                      f"avg={int(grp.mean())}, "
                      f"max={int(grp.max())}")

    # 7. Save output
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"\n✅ Done! Results saved to: {output_file}")
    except PermissionError:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = output_file.replace(".csv", f"_{timestamp}.csv")
        merged_df.to_csv(backup, index=False)
        print(f"\nWARNING: Could not save to {output_file} (file open elsewhere)")
        print(f"✅ Saved to backup: {backup}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prioritise waterpoints by action tier")
    parser.add_argument("--results", default=RESULTS_FILE,
                        help=f"Classified results CSV (default: {RESULTS_FILE})")
    parser.add_argument("--source", default=SOURCE_FILE,
                        help=f"Original WPdx+ CSV (default: {SOURCE_FILE})")
    parser.add_argument("--output", default=OUTPUT_FILE,
                        help=f"Output CSV (default: {OUTPUT_FILE})")
    args = parser.parse_args()

    run_prioritisation(args.results, args.source, args.output)
