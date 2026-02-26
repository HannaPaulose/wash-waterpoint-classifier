# WASH Waterpoint Flood Vulnerability Classifier

An LLM-powered tool that classifies waterpoints by flood vulnerability and assigns priority action tiers, grounded in the OCHA Bangladesh Anticipatory Action Framework for Monsoon Floods 2025.

## What This Does

Humanitarian WASH teams have a 5–15 day window between a flood trigger and peak impact to pre-position water purification supplies and protect critical water sources. This tool helps prioritise which waterpoints to act on — and when — by combining:

- **WPdx+ waterpoint data** (water source type, status, infrastructure age, population served)
- **Copernicus DEM elevation** via Open-Meteo API (flood exposure context)
- **Claude AI** (Anthropic) to assess flood vulnerability grounded in the AA framework
- **Rule-based prioritisation** to assign action tiers based on population served and infrastructure age

## Output

Each waterpoint is assigned a priority tier:

| Tier | Action | Timing |
|------|--------|--------|
| Tier 1 | Pre-season rehabilitation | Before May (pre-flood season) |
| Tier 2 | Anticipatory action focus | 5–15 day AA window |
| Tier 3 | Monitor and post-shock assistance | During/after flood season |

## Results (Bangladesh — Gaibandha and Kurigram, February 2026)

- 338 waterpoints classified across 2 framework districts
- Tier 1: 15 points (4.4%) — serving 1,743–4,912 people
- Tier 2: 97 points (28.7%) — serving 303–2,418 people  
- Tier 3: 202 points (59.8%) — serving 211–994 people
- Unknown: 24 points (7.1%) — missing data

## Files

| File | Description |
|------|-------------|
| `waterpoint_vulnerability_classifier.py` | Main classifier — fetches data, gets elevations, calls Claude API |
| `prioritise_waterpoints.py` | Prioritisation script — joins results with source data, assigns tiers |
| `WASH_Classifier_Decision_Log.docx` | Full technical decision log including design rationale, known limitations, and evaluation framework |

## Requirements
```
pip install anthropic pandas requests tqdm
```

Set your Anthropic API key:
```
set ANTHROPIC_API_KEY=your-key-here
```

Download WPdx+ Bangladesh data from [data.waterpointdata.org](https://data.waterpointdata.org) and save as `eqje-vguj.csv` in the same folder.

## Usage
```
python waterpoint_vulnerability_classifier.py --limit 338
python prioritise_waterpoints.py
```

## Known Limitations

- Elevation data uses Copernicus DEM (above sea level) rather than HAND (Height Above Nearest Drainage) — less accurate in delta geographies like Bangladesh where rivers are elevated above surrounding floodplains
- WPdx+ Bangladesh data is concentrated in Cox's Bazar (UNHCR) and Kurigram/Gaibandha — 6 of 8 AA framework districts have no data
- Tier thresholds (population cutoffs, install year) are based on operational judgment and have not been validated against ground truth flood impact data

## Anticipatory Action Framework

Built around the [OCHA Bangladesh AA Framework for Monsoon Floods (April 2025)](https://www.unocha.org), which covers 8 districts across the Jamuna and Padma river basins with a $9.99M pre-arranged financing mechanism.
