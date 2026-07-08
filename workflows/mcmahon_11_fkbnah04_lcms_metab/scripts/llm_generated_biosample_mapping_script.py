import pandas as pd
import re
import os

# File paths
BIOSAMPLE_CSV = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/mcmahon_11_fkbnah04_lcms_metab/metadata/biosample_attributes.csv"
RAW_FILES_CSV = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/mcmahon_11_fkbnah04_lcms_metab/metadata/downloaded_files.csv"
OUTPUT_CSV = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/mcmahon_11_fkbnah04_lcms_metab/metadata/llm_biosample_raw_file_mapper.csv"
GEODES_BOOK_CSV = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/mcmahon_11_fkbnah04_lcms_metab/metadata/GEODES_NA_processing_book.csv"

# ── Load inputs ──────────────────────────────────────────────────────────────
biosamples_df = pd.read_csv(BIOSAMPLE_CSV)
raw_files_df = pd.read_csv(RAW_FILES_CSV)
geodes_book_df = pd.read_csv(GEODES_BOOK_CSV)

# Build biosample lookup: samp_name (Filter_ID) → (biosample_id, biosample_name)
# The biosample samp_name field corresponds to Filter_ID in the GEODES book.
# Biosample names in biosample_attributes.csv contain the Filter_ID (e.g. "GEODES005").
biosample_lookup_by_id = {}
for _, row in biosamples_df.iterrows():
    bsm_id = str(row["id"]).strip()
    bsm_name = str(row["name"]).strip()
    biosample_lookup_by_id[bsm_id] = bsm_name

# Build a lookup: Filter_ID (e.g. "GEODES005") → (biosample_id, biosample_name)
# The biosample name contains the Filter_ID token.
filter_id_to_biosample = {}
for bsm_id, bsm_name in biosample_lookup_by_id.items():
    # Extract Filter_ID-style token from biosample name, e.g. "GEODES005"
    match = re.search(r'GEODES\d+', bsm_name)
    if match:
        filter_id = match.group(0)
        filter_id_to_biosample[filter_id] = (bsm_id, bsm_name)
    # Also handle SP09_SKY style names
    match_sky = re.search(r'SP09_SKY_\d+', bsm_name)
    if match_sky:
        sky_token = match_sky.group(0)
        filter_id_to_biosample[sky_token] = (bsm_id, bsm_name)

# Build a mapping from Sample_Name (e.g. "GEODES_SP_0hr") → set of Filter_IDs
# from the GEODES book, then Filter_ID → biosample
sample_name_to_filter_ids = {}
for _, row in geodes_book_df.iterrows():
    sample_name = str(row["Sample_Name"]).strip()
    filter_id = str(row["Filter_ID"]).strip()
    if sample_name not in sample_name_to_filter_ids:
        sample_name_to_filter_ids[sample_name] = []
    sample_name_to_filter_ids[sample_name].append(filter_id)

# Build mapping from Sample_Name → biosample (prefer first Filter_ID that maps)
sample_name_to_biosample = {}
for sample_name, filter_ids in sample_name_to_filter_ids.items():
    for fid in filter_ids:
        if fid in filter_id_to_biosample:
            sample_name_to_biosample[sample_name] = filter_id_to_biosample[fid]
            break

# ── Filename → sample token decoder ─────────────────────────────────────────
# Filename pattern examples:
#   20180918_KBL_TM_Lakes_GEODES_All3_QE-HF_HILICZ-VF1_..._NEG_MSMS_13_GEO-SP-0-UF_1_...mzML
#   20181217_KBL_TM_Lakes_GEODES_All3_QE-HF_C18_..._POS_MSMS_47_GEO-ME-4-F_1_...mzML
#   20160526_HILIC___NEG_MSMS_KBL_Qex_UV_Lake_Mendota______Run9.mzML
#   20180918_KBL_TM_Lakes_Mendota_DepthDate_QE-139_..._POS_MSMS_13_ME-20170908-metabo-13_1_...mzML
#
# The GEO-XX-N pattern maps as:
#   GEO-SP  → Sparkling Lake  → GEODES_SP_<N>hr
#   GEO-ME  → Lake Mendota    → GEODES_ME_<N>hr
#   GEO-TB  → Trout Bog Lake  → GEODES_TB_<N>hr
#
# The "UF" vs "F" suffix in the sample token appears to reflect replicate
# injection differences (unfiltered vs filtered fraction), but both map to
# the same biosample/sample_name timepoint.
#
# Mendota DepthDate files encode sample name as ME-YYYYMMDD-metabo-N
# → GEODES_ME_<N>hr

LAKE_CODE = {
    "SP": "SP",
    "ME": "ME",
    "TB": "TB",
}

def decode_geodes_token(token):
    """
    Decode a GEO-XX-N[-UF/-F] token to a GEODES_XX_Nhr sample name.
    Returns sample_name or None.
    """
    # GEO-SP-0-UF, GEO-ME-4-F, GEO-TB-44, etc.
    m = re.match(r'^GEO-(SP|ME|TB)-(\d+)(?:-(UF|F))?$', token, re.IGNORECASE)
    if m:
        lake = m.group(1).upper()
        hours = int(m.group(2))
        return f"GEODES_{lake}_{hours}hr"
    return None

def decode_mendota_depthdate_token(token):
    """
    Decode ME-YYYYMMDD-metabo-N token from Mendota DepthDate files.
    Returns sample_name GEODES_ME_<N>hr or None.
    """
    m = re.match(r'^ME-\d{8}-metabo-(\d+)$', token, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return f"GEODES_ME_{n}hr"
    return None

def decode_sp09_sky_token(token):
    """
    Decode SP09_SKY_N tokens.
    Returns sample_name SP09_SKY_N or None.
    """
    m = re.match(r'^SP09_SKY_(\d+)$', token, re.IGNORECASE)
    if m:
        n = m.group(1)
        return f"SP09_SKY_{n}"
    return None

def is_control_or_blank(filename):
    """
    Return True if the file is a control, blank, or calibration standard
    that should not be matched to a biosample.
    ExCtrl-JGI, ExCtrl-User, ExtractionBlank are QC/control injections.
    """
    ctrl_patterns = [
        r'ExCtrl',
        r'ExtractionBlank',
        r'UV_Lake_Mendota',
        r'UV_Lake_Sparkling',
        r'UV_Lake_TroutBob',
        r'UV_Lake_ExtractionBlank',
    ]
    for pat in ctrl_patterns:
        if re.search(pat, filename, re.IGNORECASE):
            return True
    return False

def extract_sample_token(filename):
    """
    Extract the sample identifier token from an mzML filename.
    Returns (token_type, token_value) or (None, None).
    """
    # Remove .mzML extension
    base = filename.replace('.mzML', '').replace('.mzml', '')

    # Pattern 1: GEODES_All3 files with GEO-XX-N[-UF/-F] token
    # e.g. ..._MSMS_13_GEO-SP-0-UF_1_...
    m = re.search(r'_MSMS_\d+_(GEO-(?:SP|ME|TB)-\d+(?:-(?:UF|F))?)_', base, re.IGNORECASE)
    if m:
        return ('geo_token', m.group(1))

    # Pattern 2: Mendota_DepthDate files with ME-YYYYMMDD-metabo-N
    # e.g. ..._MSMS_13_ME-20170908-metabo-13_1_...
    m = re.search(r'_MSMS_\d+_(ME-\d{8}-metabo-\d+)_', base, re.IGNORECASE)
    if m:
        return ('mendota_depth', m.group(1))

    # Pattern 3: SP09_SKY files (from 2016 HILIC runs)
    # e.g. 20160526_HILIC___NEG_MSMS_KBL_Qex_UV_Lake_Sparkling______Run11.mzML
    # These map to SP09_SKY biosamples — matched by lake name in filename
    # (handled separately by is_control_or_blank returning False for Sparkling/TroutBob
    # 2016 files, but we need special handling)
    m = re.search(r'UV_Lake_(Sparkling|TroutBob|Mendota)_', base, re.IGNORECASE)
    if m:
        lake = m.group(1)
        return ('uv_lake', lake)

    return (None, None)

# ── SP09_SKY biosample lookup ─────────────────────────────────────────────────
# From biosample_attributes.csv, find SP09_SKY biosamples
sp09_sky_biosamples = {}
for bsm_id, bsm_name in biosample_lookup_by_id.items():
    if 'SP09_SKY' in bsm_name:
        m = re.search(r'SP09_SKY_(\d+)', bsm_name)
        if m:
            key = f"SP09_SKY_{m.group(1)}"
            sp09_sky_biosamples[key] = (bsm_id, bsm_name)

# ── 2016 HILIC UV Lake files ──────────────────────────────────────────────────
# The 2016 HILIC files (20160526_HILIC___*) appear to be:
#   UV_Lake_Mendota → Lake Mendota biosample(s)
#   UV_Lake_Sparkling → Sparkling Lake biosample(s)
#   UV_Lake_TroutBob → Trout Bog Lake biosample(s)
#   UV_Lake_ExtractionBlank → control, no biosample
#
# These early files don't have a clear time-point or replicate identifier,
# so we cannot determine a specific biosample. Leave empty (low confidence
# would require a guess; omit biosample entirely is more accurate).
# We will leave these as unmatched (empty biosample fields) since
# there is insufficient information to assign a specific biosample.

# ── Build output rows ─────────────────────────────────────────────────────────
output_rows = []

for _, row in raw_files_df.iterrows():
    filename = str(row["file_name"]).strip()

    biosample_id = ""
    biosample_name = ""
    match_confidence = ""

    if is_control_or_blank(filename):
        # Control/blank files: no biosample match, leave everything empty
        biosample_id = ""
        biosample_name = ""
        match_confidence = ""
    else:
        token_type, token_value = extract_sample_token(filename)

        if token_type == 'geo_token':
            sample_name = decode_geodes_token(token_value)
            if sample_name and sample_name in sample_name_to_biosample:
                bsm_id, bsm_name = sample_name_to_biosample[sample_name]
                biosample_id = bsm_id
                biosample_name = bsm_name
                match_confidence = "high"
            else:
                biosample_id = ""
                biosample_name = ""
                match_confidence = ""

        elif token_type == 'mendota_depth':
            sample_name = decode_mendota_depthdate_token(token_value)
            if sample_name and sample_name in sample_name_to_biosample:
                bsm_id, bsm_name = sample_name_to_biosample[sample_name]
                biosample_id = bsm_id
                biosample_name = bsm_name
                match_confidence = "high"
            else:
                biosample_id = ""
                biosample_name = ""
                match_confidence = ""

        elif token_type == 'uv_lake':
            # 2016 HILIC UV lake files — cannot determine specific biosample
            biosample_id = ""
            biosample_name = ""
            match_confidence = ""

        else:
            biosample_id = ""
            biosample_name = ""
            match_confidence = ""

    output_rows.append({
        "raw_data_identifier": filename,
        "biosample_id": biosample_id,
        "biosample_name": biosample_name,
        "match_confidence": match_confidence,
    })

# ── Write output ───────────────────────────────────────────────────────────────
output_df = pd.DataFrame(output_rows, columns=[
    "raw_data_identifier",
    "biosample_id",
    "biosample_name",
    "match_confidence",
])

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
output_df.to_csv(OUTPUT_CSV, index=False)

print(f"Output written to: {OUTPUT_CSV}")
print(f"Total rows: {len(output_df)}")
print(f"Matched to biosample: {(output_df['biosample_id'] != '').sum()}")
print(f"Unmatched/control: {(output_df['biosample_id'] == '').sum()}")
print("\nMatch confidence distribution:")
print(output_df["match_confidence"].value_counts(dropna=False))

# ── Diagnostics ───────────────────────────────────────────────────────────────
unmatched = output_df[output_df["biosample_id"] == ""]
if not unmatched.empty:
    print(f"\nUnmatched files ({len(unmatched)}):")
    for fn in unmatched["raw_data_identifier"].tolist():
        print(f"  {fn}")