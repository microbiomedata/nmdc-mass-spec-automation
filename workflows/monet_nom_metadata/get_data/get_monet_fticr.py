#!/usr/bin/env python3
"""
Fetch all MONet project FT-ICR datasets from EMSL APIs.

Sources:
  - https://api.emsl.pnl.gov/nexus/  (project metadata)
  - https://api.emsl.pnl.gov/pacifica/ (upload/file metadata)
"""

import json
import csv
import sys
import requests
from collections import defaultdict


NEXUS_BASE = "https://api.emsl.pnl.gov/nexus"
PACIFICA_BASE = "https://api.emsl.pnl.gov/pacifica"

FTICR_KEYWORDS = ["fticr", "ft-icr", "fourier", "15t", "21t", "12t", "7t fticr"]

MONET_TYPE_KEYWORDS = ["monet"]

working_dir = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/get_data/"

def is_fticr(resource_name: str) -> bool:
    name = (resource_name or "").lower()
    return (any(k in name for k in FTICR_KEYWORDS) and "maldi" not in name)


def get_all_projects() -> list[dict]:
    """Fetch all projects from Nexus."""
    print("Fetching all projects from Nexus...", file=sys.stderr)
    resp = requests.get(f"{NEXUS_BASE}/projects/", timeout=60)
    resp.raise_for_status()
    projects = resp.json()
    print(f"  Total projects: {len(projects)}", file=sys.stderr)
    return projects


def filter_monet_projects(projects: list[dict]) -> list[dict]:
    """Return projects whose type or title contains 'monet'."""
    monet = []
    for p in projects:
        proj_type = (p.get("project_type") or "").lower()
        title = (p.get("title") or "").lower()
        if any(k in proj_type or k in title for k in MONET_TYPE_KEYWORDS):
            monet.append(p)
    print(f"  MONet projects found: {len(monet)}", file=sys.stderr)
    return monet


def get_uploads_for_project(project_id: str) -> list[dict]:
    """Fetch upload list for a project from Pacifica."""
    url = f"{PACIFICA_BASE}/data/uploads/project/{project_id}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json()


def parse_upload(pid: str, title: str, proj_type: str, upload: dict):
    """
    Parse a Pacifica upload record into a flat dict.
    Pacifica returns nested basic_metadata; resource_name is inside it.
    Also returns "user specified metadata" which includes DMS info.
    Returns None if this upload is not FT-ICR.
    """
    bm = upload.get("basic_metadata", {})
    usm = upload.get("user_specified_metadata", {})

    # resource_name may be at top level or nested
    resource_name = (
        upload.get("resource_name")
        or (bm.get("instrument_name", {}) or {}).get("value")
        or ""
    )
    if not is_fticr(resource_name):
        return None

    resource_id = upload.get("resource_id") or (bm.get("instrument_id", {}) or {}).get("value")

    # DOI may be an HTML anchor tag — extract plain DOI
    doi_raw = upload.get("doi") or (bm.get("doi", {}) or {}).get("value") or ""
    doi = doi_raw
    if "href=" in doi_raw:
        import re
        m = re.search(r'href="[^"]*">([^<]+)<', doi_raw)
        doi = m.group(1) if m else doi_raw

    is_released = (bm.get("release_state", {}) or {}).get("value")

    # Check for DMS info. if it's not there then error and stop
    if is_released and (not usm.get("omics.dms.dataset_id") or not usm.get("omics.dms.dataset")):
        raise ValueError(f"Upload {upload.get('upload_id')} is missing DMS info")

    return {
        "project_id": pid,
        "dms_dataset_id": usm.get("omics.dms.dataset_id", {}).get("value"),
        "dms_dataset_name": usm.get("omics.dms.dataset", {}).get("value"),
        "project_title": title,
        "project_type": proj_type,
        "upload_id": upload.get("upload_id"),
        "resource_id": resource_id,
        "resource_name": resource_name,
        "date_uploaded": upload.get("data_uploaded") or upload.get("data_created"),
        "is_released": is_released,
        "doi": doi,
        "total_file_size": upload.get("total_file_size"),
        "total_file_count": upload.get("file_count"),
        "retrieval_link": upload.get("uri"),
    }


def collect_fticr_uploads(monet_projects: list[dict]) -> list[dict]:
    """
    For each MONet project, fetch uploads from Pacifica and filter to datasets that are:
        - FT-ICR (based on resource name), but not MALDI
        - Released
    """
    results = []
    total = len(monet_projects)

    for i, proj in enumerate(monet_projects, 1):
        pid = proj["id"]
        title = proj.get("title", "")
        proj_type = proj.get("project_type", "")
        print(f"  [{i}/{total}] Project {pid}: {title[:50]}", file=sys.stderr)

        uploads = get_uploads_for_project(pid)
        fticr_count = 0
        for upload in uploads:
            record = parse_upload(pid, title, proj_type, upload)
            if record:
                if record["is_released"] is not True:
                    print(f"    - Skipping unreleased upload {record['upload_id']} ({record['resource_name']})", file=sys.stderr)
                    continue
                results.append(record)
                fticr_count += 1

        if fticr_count:
            print(f"    -> {fticr_count} non-MALDI FT-ICR uploads (of {len(uploads)} total)", file=sys.stderr)

    return results


def summarize(records: list[dict]) -> None:
    print("\n=== Summary ===", file=sys.stderr)
    print(f"Total non-MALDI FT-ICR uploads: {len(records)}", file=sys.stderr)

    by_project = defaultdict(int)
    by_resource = defaultdict(int)
    released = sum(1 for r in records if r.get("is_released"))

    for r in records:
        by_project[r["project_id"]] += 1
        by_resource[r["resource_name"]] += 1

    print(f"Released: {released} / {len(records)}", file=sys.stderr)
    print(f"Projects with non-MALDI FT-ICR data: {len(by_project)}", file=sys.stderr)

    print("\nUploads per project:", file=sys.stderr)
    for pid, count in sorted(by_project.items(), key=lambda x: -x[1]):
        print(f"  {pid}: {count}", file=sys.stderr)

    print("\nUploads per instrument:", file=sys.stderr)
    for rname, count in sorted(by_resource.items(), key=lambda x: -x[1]):
        print(f"  {rname}: {count}", file=sys.stderr)


def write_dms_sql_query(records: list[dict]) -> None:
    with open(working_dir + "monet_fticr_dms_query.sql", "w") as f:
        f.write(
"""
SELECT DISTINCT\n\
\tpublic.t_dataset.dataset_id,\n\
\tpublic.t_dataset.dataset,\n\
\tpublic.t_dataset.\"comment\",\n\
\tpublic.t_dataset.acq_time_start,\n\
\tpublic.t_instrument_name.instrument,\n\
\tpublic.t_requested_run.eus_proposal_id,\n\
\tconcat(public.t_storage_path.url_https,\n\
\t\tpublic.t_dataset.folder_name) AS "data_folder_path",\n\
\tpublic.t_dataset_rating_name.dataset_rating\n\
FROM\n\
\tpublic.t_dataset INNER JOIN\n\
\tpublic.t_instrument_name  ON public.t_dataset.instrument_id = public.t_instrument_name.instrument_id INNER JOIN\n\
\tpublic.t_requested_run ON public.t_dataset.dataset_id = public.t_requested_run.dataset_id INNER JOIN\n\
\tpublic.t_storage_path ON public.t_dataset.storage_path_id = public.t_storage_path.storage_path_id INNER JOIN\n\
\tpublic.t_dataset_rating_name ON public.t_dataset_rating_name.dataset_rating_id = public.t_dataset.dataset_rating_id
WHERE public.t_dataset.dataset_rating_id = 5 AND\n
\tpublic.t_dataset.dataset_id IN(\n
"""
)
        conditions = []
        for r in records:
            conditions.append(f"  {r['dms_dataset_id']}")
        f.write(",\n".join(conditions))
        f.write("\n);\n")


def filter_to_processed_only(records: list[dict]) -> list[dict]:
    """
    Filter the records to only include those that have processed data available
    on the MONet tab.
    """
    holder = []
    for r in records:
        # pull out project number and sample set number
        dataset_name = r.get("dms_dataset_name", "")
        # project number is five digits followed by an underscore followed by a sample set number
        import re
        m = re.search(r"^(\d{5})_(\d{1,2})", dataset_name)
        s = re.search(r"SRFA", dataset_name)
        if m and not s:
            project_number = m.group(1)
            sample_set_number = m.group(2)
            # check if the processed data is available on the MONet tab
            if check_processed_data_available(project_number, sample_set_number):
                holder.append(r)
        elif s and not m:
            # if the dataset name contains SRFA, we do want it
            holder.append(r)
        elif m and s:
            raise ValueError(f"why does this have both sample set number and srfa: {dataset_name}")
        elif not m and not s:
            continue
    return holder


def check_processed_data_available(project_number: str, sample_set_number: str) -> bool:
    """
    Check if the processed data is available using analysis api
    """
    # example curl construction
#     curl -X 'GET' \
#   'https://sc-data.emsl.pnnl.gov/processed_data/60881/7/FTICR' \
#   -H 'accept: application/json'
    
    # Construct curl URL using the project number and sample set number
    url = f"https://sc-data.emsl.pnnl.gov/processed_data/{project_number}/{sample_set_number}/FTICR"
    response = requests.get(url)
    if response.status_code != 200:
        if response.status_code == 404:
            print(f"Processed data not found for project {project_number} sample set {sample_set_number}",
                    file=sys.stderr)
        return False
    else:
        return True


def filter_to_not_in_nmdc(records: list[dict]) -> list[dict]:
    """
    Filter the records to only include those that are not already in NMDC.
    """
    holder = []
    from nmdc_client import DataObjectSearch
    dos_client = DataObjectSearch()

    # get all raw fticr data from nmdc
    raw_nom_in_nmdc = dos_client.get_record_by_attribute(
        attribute_name="data_object_type",
        attribute_value="Direct Infusion FT ICR-MS Raw Data",
        max_page_size=100,
        fields="id,name,md5_checksum,url",
        all_pages=True,
    )

    for r in records:
        dms_dataset_name = r["dms_dataset_name"]
        if not any(dms_dataset_name in raw["name"] for raw in raw_nom_in_nmdc):
            holder.append(r)
    return holder




def main():
    projects = get_all_projects()
    monet_projects = filter_monet_projects(projects)

    print(f"\nFetching FT-ICR uploads for {len(monet_projects)} MONet projects...", file=sys.stderr)
    records = collect_fticr_uploads(monet_projects)
    print(f"\nTotal non-MALDI FT-ICR uploads found: {len(records)}", file=sys.stderr)

    print(f"\nFiltering to only include records with processed data available on the MONet tab...", file=sys.stderr)
    records = filter_to_processed_only(records)
    print(f"Total non-MALDI FT-ICR uploads with processed data available: {len(records)}", file=sys.stderr)

    print(f"\nFiltering to only include records that are not already in NMDC...", file=sys.stderr)
    records = filter_to_not_in_nmdc(records)
    print(f"Total non-MALDI FT-ICR uploads not already in NMDC: {len(records)}", file=sys.stderr)

    summarize(records)

    print("\nWriting records to JSON", file=sys.stderr)
    with open(working_dir + "monet_fticr_sc_dump.json", "w") as f:
        json.dump(records, f, indent=2)
    print()

    print("Writing records to CSV", file=sys.stderr)
    with open(working_dir + "monet_fticr_sc_dump.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print()

    print("Writing DMS SQL query to file", file=sys.stderr)
    write_dms_sql_query(records)
    print()



if __name__ == "__main__":
    main()
