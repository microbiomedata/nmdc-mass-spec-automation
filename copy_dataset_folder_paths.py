#!/usr/bin/env python3
"""Copy entries from the 'Dataset Folder Path' column into a destination folder.

This script expects a tab-delimited file (like EMSL dataset exports) with a
header row that includes a path column, defaulting to 'Dataset Folder Path'.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class PrefixMap:
    source_prefix: str
    target_prefix: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy folders/files listed in a dataset export column into a "
            "destination directory."
        )
    )
    parser.add_argument(
        "input_file",
        help="Path to the tab-delimited dataset export file.",
    )
    parser.add_argument(
        "destination_dir",
        help="Directory where matching source folders/files will be copied.",
    )
    parser.add_argument(
        "--path-column",
        default="Dataset Folder Path",
        help="Column name containing source paths (default: %(default)s).",
    )
    parser.add_argument(
        "--name-column",
        default="Dataset",
        help=(
            "Optional column to use as destination names. If empty/missing, "
            "the source basename is used (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--prefix-map",
        action="append",
        default=[],
        metavar="SRC=DST",
        help=(
            "Rewrite a source path prefix before lookup. Repeat as needed. "
            "Useful when dataset files contain UNC paths (example: "
            "\\\\proto-4\\Agilent_GC_MS_03=/Volumes/proto-4/Agilent_GC_MS_03)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite destination items when they already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without copying anything.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N rows that have a non-empty path.",
    )
    parser.add_argument(
        "--only-ext",
        default=None,
        help=(
            "Only copy files matching this extension (example: .cdf). "
            "If a source row points to a directory, matching files inside it are copied."
        ),
    )
    parser.add_argument(
        "--flatten",
        action="store_true",
        help=(
            "Write copied files directly into destination_dir instead of per-row folders. "
            "Useful with --only-ext .cdf."
        ),
    )
    return parser.parse_args()


def _normalize_separators(value: str) -> str:
    value = value.strip().strip('"').strip("'")
    if not value:
        return value
    if "\\" in value:
        value = value.replace("\\", "/")
    return value


def parse_prefix_maps(raw_maps: Iterable[str]) -> list[PrefixMap]:
    parsed: list[PrefixMap] = []
    for item in raw_maps:
        if "=" not in item:
            raise ValueError(f"Invalid --prefix-map '{item}'. Expected SRC=DST.")
        src, dst = item.split("=", 1)
        src = _normalize_separators(src)
        dst = _normalize_separators(dst)
        if not src or not dst:
            raise ValueError(
                f"Invalid --prefix-map '{item}'. Both source and target are required."
            )
        parsed.append(PrefixMap(source_prefix=src, target_prefix=dst))
    return parsed


def apply_prefix_maps(path_value: str, prefix_maps: list[PrefixMap]) -> str:
    for mapping in prefix_maps:
        if path_value.startswith(mapping.source_prefix):
            return mapping.target_prefix + path_value[len(mapping.source_prefix) :]
    return path_value


def to_path(raw_value: str, prefix_maps: list[PrefixMap]) -> Path:
    normalized = _normalize_separators(raw_value)
    mapped = apply_prefix_maps(normalized, prefix_maps)
    return Path(mapped)


def unique_destination_path(base_path: Path) -> Path:
    if not base_path.exists():
        return base_path
    suffix = 1
    while True:
        candidate = base_path.with_name(f"{base_path.name}_{suffix}")
        if not candidate.exists():
            return candidate
        suffix += 1


def copy_entry(source: Path, dest: Path, overwrite: bool, dry_run: bool) -> str:
    if dry_run:
        return "dry-run"

    if source.is_dir():
        if dest.exists():
            if not overwrite:
                return "skipped-existing"
            shutil.rmtree(dest)
        shutil.copytree(source, dest)
        return "copied"

    if source.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and not overwrite:
            return "skipped-existing"
        shutil.copy2(source, dest)
        return "copied"

    return "missing"


def find_matching_files(source: Path, only_ext: str | None) -> list[Path]:
    if not source.exists():
        return []

    if only_ext is None:
        return [source]

    normalized_ext = only_ext.lower()
    if not normalized_ext.startswith("."):
        normalized_ext = f".{normalized_ext}"

    if source.is_file():
        return [source] if source.suffix.lower() == normalized_ext else []

    if source.is_dir():
        return [path for path in source.rglob("*") if path.is_file() and path.suffix.lower() == normalized_ext]

    return []


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file)
    destination_dir = Path(args.destination_dir)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        return 2

    try:
        prefix_maps = parse_prefix_maps(args.prefix_map)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    destination_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped_existing = 0
    missing = 0
    processed = 0
    errors = 0

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")

        if reader.fieldnames is None:
            print("ERROR: Input file has no header row.", file=sys.stderr)
            return 2

        if args.path_column not in reader.fieldnames:
            print(
                (
                    f"ERROR: Column '{args.path_column}' not found. "
                    f"Available columns: {', '.join(reader.fieldnames)}"
                ),
                file=sys.stderr,
            )
            return 2

        for row_index, row in enumerate(reader, start=2):
            raw_source = (row.get(args.path_column) or "").strip()
            if not raw_source:
                continue

            if args.limit is not None and processed >= args.limit:
                break

            processed += 1
            source_path = to_path(raw_source, prefix_maps)

            row_name = (row.get(args.name_column) or "").strip() if args.name_column else ""
            base_name = row_name or source_path.name
            if not base_name:
                base_name = f"row_{row_index}"

            matching_sources = find_matching_files(source_path, args.only_ext)
            if not matching_sources:
                missing += 1
                ext_text = f" matching {args.only_ext}" if args.only_ext else ""
                print(f"MISSING{ext_text}: {source_path}")
                continue

            for match in matching_sources:
                if args.flatten:
                    destination_path = destination_dir / match.name
                elif args.only_ext:
                    destination_path = destination_dir / base_name / match.name
                else:
                    destination_path = destination_dir / base_name

                if destination_path.exists() and not args.overwrite:
                    destination_path = unique_destination_path(destination_path)

                status = copy_entry(
                    source=match,
                    dest=destination_path,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                )

                if status in {"copied", "dry-run"}:
                    copied += 1
                    action = "WOULD COPY" if args.dry_run else "COPIED"
                    print(f"{action}: {match} -> {destination_path}")
                elif status == "skipped-existing":
                    skipped_existing += 1
                    print(f"SKIPPED (exists): {destination_path}")
                elif status == "missing":
                    missing += 1
                    print(f"MISSING: {match}")
                else:
                    errors += 1
                    print(f"ERROR: Unexpected status '{status}' for {match}")

    print("\nSummary")
    print(f"- Rows processed: {processed}")
    print(f"- Copied: {copied}")
    print(f"- Skipped existing: {skipped_existing}")
    print(f"- Missing sources: {missing}")
    print(f"- Errors: {errors}")

    if missing > 0:
        print(
            "\nTip: If sources are UNC paths, use --prefix-map to map them to a local mount.",
            file=sys.stderr,
        )

    return 1 if errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())