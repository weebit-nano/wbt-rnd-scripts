"""Compress clean TTK parent directories into separate zip files.

The script scans a source tree such as `CLEAN`, finds unique parent
directories that contain `.ttk` files, filters them by the same `--roots`
argument shape used by `ttk2json_batch.py`, and writes one zip archive per
directory.

Zip file names are derived from the folder hierarchy so each archive is unique
and easy to map back to its source directory.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from tqdm import tqdm


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Zip each clean TTK parent directory into a separate archive while "
            "preserving the folder hierarchy in the archive name."
        )
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.cwd(),
        help="Workspace root that contains the source tree (default: current directory).",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("CLEAN"),
        help="Directory containing the clean TTK parent folders to zip (default: CLEAN under --base-dir).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory for output zip files (default: a 'CLEAN_zips' folder "
            "next to the source tree)."
        ),
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        default=["SkyWater", "DBH", "Leti", "Onsemi"],
        help="Top-level directories to consider, matching ttk2json_batch.py.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be zipped without creating archives.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing zip files instead of skipping them.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, os.cpu_count() or 1),
        help="Number of worker processes to use for zipping (default: min(32, cpu_count)).",
    )
    return parser.parse_args(argv)


def normalize_path(path_text: str) -> Path:
    return Path(path_text.replace("/", "\\"))


def relative_root(rel_path: Path) -> str | None:
    parts = rel_path.parts
    if not parts:
        return None
    return parts[0]


def collect_ttk_parent_directories(source_dir: Path, roots: list[str]) -> list[Path]:
    root_filter = set(roots)
    parents: list[Path] = []
    seen: set[Path] = set()

    for ttk_path in source_dir.rglob("*.ttk"):
        if not ttk_path.is_file():
            continue

        parent_dir = ttk_path.parent
        try:
            rel_parent = parent_dir.relative_to(source_dir)
        except ValueError:
            continue

        if root_filter and relative_root(rel_parent) not in root_filter:
            continue

        if parent_dir not in seen:
            seen.add(parent_dir)
            parents.append(parent_dir)

    return sorted(parents, key=lambda path: path.as_posix())


def archive_name_for_directory(source_dir: Path, directory: Path) -> str:
    rel_dir = directory.relative_to(source_dir)
    flat_name = "__".join(rel_dir.parts)
    return f"{flat_name}.zip"


def zip_directory(source_dir: Path, directory: Path, zip_path: Path, dry_run: bool, overwrite: bool) -> tuple[bool, str | None]:
    if not directory.exists():
        return False, f"source directory not found: {directory}"
    if not directory.is_dir():
        return False, f"source path is not a directory: {directory}"

    if zip_path.exists() and not overwrite:
        return False, f"zip already exists: {zip_path}"

    if dry_run:
        return True, None

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED, compresslevel=9) as zip_file:
        for child in directory.rglob("*"):
            if not child.is_file():
                continue
            zip_file.write(child, arcname=child.relative_to(source_dir).as_posix())

    return True, None


def zip_directory_task(source_dir: Path, directory: Path, zip_path: Path, dry_run: bool, overwrite: bool) -> tuple[str, bool, str | None]:
    ok, message = zip_directory(source_dir, directory, zip_path, dry_run, overwrite)
    return directory.relative_to(source_dir).as_posix(), ok, message


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)

    base_dir = args.base_dir.resolve()
    source_dir = (base_dir / args.source_dir).resolve() if not args.source_dir.is_absolute() else args.source_dir.resolve()
    output_dir = (
        (base_dir / args.output_dir).resolve()
        if args.output_dir is not None and not args.output_dir.is_absolute()
        else (args.output_dir.resolve() if args.output_dir is not None else (source_dir.parent / f"{source_dir.name}_zips").resolve())
    )

    tqdm.write(f"[INFO] Base directory: {base_dir}")
    tqdm.write(f"[INFO] Source directory: {source_dir}")
    tqdm.write(f"[INFO] Output directory: {output_dir}")
    tqdm.write(f"[INFO] Roots: {', '.join(args.roots)}")

    if not source_dir.exists():
        tqdm.write(f"[ERROR] Source directory not found: {source_dir}")
        return 2

    parent_dirs = collect_ttk_parent_directories(source_dir, args.roots)
    if not parent_dirs:
        tqdm.write("[INFO] No TTK parent directories found.")
        return 0

    zipped = 0
    skipped = 0
    failed = 0

    worker_count = max(1, args.workers)
    tqdm.write(f"[INFO] Workers: {worker_count}")

    tasks = [
        (directory, output_dir / archive_name_for_directory(source_dir, directory))
        for directory in parent_dirs
    ]

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(zip_directory_task, source_dir, directory, zip_path, args.dry_run, args.overwrite): (directory, zip_path)
            for directory, zip_path in tasks
        }

        for future in tqdm(as_completed(future_map), total=len(future_map), desc="Zipping", unit="dir"):
            rel_dir, ok, message = future.result()
            directory, zip_path = future_map[future]

            if ok:
                zipped += 1
                tqdm.write(f"[ZIPPED] {rel_dir} -> {zip_path.name}")
            else:
                if message and message.startswith("zip already exists"):
                    skipped += 1
                    tqdm.write(f"[SKIP] {rel_dir}: {message}")
                else:
                    failed += 1
                    tqdm.write(f"[FAIL] {rel_dir}: {message}")

    summary = {
        "base_dir": str(base_dir),
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "roots": args.roots,
        "zipped": zipped,
        "skipped": skipped,
        "failed": failed,
        "dry_run": args.dry_run,
    }

    tqdm.write("\n[SUMMARY]")
    tqdm.write(json.dumps(summary, indent=2))

    return 2 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))