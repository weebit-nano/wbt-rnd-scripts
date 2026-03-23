"""Batch-convert runnable TTK recipes to JSON using TTK2Json.exe.

This script recursively scans selected top-level directories (default:
SkyWater, DBH, Leti, Onsemi), identifies likely *master* TTK scripts, and runs
TTK2Json.exe on each one.

Heuristic summary (default mode = auto):
- Filename score:
  - +2 if filename looks like a dated run script (e.g. 2024_09_27__...)
  - -3 if filename looks like an include/template/helper file
- Content score:
  - +1 for each detected major section: Using System / Sequencer / Algorithm / Scheduler
  - +1 if it has Import statements
  - +1 if it has run metadata (Set mylot / mytest / mywafer / myscribe)
- Must include "Using Scheduler" to be considered runnable.
- Default acceptance threshold: score >= 4.

Limitations are documented in --help and in the printed summary.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import fnmatch
import os
import ntpath
from datetime import datetime
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
import tempfile

from tqdm import tqdm


DATE_FILENAME_RE = re.compile(r"^\d{4}[_-]\d{2}[_-]\d{2}__.+\.ttk$", re.IGNORECASE)
HELPER_NAME_PATTERNS = (
    "*hardwaresettings*.ttk",
    "*structure*database*.ttk",
    "*templatedatabase*.ttk",
    "*template*.ttk",
    "*die_list*.ttk",
    "*die_lists*.ttk",
    "*algos*.ttk",
    "*algo*.ttk",
)

# Flexible Import matcher for TTK lines such as:
# - Import C:/TTK_CODE/HardwareSettings.ttk
# - Import "E:/Other/Path With Spaces/File.ttk";
# - Import .\\local\\file.ttk   # comment
IMPORT_LINE_RE = re.compile(
    r"^(?P<indent>\s*)Import\s+(?P<path>(?:\"[^\"]*\"|'[^']*'|[^#;\r\n]+?))(?:\s*;)?(?P<tail>\s*(?:#.*)?)$",
    re.IGNORECASE,
)


@dataclass
class MatchResult:
    score: int
    is_runnable: bool
    reasons: list[str]


@dataclass
class ConversionResult:
    ttk_path: Path
    rel_path: Path
    score: int
    reasons: list[str]
    success: bool = False
    dry_run: bool = False
    import_rewrites: int = 0
    messages: list[str] = field(default_factory=list)
    rc: int | None = None
    stdout: str = ""
    stderr: str = ""
    log_path: Path | None = None
    log_ok: bool = False
    log_error: str | None = None
    outputs_ok: bool = False
    output_error: str | None = None
    created_json: list[Path] = field(default_factory=list)
    snapshot: str | None = None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively find runnable master .ttk files and invoke TTK2Json.exe on each. "
            "Default roots: SkyWater DBH Leti Onsemi"
        )
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.cwd(),
        help="Workspace root that contains the top-level foundry directories (default: current directory).",
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        default=["SkyWater", "DBH", "Leti", "Onsemi"],
        help="Top-level directories under --base-dir to scan.",
    )
    parser.add_argument(
        "--ttk2json-exe",
        default="TTK2Json.exe",
        help="Executable or full path for TTK2Json converter.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "content", "filename", "all"],
        default="auto",
        help=(
            "File selection mode: "
            "auto=combined score, content=content-only score, "
            "filename=filename-only score, all=every .ttk file."
        ),
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=4,
        help="Minimum heuristic score for acceptance in auto/content/filename mode (default: 4).",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=1_000_000,
        help="Maximum bytes to read per .ttk when evaluating content heuristics.",
    )
    parser.add_argument(
        "--include-glob",
        default="*.ttk",
        help="Glob pattern for candidate files (default: *.ttk).",
    )
    parser.add_argument(
        "--exclude-glob",
        action="append",
        default=[],
        help="Additional glob(s) to exclude, may be repeated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print which files would be converted; do not execute TTK2Json.exe.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately if one conversion command fails.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print selected file reasons and converter stdout/stderr.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, (os.cpu_count() or 1) + 4),
        help="Number of worker threads to use for conversions (default: min(32, cpu_count + 4)).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="Kill a TTK2Json run if it exceeds this many seconds (0 disables the timeout).",
    )
    return parser.parse_args(argv)


def is_helper_name(path: Path) -> bool:
    name = path.name.lower()
    return any(fnmatch.fnmatch(name, pattern) for pattern in HELPER_NAME_PATTERNS)


def score_by_filename(path: Path) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    if DATE_FILENAME_RE.match(path.name):
        score += 2
        reasons.append("filename matches dated-master pattern")

    if is_helper_name(path):
        score -= 3
        reasons.append("filename matches common helper/include pattern")
    return score, reasons


def score_by_content(path: Path, max_bytes: int) -> tuple[int, bool, list[str]]:
    score = 0
    reasons: list[str] = []
    is_runnable = False

    try:
        raw = path.read_bytes()
    except OSError as exc:
        return -999, False, [f"read error: {exc}"]

    if len(raw) > max_bytes:
        raw = raw[:max_bytes]
        reasons.append("content truncated to max-bytes for heuristic scan")

    # Replace undecodable bytes to remain resilient to mixed encodings.
    text = raw.decode("utf-8", errors="replace")
    lowered = text.lower()

    sections = [
        ("using system", "Using System section"),
        ("using sequencer", "Using Sequencer section"),
        ("using algorithm", "Using Algorithm section"),
        ("using scheduler", "Using Scheduler section"),
    ]

    found_scheduler = False
    for token, label in sections:
        if token in lowered:
            score += 1
            reasons.append(f"contains {label}")
            if token == "using scheduler":
                found_scheduler = True

    if re.search(r"^\s*import\s+", text, flags=re.IGNORECASE | re.MULTILINE):
        score += 1
        reasons.append("contains Import statements")

    if re.search(r"\bset\s+my(lot|test|wafer|scribe)\b", lowered):
        score += 1
        reasons.append("contains run metadata (Set my...) ")

    # Strong requirement to mimic runnable top-level scripts.
    is_runnable = found_scheduler
    if not found_scheduler:
        reasons.append("missing Using Scheduler (likely include/helper file)")

    return score, is_runnable, reasons


def evaluate_candidate(path: Path, mode: str, min_score: int, max_bytes: int) -> MatchResult:
    filename_score, filename_reasons = score_by_filename(path)
    content_score, content_runnable, content_reasons = score_by_content(path, max_bytes)

    reasons: list[str] = []
    score = 0
    is_runnable = False

    if mode == "all":
        return MatchResult(score=0, is_runnable=True, reasons=["mode=all"])

    if mode == "filename":
        score = filename_score
        reasons.extend(filename_reasons)
        # Filename-only mode cannot prove runnable; require non-helper and score threshold.
        is_runnable = not is_helper_name(path)
        if not is_runnable:
            reasons.append("filename-only mode rejected helper/include filename")

    elif mode == "content":
        score = content_score
        reasons.extend(content_reasons)
        is_runnable = content_runnable

    else:  # auto
        score = filename_score + content_score
        reasons.extend(filename_reasons)
        reasons.extend(content_reasons)
        is_runnable = content_runnable

    if score < min_score:
        reasons.append(f"score {score} < min-score {min_score}")
        return MatchResult(score=score, is_runnable=False, reasons=reasons)

    if not is_runnable:
        return MatchResult(score=score, is_runnable=False, reasons=reasons)

    return MatchResult(score=score, is_runnable=True, reasons=reasons)


def iter_ttk_files(base_dir: Path, roots: Iterable[str], include_glob: str, exclude_globs: list[str]) -> list[Path]:
    files: list[Path] = []
    for root_name in tqdm(list(roots), desc="Scanning roots", unit="root"):
        root_path = base_dir / root_name
        if not root_path.exists():
            print(f"[WARN] Root not found, skipping: {root_path}")
            continue
        for path in root_path.rglob("*.ttk"):
            if not fnmatch.fnmatch(path.name, include_glob):
                continue
            rel = path.relative_to(base_dir)
            rel_posix = rel.as_posix()
            if any(fnmatch.fnmatch(rel_posix, pat) or fnmatch.fnmatch(path.name, pat) for pat in exclude_globs):
                continue
            files.append(path)
    return files


def run_converter(exe: str, ttk_path: Path, timeout_seconds: int) -> tuple[list[str], int, str, str]:
    cmd = [exe, str(ttk_path)]
    timeout = timeout_seconds if timeout_seconds > 0 else None

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(ttk_path.parent),
            check=False,
            timeout=timeout,
        )
        return cmd, proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout.decode("utf-8", errors="replace") if exc.stdout else "")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "")
        stderr = stderr.rstrip() + ("\n" if stderr.strip() else "")
        stderr += f"[TIMEOUT] Exceeded {timeout_seconds} seconds; process was killed by Python."
        return cmd, 124, stdout, stderr


def list_json_files(directory: Path) -> set[Path]:
    return {path for path in directory.glob("*.json") if path.is_file()}


def describe_directory_state(directory: Path) -> str:
    ttk_names = sorted(path.name for path in directory.glob("*.ttk") if path.is_file())
    json_names = sorted(path.name for path in directory.glob("*.json") if path.is_file())
    log_names = sorted(path.name for path in directory.glob("*.log") if path.is_file())

    lines = [
        f"directory: {directory}",
        f"ttk files ({len(ttk_names)}): {', '.join(ttk_names) or 'none'}",
        f"json files ({len(json_names)}): {', '.join(json_names) or 'none'}",
        f"log files ({len(log_names)}): {', '.join(log_names) or 'none'}",
    ]
    return "\n".join(lines)


def build_run_log_path(ttk_path: Path) -> Path:
    return ttk_path.with_suffix(".ttk2json.log")


def write_run_log(log_path: Path, content: str) -> tuple[bool, str | None]:
    try:
        log_path.write_text(content, encoding="utf-8")
    except OSError as exc:
        return False, f"failed writing log file: {exc}"
    return True, None


def append_run_log(log_path: Path, content: str) -> tuple[bool, str | None]:
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(content)
    except OSError as exc:
        return False, f"failed appending to log file: {exc}"
    return True, None


def write_run_log_json(log_path: Path, payload: dict[str, object]) -> tuple[bool, str | None]:
    try:
        log_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as exc:
        return False, f"failed writing JSON run log: {exc}"
    return True, None


def relative_parent_dir(path: Path, base_dir: Path) -> str:
    try:
        return path.parent.relative_to(base_dir).as_posix()
    except ValueError:
        return path.parent.as_posix()


def check_directory_write_access(directory: Path) -> tuple[bool, str | None]:
    if not directory.exists():
        return False, f"working directory does not exist: {directory}"
    if not directory.is_dir():
        return False, f"working path is not a directory: {directory}"

    probe_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=directory,
            prefix=f".ttk2json_write_probe_{os.getpid()}_",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
        ) as probe_file:
            probe_path = Path(probe_file.name)
            probe_file.write("probe\n")
            probe_file.flush()
    except OSError as exc:
        return False, f"cannot create files in working directory {directory}: {exc}"

    try:
        probe_path.unlink()
    except OSError as exc:
        return False, f"wrote probe file {probe_path.name} in {directory}, but could not remove it: {exc}"

    return True, None


def verify_json_outputs(ttk_path: Path, json_before: set[Path]) -> tuple[bool, list[Path], str | None]:
    directory = ttk_path.parent
    json_after = list_json_files(directory)
    created_json = sorted(json_after - json_before)

    if len(created_json) == 2:
        return True, created_json, None

    before_names = ", ".join(sorted(path.name for path in json_before)) or "none"
    after_names = ", ".join(sorted(path.name for path in json_after)) or "none"
    created_names = ", ".join(path.name for path in created_json) or "none"
    error = (
        f"expected 2 new .json files in {directory}, but found {len(created_json)}. "
        f"Created: {created_names}. Pre-run .json files: {before_names}. Current .json files: {after_names}."
    )
    return False, created_json, error


def is_windows_exception_exit_code(return_code: int) -> bool:
    return return_code < 0 or return_code >= 0xC0000000


def localize_import_lines(ttk_path: Path, dry_run: bool) -> tuple[bool, int, str | None]:
    """Rewrite absolute Import paths to local relative form: Import ./<filename>."""
    try:
        original = ttk_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return False, 0, f"failed reading file: {exc}"

    changed = 0
    rewritten_lines: list[str] = []

    for line in original.splitlines(keepends=True):
        newline = "\n" if line.endswith("\n") else ""
        body = line[:-1] if newline else line

        m = IMPORT_LINE_RE.match(body)
        if not m:
            rewritten_lines.append(line)
            continue

        indent = m.group("indent")
        raw_path = m.group("path").strip()
        tail = m.group("tail") or ""

        # Remove optional wrapping quotes from the path token.
        if (raw_path.startswith('"') and raw_path.endswith('"')) or (
            raw_path.startswith("'") and raw_path.endswith("'")
        ):
            path_token = raw_path[1:-1].strip()
        else:
            path_token = raw_path.strip()

        # Support both Windows and POSIX separators, independent of host OS.
        normalized = path_token.replace("\\", "/")
        name = ntpath.basename(normalized)
        if not name:
            rewritten_lines.append(line)
            continue

        new_line_full = f"{indent}Import ./{name}{tail}{newline}"

        if new_line_full != line:
            changed += 1
        rewritten_lines.append(new_line_full)

    if changed == 0:
        return True, 0, None

    if dry_run:
        return True, changed, None

    try:
        ttk_path.write_text("".join(rewritten_lines), encoding="utf-8")
    except OSError as exc:
        return False, changed, f"failed writing file: {exc}"
    return True, changed, None


def process_selected_file(base_dir: Path, args: argparse.Namespace, ttk_path: Path, result: MatchResult) -> ConversionResult:
    rel = ttk_path.relative_to(base_dir)
    conversion = ConversionResult(
        ttk_path=ttk_path,
        rel_path=rel,
        score=result.score,
        reasons=result.reasons,
        dry_run=args.dry_run,
    )

    conversion.messages.append("")
    conversion.messages.append(f"[SELECTED] {rel} (score={result.score})")
    if args.verbose:
        for reason in result.reasons:
            conversion.messages.append(f"  - {reason}")

    ok, changed, rewrite_error = localize_import_lines(ttk_path, args.dry_run)
    conversion.import_rewrites = changed
    if not ok:
        conversion.messages.append(f"  [FAIL] Could not rewrite Import lines: {rewrite_error}")
        return conversion

    if args.dry_run:
        if changed:
            conversion.messages.append(f"  [DRY-RUN] Would rewrite {changed} Import line(s) in {ttk_path.name}")
        conversion.messages.append("  [DRY-RUN] Skipping TTK2Json execution")
        conversion.success = True
        return conversion

    write_ok, write_error = check_directory_write_access(ttk_path.parent)
    if not write_ok:
        conversion.messages.append(f"  [FAIL] {write_error}")
        return conversion

    json_before = list_json_files(ttk_path.parent)
    cmd, rc, out, err = run_converter(args.ttk2json_exe, ttk_path, args.timeout_seconds)
    conversion.rc = rc
    conversion.stdout = out
    conversion.stderr = err
    if args.verbose:
        conversion.messages.append(f"  [DEBUG] Command: {' '.join(cmd)}")

    log_path = build_run_log_path(ttk_path)
    conversion.log_path = log_path
    log_content = [
        f"timestamp: {datetime.now().isoformat(timespec='seconds')}",
        f"ttk_path: {ttk_path}",
        f"working_directory: {ttk_path.parent}",
        f"command: {args.ttk2json_exe} {ttk_path}",
        f"return_code: {rc}",
        "",
    ]
    log_content.append("[STDOUT]")
    log_content.append(out.rstrip() if out.strip() else "<empty>")
    log_content.append("")
    log_content.append("[STDERR]")
    log_content.append(err.rstrip() if err.strip() else "<empty>")
    log_content.append("")
    log_ok, log_error = write_run_log(log_path, "\n".join(log_content).rstrip() + "\n")
    conversion.log_ok = log_ok
    conversion.log_error = log_error

    outputs_ok, created_json, output_error = verify_json_outputs(ttk_path, json_before)
    conversion.outputs_ok = outputs_ok
    conversion.created_json = created_json
    conversion.output_error = output_error

    if outputs_ok:
        conversion.success = log_ok
        if rc == 0:
            conversion.messages.append("  [OK] TTK2Json created 2 JSON files")
        else:
            conversion.messages.append(f"  [WARN] TTK2Json exited with code {rc}, but output check passed")
    else:
        conversion.messages.append(f"  [FAIL] {output_error}")
        if rc != 0:
            conversion.messages.append(f"  [FAIL] TTK2Json also exited with code {rc}")
            if is_windows_exception_exit_code(rc):
                conversion.messages.append(
                    "  [HINT] This looks like a Windows exception/Crash code. Check the run log and directory snapshot below."
                )
        if created_json:
            conversion.messages.append(f"  [INFO] Created JSON files: {', '.join(path.name for path in created_json)}")

    if log_ok:
        conversion.messages.append(f"  [LOG] Wrote converter log to {log_path.name}")
    else:
        conversion.messages.append(f"  [FAIL] {log_error}")

    if args.verbose and not outputs_ok:
        snapshot = describe_directory_state(ttk_path.parent)
        conversion.snapshot = snapshot
        conversion.messages.append("  [DEBUG] Directory snapshot:")
        for line in snapshot.splitlines():
            conversion.messages.append(f"    {line}")
        if log_ok:
            snapshot_ok, snapshot_error = append_run_log(
                log_path,
                "\n".join(["", "[DIRECTORY SNAPSHOT]", snapshot]).rstrip() + "\n",
            )
            if not snapshot_ok:
                conversion.messages.append(f"  [FAIL] {snapshot_error}")

    if args.verbose:
        conversion.messages.append("  [STDOUT]")
        conversion.messages.append(out.rstrip() if out.strip() else "  <empty>")
        conversion.messages.append("  [STDERR]")
        conversion.messages.append(err.rstrip() if err.strip() else "  <empty>")

    return conversion


def print_conversion_result(conversion: ConversionResult) -> None:
    for line in conversion.messages:
        tqdm.write(line)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)

    base_dir = args.base_dir.resolve()
    print(f"[INFO] Base directory: {base_dir}")
    print(f"[INFO] Roots: {', '.join(args.roots)}")
    print(f"[INFO] Mode: {args.mode}, min-score: {args.min_score}")
    print(f"[INFO] Workers: {max(1, args.workers)}")
    if args.timeout_seconds > 0:
        print(f"[INFO] Timeout: {args.timeout_seconds} seconds")

    candidates = iter_ttk_files(base_dir, args.roots, args.include_glob, args.exclude_glob)
    print(f"[INFO] Found {len(candidates)} candidate .ttk files before heuristic filtering")

    selected: list[tuple[Path, MatchResult]] = []
    rejected = 0

    for path in tqdm(candidates, desc="Heuristic filtering", unit="file"):
        result = evaluate_candidate(path, args.mode, args.min_score, args.max_bytes)
        if result.is_runnable:
            selected.append((path, result))
        else:
            rejected += 1

    print(f"[INFO] Selected {len(selected)} files, rejected {rejected}")

    ok_parent_dirs: list[str] = []
    failed_parent_dirs: list[str] = []
    seen_ok_dirs: set[str] = set()
    seen_failed_dirs: set[str] = set()

    def add_unique(directory_list: list[str], seen_dirs: set[str], directory_value: str) -> None:
        if directory_value not in seen_dirs:
            seen_dirs.add(directory_value)
            directory_list.append(directory_value)

    def write_summary_log() -> tuple[bool, str | None, Path]:
        summary_path = base_dir / "run_log.json"
        payload: dict[str, object] = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "base_dir": str(base_dir),
            "selected_counts": {
                "ok_parent_dirs": len(ok_parent_dirs),
                "failed_parent_dirs": len(failed_parent_dirs),
            },
            "selected_parent_dirs": {
                "OK": ok_parent_dirs,
                "FAILED": failed_parent_dirs,
            },
        }
        ok, error = write_run_log_json(summary_path, payload)
        return ok, error, summary_path

    if not selected:
        json_ok, json_error, json_path = write_summary_log()
        if json_ok:
            print(f"[LOG] Wrote run summary to {json_path}")
        else:
            print(f"[WARN] {json_error}")
        print("[INFO] Nothing to convert.")
        return 0

    failures = 0
    converted = 0
    import_rewrites = 0

    worker_count = max(1, args.workers)
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="ttk2json") as executor:
        future_map = {
            executor.submit(process_selected_file, base_dir, args, ttk_path, result): (ttk_path, result)
            for ttk_path, result in selected
        }

        for future in tqdm(as_completed(future_map), total=len(future_map), desc="Converting", unit="file"):
            conversion = future.result()
            import_rewrites += conversion.import_rewrites
            print_conversion_result(conversion)

            parent_dir = relative_parent_dir(conversion.ttk_path, base_dir)

            if conversion.success:
                converted += 1
                add_unique(ok_parent_dirs, seen_ok_dirs, parent_dir)
                continue

            failures += 1
            add_unique(failed_parent_dirs, seen_failed_dirs, parent_dir)
            if args.stop_on_error:
                print("[INFO] Stopping due to --stop-on-error")
                for pending_future in future_map:
                    if not pending_future.done():
                        pending_future.cancel()
                break

    json_ok, json_error, json_path = write_summary_log()

    print("\n[SUMMARY]")
    print(f"  Converted successfully: {converted}")
    print(f"  Failed conversions: {failures}")
    print(f"  Import lines rewritten: {import_rewrites}")
    if args.dry_run:
        print("  Dry run: yes (no conversions executed)")
    if json_ok:
        print(f"  Run log JSON: {json_path}")
    else:
        print(f"  [WARN] {json_error}")

    if failures > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
