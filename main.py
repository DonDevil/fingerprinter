import argparse

from config.settings import load_settings
from logger.logger import configure_logging
from worker.fingerprint_worker import FingerprintWorker


def _parse_techniques(value: str | None) -> set[str] | None:
    if value is None:
        return None
    raw = value.strip().lower()
    if not raw or raw == "all":
        return None
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def main():
    parser = argparse.ArgumentParser(description="Fingerprinter worker")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML settings file")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one claimed job and exit",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        help="Process up to N jobs then exit",
    )
    parser.add_argument(
        "--target",
        help="Optional target film title for quick target matching",
    )
    parser.add_argument(
        "--compare-file",
        help="Compare one file/URL against the target and exit",
    )
    parser.add_argument(
        "--compare-dir",
        nargs="?",
        const="storage/downloads",
        help="Compare files from a directory against the target (default: storage/downloads)",
    )
    parser.add_argument(
        "--techniques",
        default="all",
        help="Comma-separated techniques: metadata,visual,audio,temporal (default: all)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print completed/unfinished compare-task counters and exit",
    )
    parser.add_argument(
        "--no-resume-unfinished",
        action="store_true",
        help="Do not reclaim stale claimed queue jobs on startup",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset all local processing metadata tables in storage/processing.db",
    )
    parser.add_argument(
        "--clear-assets",
        action="store_true",
        help="Delete all files from downloader directory (default: storage/downloads)",
    )
    parser.add_argument(
        "--keep-non-matches",
        action="store_true",
        help="Retain non-matching local files instead of deleting them",
    )
    args = parser.parse_args()

    configure_logging()
    settings = load_settings(args.config)
    if args.target is not None:
        settings.pipeline.target_title = args.target.strip()
    selected_techniques = _parse_techniques(args.techniques)
    worker = FingerprintWorker(
        settings,
        enabled_techniques=selected_techniques,
        keep_non_matches=args.keep_non_matches,
    )

    try:
        maintenance_ran = False
        if args.reset:
            worker.reset_processing_state()
            print("Reset complete: local processing metadata tables were cleared")
            maintenance_ran = True

        if args.clear_assets:
            files_deleted, bytes_deleted, rows_marked = worker.clear_downloaded_assets()
            print(
                "Clear assets complete: "
                f"files_deleted={files_deleted}, bytes_deleted={bytes_deleted}, metadata_rows_marked_deleted={rows_marked}"
            )
            maintenance_ran = True

        # If maintenance is requested without an execution mode, exit after maintenance.
        if maintenance_ran and not (
            args.status
            or args.compare_file
            or args.compare_dir is not None
            or args.once
            or args.max_jobs is not None
        ):
            return

        if args.status:
            counts = worker.compare_task_status_counts()
            print("Compare task status counts:")
            print(f"  completed:   {counts.get('completed', 0)}")
            print(f"  in_progress: {counts.get('in_progress', 0)}")
            print(f"  failed:      {counts.get('failed', 0)}")
            print(f"Queue pending jobs: {worker.queue.pending_count()}")
            return

        if not args.no_resume_unfinished:
            reclaimed = worker.requeue_unfinished_claims()
            if reclaimed:
                print(f"Reclaimed stale claimed jobs: {reclaimed}")

        if args.compare_file:
            status = worker.compare_single_video(args.compare_file)
            print(f"Single compare result: {status}")
            return

        if args.compare_dir is not None:
            summary = worker.compare_directory(args.compare_dir, max_files=args.max_jobs)
            print("Directory compare summary:")
            for key in sorted(summary.keys()):
                print(f"  {key}: {summary[key]}")
            return

        print(f"Pending jobs: {worker.queue.pending_count()}")

        if args.once:
            worker.run_once()
            return

        worker.run_loop(max_jobs=args.max_jobs)
    finally:
        worker.close()

if __name__ == "__main__":
    main()
