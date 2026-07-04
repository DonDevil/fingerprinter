import argparse

from fingerprinter.config.settings import load_settings
from fingerprinter.logger.logger import configure_logging
from fingerprinter.worker.phase_a_worker import PhaseAWorker


def main():
    parser = argparse.ArgumentParser(description="Fingerprinter Phase A worker")
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
    args = parser.parse_args()

    configure_logging()
    settings = load_settings(args.config)
    worker = PhaseAWorker(settings)

    try:
        print(f"Pending jobs: {worker.queue.pending_count()}")

        if args.once:
            worker.run_once()
            return

        worker.run_loop(max_jobs=args.max_jobs)
    finally:
        worker.close()

if __name__ == "__main__":
    main()
