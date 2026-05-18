from fingerprinter.queue.job_queue import JobQueue
from fingerprinter.logger.logger import configure_logging


def main():
    configure_logging()
    queue = JobQueue()
    print(f"Pending jobs: {queue.pending_count()}")
    # Placeholder for worker/consumer logic
    # Will be expanded in later phases

if __name__ == "__main__":
    main()
