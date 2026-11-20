
import argparse
import os

from src.extract import run_extract
from src.transform import run_transform
from src.curate import run_curate
from src.checks import run_checks


def run_init():
    """Check basic project configuration."""
    if not os.path.exists(".env"):
        raise FileNotFoundError(
            ".env tidak ditemukan. Buat file .env terlebih dahulu."
        )

    print("Project configuration found.")
    print("Pastikan Docker Postgres dan MinIO sudah berjalan.")
    print("Initialization check completed.")


def run_ingest():
    print("\n========== INGEST ==========")
    run_extract("incremental")


def run_process():
    print("\n========== PROCESS ==========")
    run_transform()


def run_curation():
    print("\n========== CURATE ==========")
    run_curate()


def run_check():
    print("\n========== RECONCILIATION ==========")
    run_checks()


def main():
    parser = argparse.ArgumentParser(
        description="Toko Bandung Source-to-Lake Pipeline"
    )

    parser.add_argument(
        "command",
        choices=["init", "ingest", "process", "curate", "check"],
        help="Pipeline command to execute"
    )

    args = parser.parse_args()

    commands = {
        "init": run_init,
        "ingest": run_ingest,
        "process": run_process,
        "curate": run_curation,
        "check": run_check,
    }

    commands[args.command]()


if __name__ == "__main__":
    main()
