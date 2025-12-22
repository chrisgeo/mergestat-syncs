#!/usr/bin/env python3
import argparse
import asyncio
import logging
import os
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from analytics.complexity import ComplexityScanner, FileComplexity
from metrics.schemas import FileComplexitySnapshot, RepoComplexityDaily
from metrics.sinks.clickhouse import ClickHouseMetricsSink
from storage import create_store, detect_db_type

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("scan_complexity")

REPO_ROOT = Path(__file__).resolve().parent.parent

def parse_args():
    parser = argparse.ArgumentParser(description="Scan repo complexity.")
    parser.add_argument("--repo-path", required=True, help="Path to local git repo.")
    parser.add_argument("--repo-id", required=True, type=uuid.UUID, help="Repo UUID.")
    parser.add_argument("--ref", default="HEAD", help="Git ref/branch analyzed.")
    parser.add_argument(
        "--date", 
        type=date.fromisoformat, 
        default=date.today().isoformat(),
        help="Date of snapshot (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--db", 
        default=os.getenv("CLICKHOUSE_DSN"),
        help="ClickHouse DSN for sinking metrics."
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    if not args.db:
        logger.error("ClickHouse DB connection string is required (--db or CLICKHOUSE_DSN).")
        sys.exit(1)

    repo_path = Path(args.repo_path)
    if not repo_path.exists():
        logger.error(f"Repo path {repo_path} does not exist.")
        sys.exit(1)

    # 1. Scan
    scanner = ComplexityScanner(config_path=REPO_ROOT / "config/complexity.yaml")
    logger.info(f"Scanning {repo_path} (ref={args.ref})...")
    file_results = scanner.scan_repo(repo_path)
    logger.info(f"Scanned {len(file_results)} files.")

    if not file_results:
        logger.warning("No complexity data found.")
        return

    # 2. Aggregations
    computed_at = datetime.now(timezone.utc)
    
    snapshots = []
    total_loc = 0
    total_cc = 0
    total_high = 0
    total_very_high = 0
    
    for f in file_results:
        snapshots.append(FileComplexitySnapshot(
            repo_id=args.repo_id,
            as_of_day=args.date,
            ref=args.ref,
            file_path=f.file_path,
            language=f.language,
            loc=f.loc,
            functions_count=f.functions_count,
            cyclomatic_total=f.cyclomatic_total,
            cyclomatic_avg=f.cyclomatic_avg,
            high_complexity_functions=f.high_complexity_functions,
            very_high_complexity_functions=f.very_high_complexity_functions,
            computed_at=computed_at
        ))
        
        total_loc += f.loc
        total_cc += f.cyclomatic_total
        total_high += f.high_complexity_functions
        total_very_high += f.very_high_complexity_functions

    cc_per_kloc = (total_cc / (total_loc / 1000.0)) if total_loc > 0 else 0.0
    
    repo_daily = RepoComplexityDaily(
        repo_id=args.repo_id,
        day=args.date,
        loc_total=total_loc,
        cyclomatic_total=total_cc,
        cyclomatic_per_kloc=cc_per_kloc,
        high_complexity_functions=total_high,
        very_high_complexity_functions=total_very_high,
        computed_at=computed_at
    )

    # 3. Sink
    sink = ClickHouseMetricsSink(args.db)
    try:
        sink.ensure_tables()
        logger.info(f"Writing {len(snapshots)} file snapshots...")
        sink.write_file_complexity_snapshots(snapshots)
        logger.info("Writing repo daily summary...")
        sink.write_repo_complexity_daily([repo_daily])
        logger.info("Done.")
    finally:
        sink.close()

if __name__ == "__main__":
    main()
