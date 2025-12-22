import logging
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from analytics.complexity import ComplexityScanner
from metrics.schemas import FileComplexitySnapshot, RepoComplexityDaily
from metrics.sinks.clickhouse import ClickHouseMetricsSink

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

def run_complexity_scan_job(
    *,
    repo_path: Path,
    repo_id: uuid.UUID,
    db_url: str,
    date: date,
    ref: str = "HEAD",
) -> None:
    if not db_url:
        raise ValueError("ClickHouse DB connection string is required.")

    if not repo_path.exists():
        raise FileNotFoundError(f"Repo path {repo_path} does not exist.")

    # 1. Scan
    config_path = REPO_ROOT / "config/complexity.yaml"
    scanner = ComplexityScanner(config_path=config_path)
    logger.info(f"Scanning {repo_path} (ref={ref})...")
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
            repo_id=repo_id,
            as_of_day=date,
            ref=ref,
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
        repo_id=repo_id,
        day=date,
        loc_total=total_loc,
        cyclomatic_total=total_cc,
        cyclomatic_per_kloc=cc_per_kloc,
        high_complexity_functions=total_high,
        very_high_complexity_functions=total_very_high,
        computed_at=computed_at
    )

    # 3. Sink
    sink = ClickHouseMetricsSink(db_url)
    try:
        sink.ensure_tables()
        logger.info(f"Writing {len(snapshots)} file snapshots...")
        sink.write_file_complexity_snapshots(snapshots)
        logger.info("Writing repo daily summary...")
        sink.write_repo_complexity_daily([repo_daily])
        logger.info("Done.")
    finally:
        sink.close()
