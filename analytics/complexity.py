import fnmatch
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from radon.complexity import cc_visit
from radon.visitors import ComplexityVisitor

logger = logging.getLogger(__name__)

@dataclass
class FileComplexity:
    file_path: str
    language: str
    loc: int
    functions_count: int
    cyclomatic_total: int
    cyclomatic_avg: float
    high_complexity_functions: int
    very_high_complexity_functions: int

class ComplexityScanner:
    def __init__(self, config_path: Path):
        self.config = self._load_config(config_path)
        self.high_threshold = self.config.get("high_complexity_threshold", 15)
        self.very_high_threshold = self.config.get("very_high_threshold", 25)
        self.include_globs = self.config.get("include_globs", ["**/*.py"])
        self.exclude_globs = self.config.get("exclude_globs", [])

    def _load_config(self, path: Path) -> Dict:
        if not path.exists():
            logger.warning(f"Complexity config not found at {path}, using defaults")
            return {}
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def should_process(self, file_path: str) -> bool:
        # Check excludes first
        for pat in self.exclude_globs:
            if fnmatch.fnmatch(file_path, pat):
                return False
        # Check includes
        for pat in self.include_globs:
            if fnmatch.fnmatch(file_path, pat):
                return True
        return False

    def scan_repo(self, repo_root: Path) -> List[FileComplexity]:
        results = []
        repo_root = repo_root.resolve()

        for root, dirs, files in os.walk(repo_root):
            # Modify dirs in-place to skip hidden directories (e.g. .git)
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            
            for file in files:
                full_path = Path(root) / file
                rel_path = str(full_path.relative_to(repo_root))

                if self.should_process(rel_path):
                    try:
                        metrics = self._analyze_file(full_path)
                        if metrics:
                            # Add path as relative
                            metrics.file_path = rel_path
                            results.append(metrics)
                    except Exception as e:
                        logger.warning(f"Failed to analyze {rel_path}: {e}")

        return results

    def _analyze_file(self, file_path: Path) -> Optional[FileComplexity]:
        # Currently only Python is supported via radon
        if not file_path.suffix == ".py":
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                code = f.read()
            
            # Radon analysis
            blocks = cc_visit(code)
            
            functions_count = len(blocks)
            cyclomatic_total = sum(b.complexity for b in blocks)
            cyclomatic_avg = cyclomatic_total / functions_count if functions_count > 0 else 0.0
            
            high_count = sum(1 for b in blocks if b.complexity > self.high_threshold)
            very_high_count = sum(1 for b in blocks if b.complexity > self.very_high_threshold)
            
            # Count LOC (simple line count for now, or use radon's raw metrics if needed)
            loc = len(code.splitlines())

            return FileComplexity(
                file_path=str(file_path),
                language="python",
                loc=loc,
                functions_count=functions_count,
                cyclomatic_total=cyclomatic_total,
                cyclomatic_avg=cyclomatic_avg,
                high_complexity_functions=high_count,
                very_high_complexity_functions=very_high_count
            )
        except Exception:
            # Syntax errors or other issues
            return None
