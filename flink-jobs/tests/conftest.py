"""flink-jobs 测试路径注入：hunter_flink 包位于仓库根的 flink-jobs/ 下（非安装包）。"""
import sys
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1]
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))
