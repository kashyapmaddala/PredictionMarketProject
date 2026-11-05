"""Check that the saved Polymarket data is right.

Reads the raw files saved by the collector, rebuilds every order book from them message by
message, and checks: our rebuilt books against Polymarket's own best prices (every change) and
against the independent REST copies (every level); sanity (crossed books, prices on the tick,
trades between 0 and 1, messages with bad fields); gaps (when tokens had no live book, the real
blind time, and pauses such as the computer sleeping); damaged or truncated raw files.

Nothing is changed: the raw files are only read, so this can be re-run any time. Memory stays
small however much data there is. The report is printed and saved to
data/reports/polymarket/<UTC time>.txt. Exit code 0 = PASS, 1 = problems found.

    python3 check_data.py                  # every saved file
    python3 check_data.py 2026-11-03       # one UTC day
"""
import re
import sys

from pipeline.config import Paths
from pipeline.polymarket.validate import save_report, validate


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    day = argv[0] if argv else None
    if len(argv) > 1 or (day and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day)):
        sys.exit(__doc__)
    paths = Paths.from_env()
    report = validate(paths, day)
    print(report.text)
    path = save_report(paths, report.text)
    print(f"\nSaved: {path}")
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
