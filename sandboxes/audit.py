"""Find provider-specific code in a consuming project. The rule: only the sandboxes library knows what Daytona, E2B, Blaxel, Modal or
Fly are. A project that generates environments or data must not import a provider SDK, name a provider's class or credential, or
branch on a provider's name; it asks the library (sandboxes.fleet, the router, the harbor adapter) and gets the same answer on any
provider. This scanner lists the places that break the rule.

    python -m sandboxes audit /ansh/euphoria/euphoria /ansh/greenhouse/src          # exit 1 if it finds any
A line carrying `# sandboxes-audit: ok` is allowed (use it, with a reason, for a deliberate exception).
"""

import fnmatch
import os
import re
import sys

PATTERNS = [
    (re.compile(r"^\s*(import|from)\s+(daytona|e2b|blaxel|modal|fly)\b"), "provider SDK import"),
    (re.compile(r"harbor\.environments\.(daytona|e2b|blaxel|modal|fly)"), "harbor provider environment"),
    (re.compile(r"\b(DaytonaEnvironment|AsyncDaytona|DaytonaError|DaytonaClientManager|E2BEnvironment|BlaxelEnvironment|SandboxInstance)\b"), "provider class"),
    (re.compile(r"\b(DAYTONA_API_KEY|E2B_API_KEY|BL_API_KEY|BL_WORKSPACE|FLY_API_KEY|MODAL_TOKEN_(ID|SECRET))\b"), "provider credential"),
    (re.compile(r"""(==|!=|\bin\b)\s*[(\["']*\s*["'](daytona|e2b|blaxel|modal|fly)["']"""), "branch on provider name"),
]
SKIP_DIRS = {".venv", "venv", "__pycache__", "site-packages", ".git", "node_modules", "runs", "logs"}
EXTS = (".py", ".sh", ".yaml", ".yml", ".toml")


def _ignores(roots) -> list:
    """Globs from a `.sandboxes-audit-ignore` file in a scanned root (one per line, `#` comments): for analysis scripts of past
    experiments that name providers as data, not as sandbox operations."""
    out = []
    for r in roots:
        f = os.path.join(r if os.path.isdir(r) else os.path.dirname(r), ".sandboxes-audit-ignore")
        if os.path.exists(f):
            out += [l.strip() for l in open(f) if l.strip() and not l.startswith("#")]
    return out


def scan(paths) -> list[tuple[str, int, str, str]]:
    hits = []
    roots = list(paths)
    for root in paths:
        files = [root] if os.path.isfile(root) else [os.path.join(d, f) for d, ds, fs in os.walk(root) if not (set(d.split(os.sep)) & SKIP_DIRS)
                                                       for f in fs if f.endswith(EXTS)]
        for path in files:
            if "/tests/" in path or path.endswith(("_test.py", "conftest.py")):
                continue
            if any(fnmatch.fnmatch(path, pat) or fnmatch.fnmatch(os.path.basename(path), pat) for pat in _ignores(roots)):
                continue
            try:
                lines = open(path, errors="ignore").read().splitlines()
            except OSError:
                continue
            for i, line in enumerate(lines, 1):
                if "sandboxes-audit: ok" in line or line.lstrip().startswith("#"):
                    continue
                for pat, why in PATTERNS:
                    if pat.search(line):
                        hits.append((path, i, why, line.strip()[:110])); break
    return hits


def main(paths) -> int:
    hits = scan(paths)
    for path, i, why, text in hits:
        print(f"{path}:{i}: {why}: {text}")
    by = {}
    for path, *_ in hits:
        by[path] = by.get(path, 0) + 1
    print(f"\n{len(hits)} provider-specific line(s) in {len(by)} file(s)" + ("" if hits else " - clean"))
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
