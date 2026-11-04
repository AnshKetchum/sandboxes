"""Shared setup for the Prime scripts: the key, the team, the cost ledger and the run labels, set before `sandboxes` is
imported (the ledger path is read then).

    --env-file PATH   read PRIME_API_KEY (or PRIME_INTELLECT_API_KEY) from an env file when the shell did not export it
    --team TEAM_ID    bill this team's wallet (else PRIME_TEAM_ID; else the key's personal wallet)
    --ledger PATH     where sandbox costs go (else SANDBOX_COST_LEDGER, else the library default)
The key is never printed."""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
KEY_NAMES = ("PRIME_API_KEY", "PRIME_INTELLECT_API_KEY")


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env-file", help="env file holding PRIME_API_KEY or PRIME_INTELLECT_API_KEY")
    parser.add_argument("--team", help="Prime team id to bill (default: $PRIME_TEAM_ID)")
    parser.add_argument("--ledger", help="cost ledger path (default: $SANDBOX_COST_LEDGER)")


def apply_common_args(args: argparse.Namespace, run_id: str) -> None:
    """Set the environment the backend reads, and make the repo importable."""
    if not os.environ.get("PRIME_API_KEY") and args.env_file:
        for line in open(args.env_file):
            name, _, value = line.strip().partition("=")
            if name in KEY_NAMES and value:
                os.environ["PRIME_API_KEY"] = value.strip().strip('"').strip("'")
                break
    if not os.environ.get("PRIME_API_KEY"):
        raise SystemExit("PRIME_API_KEY is not set (export it or pass --env-file)")
    if args.team:
        os.environ["PRIME_TEAM_ID"] = args.team
    if args.ledger:
        os.environ["SANDBOX_COST_LEDGER"] = str(Path(args.ledger).resolve())
    os.environ.setdefault("SANDBOX_RUN_ID", run_id)
    os.environ.setdefault("SANDBOX_OWNER", "prime-scripts")
    sys.path.insert(0, str(ROOT))
