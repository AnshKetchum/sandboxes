"""Content addressing for Dockerfile build contexts, shared by every provider's `open_from_dockerfile`.

The digest is the same one euphoria (`euphoria.api._env_digest`) and greenhouse (`stages/taskio._env_digest`) tag their
precheck images with (sha256 over each file's relative path and bytes, sorted, first 16 hex), so an image this package
builds for a context is the image those callers already cache and prune under their own tag namespace.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional, Union


def resolve_dockerfile(context_dir: Union[str, Path], dockerfile: Optional[str] = None) -> Path:
    """The Dockerfile `open_from_dockerfile` builds: `dockerfile` (relative to `context_dir` unless absolute) or
    `context_dir`/Dockerfile."""
    ctx = Path(context_dir)
    if not dockerfile:
        return ctx / "Dockerfile"
    p = Path(dockerfile)
    return p if p.is_absolute() else ctx / p


def context_digest(context_dir: Union[str, Path], dockerfile: Optional[str] = None) -> str:
    """16 hex of sha256 over the context's files (relative path + bytes, sorted). A non-default Dockerfile is folded in
    too, so two Dockerfiles over one context get different digests; the default one is already part of the context."""
    ctx = Path(context_dir)
    h = hashlib.sha256()
    for p in sorted(ctx.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(ctx).as_posix().encode())
            try:
                h.update(p.read_bytes())
            except OSError:
                pass
    df = resolve_dockerfile(ctx, dockerfile)
    if dockerfile and df != ctx / "Dockerfile":
        h.update(b"\0dockerfile\0" + str(df).encode())
        try:
            h.update(df.read_bytes())
        except OSError:
            pass
    return h.hexdigest()[:16]
