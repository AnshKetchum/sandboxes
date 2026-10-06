"""Import shims for optional provider SDKs."""

import sys
import types


def ensure_blaxel_importable() -> None:
    """`import blaxel` pulls in `mcp.server.fastmcp`, which mcp 2.x renamed (FastMCP -> MCPServer). We only use
    Blaxel's sandbox API, never its MCP server, so alias the old name instead of pinning mcp<2 for every
    environment that also needs mcp 2 (hermes/nexau)."""
    try:
        import mcp.client.streamable_http as sh
    except ImportError:
        return        # no mcp at all: blaxel will raise its own clear error
    if not hasattr(sh, "streamablehttp_client") and hasattr(sh, "streamable_http_client"):
        sh.streamablehttp_client = sh.streamable_http_client      # renamed in mcp 2.x
    try:
        import mcp.server.fastmcp  # noqa: F401
        return
    except ImportError:
        pass
    try:
        from mcp.server.mcpserver import MCPServer
    except ImportError:
        return
    shim = types.ModuleType("mcp.server.fastmcp")
    shim.FastMCP = MCPServer
    sys.modules["mcp.server.fastmcp"] = shim
    import mcp.server
    mcp.server.fastmcp = shim
