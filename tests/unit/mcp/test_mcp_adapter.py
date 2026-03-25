"""Tests for MCPAdapter."""

import pytest

from sdol.mcp.mcp_adapter import (
    MCPAdapter,
    MCPResponse,
    MCPServerConfig,
    MCPToolCall,
    MockMCPTransport,
)
from sdol.types.errors import MCPTransportError


class TestMCPAdapter:
    @pytest.mark.asyncio
    async def test_routes_to_correct_server(self) -> None:
        transport = MockMCPTransport()
        transport.set_response("get_data", MCPResponse(content={"result": 42}))
        adapter = MCPAdapter(transport)
        adapter.register_server(MCPServerConfig(
            server_id="test-server",
            server_url="http://localhost:8080",
        ))
        response = await adapter.call(
            "test-server",
            MCPToolCall(tool_name="get_data", parameters={}),
        )
        assert response.content == {"result": 42}

    @pytest.mark.asyncio
    async def test_raises_on_unknown_server(self) -> None:
        adapter = MCPAdapter(MockMCPTransport())
        with pytest.raises(MCPTransportError):
            await adapter.call(
                "nonexistent",
                MCPToolCall(tool_name="get_data", parameters={}),
            )

    @pytest.mark.asyncio
    async def test_returns_empty_for_unknown_tool(self) -> None:
        transport = MockMCPTransport()
        adapter = MCPAdapter(transport)
        adapter.register_server(MCPServerConfig(
            server_id="s1", server_url="http://localhost"
        ))
        response = await adapter.call(
            "s1",
            MCPToolCall(tool_name="unknown_tool", parameters={}),
        )
        assert response.content == []
