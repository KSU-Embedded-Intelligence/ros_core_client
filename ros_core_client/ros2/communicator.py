"""
communicator.py — GatewayClient for the ROS2 Gateway relay.

Connects to the relay's gRPC server and wraps all three transport paradigms
(Unary, ClientStream, ServerStream) with a high-level API that mirrors the
gateway payload convention exactly — every command takes a dict that is sent
as-is in the GatewayEnvelope payload.

The relay runs inside Docker; the client runs on the host.
The Docker container must expose port 50051 (or whatever port the relay binds).

─────────────────────────────────────────────────────────────────────────────
Payload conventions (same as gateway.proto comments and relay_processor.py)
─────────────────────────────────────────────────────────────────────────────

CMD_EXCHANGE — payload is {command_name: data, ...}
  The relay resolves each command_name → node via the command map, groups
  commands that share a node, and dispatches them concurrently.
  Examples:
    {"get_scan": {}}
    {"set_speed": {"linear_x": 0.5, "angular_z": 0.1}, "get_scan": {}}

CMD_START / CMD_STOP / CMD_CONFIGURE / CMD_RESET / CMD_PING
  payload is {node_name: {params}, ...}
  Multiple nodes are dispatched concurrently by the relay.
  Examples:
    {"test_lidar_front": {}}
    {"lidar_front": {"mode": "high_res"}, "lidar_rear": {}}

CMD_STATUS — empty payload.
  Response contains command_map, known_nodes, node_states.

─────────────────────────────────────────────────────────────────────────────
Three gRPC transports (same GatewayEnvelope message type throughout)
─────────────────────────────────────────────────────────────────────────────
  Unary        GatewayEnvelope → GatewayEnvelope           request / response
  ClientStream stream GatewayEnvelope → GatewayEnvelope   N frames → 1 reply
  ServerStream GatewayEnvelope → stream GatewayEnvelope   1 request → N frames

─────────────────────────────────────────────────────────────────────────────
Usage
─────────────────────────────────────────────────────────────────────────────
    from ros_core_client.ros2.communicator import GatewayClient

    with GatewayClient("localhost", 50051) as client:
        # query relay state
        info = client.status()

        # lifecycle — single node
        client.start({"test_lidar_front": {}})
        client.stop({"test_lidar_front": {}})
        client.reset({"test_lidar_front": {}})

        # lifecycle — multiple nodes concurrently
        client.start({"lidar_front": {}, "lidar_rear": {}})

        # exchange — Unary (one round-trip)
        result = client.exchange({"get_scan": {}})

        # exchange — ClientStream (stream N frames, get summary)
        frames = [{"get_scan": {}} for _ in range(10)]
        reply  = client.exchange_stream(frames)

        # exchange — ServerStream (send once, iterate reply frames)
        for frame in client.exchange_server_stream({"get_scan": {}}):
            scan = frame["get_scan"]

─────────────────────────────────────────────────────────────────────────────
Proto stubs
─────────────────────────────────────────────────────────────────────────────
gateway_pb2 / gateway_pb2_grpc are in ros_core_client/ros2/proto/ and are
regenerated automatically during package installation via the hatchling build
hook (hatch_build.py).  See proto/gateway.proto for the source schema.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Dict, Iterable, Iterator, Optional

import grpc
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import MessageToDict

from .proto import gateway_pb2, gateway_pb2_grpc


PROTOCOL_VERSION = "0.1.0"


# ── Envelope helpers (no ROS dependency) ────────────────────────────────────

def _now_ts() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    return ts


def _dict_to_struct(d: dict) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(d)
    return s


def _struct_to_dict(s: struct_pb2.Struct) -> dict:
    return MessageToDict(s)


def _make_envelope(
    command: int,
    payload: Optional[dict] = None,
) -> "gateway_pb2.GatewayEnvelope":
    env = gateway_pb2.GatewayEnvelope(
        time    = _now_ts(),
        version = PROTOCOL_VERSION,
        status  = gateway_pb2.STATUS_UNSPECIFIED,
        command = command,
    )
    if payload:
        env.payload.CopyFrom(_dict_to_struct(payload))
    return env


def _response_payload(reply: "gateway_pb2.GatewayEnvelope") -> dict:
    if reply.HasField("payload"):
        return _struct_to_dict(reply.payload)
    return {}


# ── GatewayClient ─────────────────────────────────────────────────────────────

class GatewayClient:
    """
    gRPC client for the ROS2 Gateway relay.

    High-level API
    ──────────────
    status()                              → CMD_STATUS
    exchange(commands)                    → CMD_EXCHANGE  Unary
    exchange_stream(frames)               → CMD_EXCHANGE  ClientStream
    exchange_server_stream(commands)      → CMD_EXCHANGE  ServerStream
    start(nodes)                          → CMD_START
    stop(nodes)                           → CMD_STOP
    configure(nodes)                      → CMD_CONFIGURE
    reset(nodes)                          → CMD_RESET
    ping(nodes)                           → CMD_PING

    All lifecycle methods accept {node_name: params_dict, ...} so multiple
    nodes can be targeted in one call (the relay dispatches them concurrently).
    All exchange methods accept {command_name: data, ...} (same convention).

    Low-level transport
    ───────────────────
    unary(command, payload)               raw Unary RPC
    client_stream(envelopes)              raw ClientStream RPC
    server_stream(command, payload)       raw ServerStream RPC
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        max_message_mb: int = 20,
    ) -> None:
        self.logger = logging.getLogger(type(self).__name__)
        options = [("grpc.max_receive_message_length", max_message_mb * 1024 * 1024)]
        self._channel = grpc.insecure_channel(f"{host}:{port}", options=options)
        self._stub    = gateway_pb2_grpc.GatewayServiceStub(self._channel)
        self.logger.info(f"GatewayClient → {host}:{port}")

    # ── Low-level transport ───────────────────────────────────────────────────

    def unary(
        self,
        command: int,
        payload: Optional[dict] = None,
    ) -> "gateway_pb2.GatewayEnvelope":
        """Send one envelope, receive one reply."""
        return self._stub.Unary(_make_envelope(command, payload))

    def client_stream(
        self,
        envelopes: Iterable["gateway_pb2.GatewayEnvelope"],
    ) -> "gateway_pb2.GatewayEnvelope":
        """Stream N envelopes to the relay, receive one summary reply."""
        return self._stub.ClientStream(iter(envelopes))

    def server_stream(
        self,
        command: int,
        payload: Optional[dict] = None,
    ) -> Iterator["gateway_pb2.GatewayEnvelope"]:
        """Send one envelope, yield each reply frame as it arrives."""
        yield from self._stub.ServerStream(_make_envelope(command, payload))

    # ── CMD_STATUS ────────────────────────────────────────────────────────────

    def status(self) -> Dict[str, Any]:
        """
        Query relay status (no payload required).

        Returns dict with keys:
          command_map  — {command_name: {node_name, api_entry, message_type}}
          known_nodes  — list of registered driver node names
          node_states  — {node_name: lifecycle_state_string}
        """
        reply = self.unary(gateway_pb2.CMD_STATUS)
        self.logger.debug("status → %s", gateway_pb2.Status.Name(reply.status))
        return _response_payload(reply)

    # ── CMD_EXCHANGE ──────────────────────────────────────────────────────────
    # Three functions, one per gRPC transport.  All take the same dict format:
    #   {command_name: data, ...}

    def exchange(self, commands: Dict[str, Any]) -> Dict[str, Any]:
        """
        CMD_EXCHANGE — Unary transport.

        Send one dict of commands, receive one dict of results.
        Commands that route to different nodes are dispatched concurrently
        by the relay.  Commands sharing a node are batched into one letter.

        Args:
            commands: {command_name: data, ...}
                      data can be a dict, list, or any Struct-compatible value.

        Returns:
            {command_name: result, ...}

        Examples:
            result = client.exchange({"get_scan": {}})
            result = client.exchange({
                "set_speed": {"linear_x": 0.5, "angular_z": 0.1},
                "get_scan":  {},
            })
        """
        reply = self.unary(gateway_pb2.CMD_EXCHANGE, payload=commands)
        self.logger.debug(
            "exchange %s → %s", list(commands.keys()),
            gateway_pb2.Status.Name(reply.status),
        )
        return _response_payload(reply)

    def exchange_stream(
        self,
        frames: Iterable[Dict[str, Any]],
    ) -> "gateway_pb2.GatewayEnvelope":
        """
        CMD_EXCHANGE — ClientStream transport.

        Stream N command dicts to the relay; receive one summary envelope.
        Each frame is processed independently.  The reply contains
        frames_processed and a list of per-frame errors (if any).

        Args:
            frames: iterable of {command_name: data, ...} dicts.

        Returns:
            Summary GatewayEnvelope (check .status and .payload["frames_processed"]).

        Example:
            reply = client.exchange_stream([
                {"set_speed": {"linear_x": round(i * 0.01, 3), "angular_z": 0.0}}
                for i in range(30)
            ])
        """
        envs = [_make_envelope(gateway_pb2.CMD_EXCHANGE, payload=f) for f in frames]
        reply = self.client_stream(envs)
        self.logger.debug(
            "exchange_stream %d frames → %s  %s",
            len(envs), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply

    def exchange_server_stream(
        self,
        commands: Dict[str, Any],
    ) -> Iterator[Dict[str, Any]]:
        """
        CMD_EXCHANGE — ServerStream transport.

        Send one command dict, yield each reply frame's payload dict as it
        arrives.  (The current relay yields exactly one frame per request;
        the iterator is kept for forward compatibility with future streaming
        drivers.)

        Args:
            commands: {command_name: data, ...}

        Yields:
            {command_name: result, ...} for each reply frame.

        Example:
            for frame in client.exchange_server_stream({"get_scan": {}}):
                scan = frame["get_scan"]
        """
        for reply in self.server_stream(gateway_pb2.CMD_EXCHANGE, payload=commands):
            yield _response_payload(reply)

    # ── Lifecycle commands ────────────────────────────────────────────────────
    # All accept {node_name: params_dict, ...}.
    # Multiple nodes are dispatched concurrently by the relay.
    # Pass {} as params when no parameters are needed.

    def start(self, nodes: Dict[str, dict]) -> bool:
        """
        CMD_START — configure (if needed) then activate driver node(s).

        The relay's driver manager handles the configure+activate sequence
        automatically if a node is currently unconfigured.

        Args:
            nodes: {node_name: params_dict, ...}
                   Use {} as params when no parameters are needed.

        Examples:
            client.start({"test_lidar_front": {}})
            client.start({"lidar_front": {"mode": "high_res"}, "lidar_rear": {}})
        """
        reply = self.unary(gateway_pb2.CMD_START, payload=nodes)
        self.logger.debug(
            "start %s → %s  %s",
            list(nodes.keys()), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply.status == gateway_pb2.STATUS_OK

    def stop(self, nodes: Dict[str, dict]) -> bool:
        """
        CMD_STOP — deactivate driver node(s) (active → inactive).

        Args:
            nodes: {node_name: {}, ...}

        Example:
            client.stop({"test_lidar_front": {}})
        """
        reply = self.unary(gateway_pb2.CMD_STOP, payload=nodes)
        self.logger.debug(
            "stop %s → %s  %s",
            list(nodes.keys()), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply.status == gateway_pb2.STATUS_OK

    def configure(self, nodes: Dict[str, dict]) -> bool:
        """
        CMD_CONFIGURE — configure driver node(s) (unconfigured → inactive).

        Args:
            nodes: {node_name: params_dict, ...}

        Example:
            client.configure({"test_lidar_front": {"num_rays": 360}})
        """
        reply = self.unary(gateway_pb2.CMD_CONFIGURE, payload=nodes)
        self.logger.debug(
            "configure %s → %s  %s",
            list(nodes.keys()), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply.status == gateway_pb2.STATUS_OK

    def reset(self, nodes: Dict[str, dict]) -> bool:
        """
        CMD_RESET — cycle driver node(s): deactivate → cleanup → configure.

        Leaves each node in inactive state regardless of starting state.

        Args:
            nodes: {node_name: params_dict, ...}

        Example:
            client.reset({"test_lidar_front": {}})
        """
        reply = self.unary(gateway_pb2.CMD_RESET, payload=nodes)
        self.logger.debug(
            "reset %s → %s  %s",
            list(nodes.keys()), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply.status == gateway_pb2.STATUS_OK

    def ping(self, nodes: Dict[str, dict]) -> bool:
        """
        CMD_PING — query current lifecycle state of driver node(s).

        Returns True if the relay can reach all listed nodes and they respond.

        Args:
            nodes: {node_name: {}, ...}

        Example:
            alive = client.ping({"test_lidar_front": {}})
        """
        reply = self.unary(gateway_pb2.CMD_PING, payload=nodes)
        self.logger.debug(
            "ping %s → %s  %s",
            list(nodes.keys()), gateway_pb2.Status.Name(reply.status), reply.message,
        )
        return reply.status == gateway_pb2.STATUS_OK

    # ── Connection management ─────────────────────────────────────────────────

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "GatewayClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ── Quick smoke-test (run directly on host) ──────────────────────────────────

def main() -> None:
    """
    Minimal manual smoke-test.

    Prerequisites:
        ros2 launch gateway_launch test_gateway_stack.launch.py   # in Docker
    Then on host:
        python -m ros_core_client.ros2.communicator
    """
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    host = "192.168.253.14"
    port = 50051
    node = "test_lidar_front"

    print(f"\nConnecting to {host}:{port} ...")
    with GatewayClient(host, port) as client:
        info = client.status()
        print("\n── CMD_STATUS ──────────────────────────────────────────")
        print(json.dumps(info, indent=2))

        print(f"\n── CMD_PING  {node} ─────────────────────────────────")
        ok = client.ping({node: {}})
        print(f"  ping → {'OK' if ok else 'FAILED'}")

        print(f"\n── CMD_EXCHANGE  get_scan (Unary) ───────────────────")
        result = client.exchange({"get_scan": {}})
        scan   = result.get("get_scan", result.get("getScan", {}))
        ranges = scan.get("ranges", [])
        print(f"  len(ranges)={len(ranges)}")
        if ranges:
            print(f"  ranges[:5] = {[round(r, 3) for r in ranges[:5]]}")

        print(f"\n── CMD_EXCHANGE  get_scan (ServerStream) ────────────")
        for frame in client.exchange_server_stream({"get_scan": {}}):
            scan = frame.get("get_scan", frame.get("getScan", {}))
            print(f"  streamed frame  len(ranges)={len(scan.get('ranges', []))}")


if __name__ == "__main__":
    main()
