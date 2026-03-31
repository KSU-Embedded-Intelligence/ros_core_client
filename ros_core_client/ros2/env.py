import logging
from typing import Any, Dict, Iterable, Iterator, List, Union

import grpc

from .communicator import GatewayClient, _response_payload


class Ros2Environment:
    """
    High-level environment interface for the ROS2 Gateway relay.

    Wraps GatewayClient and provides the following operations:

      status()                         — query relay and store command_map locally
      manage_node(command, params)     — lifecycle commands (start/stop/configure/reset/ping)
      exchange(commands)               — unary data exchange
      exchange_stream(frames)          — client-stream data exchange
      exchange_server_stream(commands) — server-stream data exchange
      reset(nodes)                     — convenience wrapper: manage_node("reset", nodes)
      close()                          — close the gRPC channel

    Connection is verified during __init__ by calling status(); a
    ConnectionError is raised immediately if the relay is unreachable.

    Usage
    ─────
        env = Ros2Environment("localhost", 50051)  # raises ConnectionError if down

        # start nodes
        env.manage_node("start", {"lidar_front": {}})
        env.manage_node("configure", {"lidar_front": {"num_rays": 360}, "lidar_rear": {}})

        # exchange data — unary
        result = env.exchange({"get_scan": {}})

        # exchange data — client stream (send N frames, get summary)
        frames = [{"set_speed": {"linear_x": round(i * 0.01, 2)}} for i in range(30)]
        summary = env.exchange_stream(frames)

        # exchange data — server stream (send once, iterate reply frames)
        for frame in env.exchange_server_stream({"get_scan": {}}):
            scan = frame["get_scan"]

        # reset nodes
        env.reset({"lidar_front": {}, "lidar_rear": {}})

        env.close()
    """

    def __init__(self, host: str, port: int, max_message_mb: int = 20) -> None:
        self.logger = logging.getLogger(type(self).__name__)
        self.comm = GatewayClient(host, port, max_message_mb=max_message_mb)
        self.command_map: Dict[str, Any] = {}

        # Verify the relay is reachable by fetching status once at startup.
        # GatewayClient channels are lazy; this forces the first real RPC so
        # a broken connection is caught here rather than on the first use.
        try:
            self.status()
        except grpc.RpcError as exc:
            self.comm.close()
            raise ConnectionError(
                f"Cannot reach ROS2 Gateway relay at {host}:{port}: {exc.details()}"
            ) from exc

        self.logger.info("Ros2Environment ready → %s:%d", host, port)

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> Dict[str, Any]:
        """
        Query relay status and store the command map for later reference.

        Sends CMD_STATUS to the relay and caches the returned command_map
        in self.command_map so callers can inspect available commands without
        making additional network calls.

        Returns:
            dict with keys:
              command_map  — {command_name: {node_name, api_entry, message_type}}
              known_nodes  — list of registered driver node names
              node_states  — {node_name: lifecycle_state_string}

        Example:
            info = env.status()
            print(env.command_map)   # cached after the call
        """
        info = self.comm.status()
        self.command_map = info.get("command_map", {})
        self.logger.debug("status fetched; %d commands in map", len(self.command_map))
        return info

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def manage_node(self, command: str, params: Dict[str, dict]) -> bool:
        """
        Execute a lifecycle command on one or more nodes.

        Args:
            command: one of "start", "stop", "configure", "reset", "ping"
            params:  {node_name: {param_key: value, ...}, ...}
                     Use {} as the value when no parameters are needed.
                     Multiple nodes are dispatched concurrently by the relay.

        Returns:
            True if the relay accepted the command, False otherwise.

        Examples:
            env.manage_node("start",     {"lidar_front": {}})
            env.manage_node("configure", {"lidar_front": {"num_rays": 360}})
            env.manage_node("ping",      {"lidar_front": {}, "lidar_rear": {}})
        """
        dispatch = {
            "start":     self.comm.start,
            "stop":      self.comm.stop,
            "configure": self.comm.configure,
            "reset":     self.comm.reset,
            "ping":      self.comm.ping,
        }

        key = command.lower().strip()
        if key not in dispatch:
            raise ValueError(
                f"Unknown command '{command}'. "
                f"Valid commands: {list(dispatch.keys())}"
            )

        return dispatch[key](params)

    # ── Exchange — Unary ──────────────────────────────────────────────────────

    def exchange(self, commands: Dict[str, Any]) -> Dict[str, Any]:
        """
        Exchange data with driver nodes — Unary transport.

        Sends one dict of commands, receives one dict of results.
        Commands that route to different nodes are dispatched concurrently
        by the relay.

        Args:
            commands: {command_name: data, ...}

        Returns:
            {command_name: result, ...}

        Examples:
            result = env.exchange({"get_scan": {}})
            result = env.exchange({
                "set_speed": {"linear_x": 0.5, "angular_z": 0.1},
                "get_scan":  {},
            })
        """
        return self.comm.exchange(commands)

    # ── Exchange — ClientStream ───────────────────────────────────────────────

    def exchange_stream(
        self,
        frames: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """
        Exchange data with driver nodes — ClientStream transport.

        Streams N command dicts to the relay; receives one summary dict.
        Each frame is processed independently by the relay.

        Args:
            frames: iterable of {command_name: data, ...} dicts.
                    A single dict is automatically wrapped in a list.

        Returns:
            Summary dict with keys "frames_processed" and "errors".

        Example:
            summary = env.exchange_stream([
                {"set_speed": {"linear_x": round(i * 0.01, 2), "angular_z": 0.0}}
                for i in range(30)
            ])
        """
        if isinstance(frames, dict):
            frames = [frames]
        reply = self.comm.exchange_stream(frames)
        return _response_payload(reply)

    # ── Exchange — ServerStream ───────────────────────────────────────────────

    def exchange_server_stream(
        self,
        commands: Dict[str, Any],
    ) -> Iterator[Dict[str, Any]]:
        """
        Exchange data with driver nodes — ServerStream transport.

        Sends one command dict, yields each reply frame's payload dict as it
        arrives from the relay.

        Args:
            commands: {command_name: data, ...}

        Yields:
            {command_name: result, ...} for each reply frame.

        Example:
            for frame in env.exchange_server_stream({"get_scan": {}}):
                scan = frame["get_scan"]
        """
        yield from self.comm.exchange_server_stream(commands)

    # ── Convenience ───────────────────────────────────────────────────────────

    def reset(self, nodes: Dict[str, dict]) -> bool:
        """
        Reset one or more driver nodes.

        Convenience wrapper for manage_node("reset", nodes).
        Cycles each node through deactivate → cleanup → configure, leaving it
        in inactive state ready for the next activation.

        Args:
            nodes: {node_name: {param_key: value, ...}, ...}
                   Use {} as the value when no reset parameters are needed.

        Returns:
            True if all nodes were reset successfully, False otherwise.

        Example:
            env.reset({"lidar_front": {}, "lidar_rear": {}})
        """
        return self.manage_node("reset", nodes)

    # ── Connection ────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        self.comm.close()
        self.logger.info("Ros2Environment closed")

    def __enter__(self) -> "Ros2Environment":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
