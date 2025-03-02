import logging
from .communicator import RpcCommunicatorClient


class Ros1Environment:
    VERSION = "2.0"

    def __init__(self, rpc_ip: str, rpc_port: int):
        self.logger = logging.getLogger(type(self).__name__)

        comm = RpcCommunicatorClient(rpc_ip, rpc_port)
        server_version = comm.get_server_version()
        if server_version != self.VERSION:
            self.logger.warning(f"server version {server_version} does not match client version {self.VERSION}")

        self.comm = comm
        self.logger.warning("Ros1Environment initialized")

    def init_nodes(self, args_list, kwargs_dict):
        return self._perform_comm_operation(self.comm.start_node, args_list, kwargs_dict)

    def reset(self, args_list, kwargs_dict):
        return self._perform_comm_operation(self.comm.reset_node, args_list, kwargs_dict)

    def collect_observations(self, args_list, kwargs_dict):
        return self._perform_comm_operation(self.comm.exchange_node, args_list, kwargs_dict)

    def close(self):
        self.comm.close()
        self.logger.warning("Ros1Environment closed")

    @staticmethod
    def _perform_comm_operation(operation, args_list, kwargs_dict):
        """
        Groups parameters by node driver and executes the given operation for each group.

        Both args_list and kwargs_dict are expected to have a prefix in the format
        "[node_name]_..." where the node name is the part before the first underscore.
        The driver name is constructed as "[node_name]_node".

        Parameters:
            operation (callable): A method on self.comm to be called for each driver.
                                  Expected signature: operation(driver_name, *args, **kwargs)
            args_list (list): List of strings formatted as "[node_name]_...".
            kwargs_dict (dict): Dictionary where keys are formatted as "[node_name]_..." and
                                values are the corresponding parameters.

        Returns:
            dict: A dictionary with keys as driver names and values as the result
                  of executing the operation for that driver.
        """
        grouped = {}

        # Process positional arguments.
        for arg in args_list:
            if arg:
                # Extract the node name from the first part of the string.
                parts = arg.split('_', 1)
                node_name = parts[0]
                driver_name = f"{node_name}_node"
                if driver_name not in grouped:
                    grouped[driver_name] = {"args": [], "kwargs": {}}
                grouped[driver_name]["args"].append(arg)

        # Process keyword arguments.
        for key, value in kwargs_dict.items():
            if key:
                parts = key.split('_', 1)
                node_name = parts[0]
                driver_name = f"{node_name}_node"
                if driver_name not in grouped:
                    grouped[driver_name] = {"args": [], "kwargs": {}}
                grouped[driver_name]["kwargs"][key] = value

        # Execute the operation for each grouped driver.
        merged_result = {}
        for driver, params in grouped.items():
            op_result = operation(driver, *params["args"], **params["kwargs"])
            if op_result is None:
                # rpc communication error
                # most likely happens if start nodes multiple times
                continue
            merged_result.update(op_result)
        return merged_result


def main():
    logging.basicConfig(
        level=logging.INFO,
        force=True,
        handlers=[
            logging.StreamHandler()
        ]
    )
    rpc_ip = "192.168.122.125"
    rpc_port = 50051

    env = Ros1Environment(rpc_ip, rpc_port)
    init_dict = env.init_nodes(["camera_node", "goal_node", "lidar_node", "map_node", "odom_node", "rfid_node", "unity_node"], {})
    observation_dict = env.collect_observations(["odom_pose", "goal_status", "lidar_angles", "lidar_vector"], {})
    pass
    env.close()

if __name__ == '__main__':
    main()
