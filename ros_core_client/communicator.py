import logging
import pickle
import sys

import grpc
from PIL import Image

from .communication_objects.relay_service_pb2 import CommandRequestMessage
from .communication_objects.relay_service_pb2_grpc import RpcCommunicatorStub


class RpcCommunicatorClient:
    def __init__(self, rpc_ip, rpc_port):
        # setup logger
        self.logger = logging.getLogger(type(self).__name__)

        ros_addr = rpc_ip + ':' + str(rpc_port)
        options = [('grpc.max_receive_message_length', 20 * 1024 * 1024)]
        channel = grpc.insecure_channel(ros_addr, options=options)
        stub = RpcCommunicatorStub(channel)

        self.stub = stub
        self.channel = channel

    def rpc_call(self, node_name, command, *args, **kwargs):
        # make sure the node is started
        try:
            rpc_request = CommandRequestMessage()
            rpc_request.time.GetCurrentTime()
            rpc_request.node_name = node_name
            rpc_request.command = command
            rpc_request.request = pickle.dumps({"args": args, "kwargs": kwargs})

            rpc_response = self.stub.ExecuteCommand(rpc_request)
        except grpc.RpcError as e:
            self.logger.warning(f"grpc error triggered when trying to communicate with ros {e}")
            return None
        return rpc_response

    def get_server_version(self):
        rpc_response = self.rpc_call("ros_relay_node", "")
        server_version = rpc_response.version
        return server_version

    def start_node(self, node_name, *args, **kwargs):
        rpc_response = self.rpc_call(node_name, "start", *args, **kwargs)
        if rpc_response is None:
            return None
        self.logger.info(f"start node {node_name} with args {args} and kwargs {kwargs}\n"
                         f"time: {rpc_response.time}"
                         f"version: {rpc_response.version}")
        return pickle.loads(rpc_response.response)

    def stop_node(self, node_name, *args, **kwargs):
        rpc_response = self.rpc_call(node_name, "stop", *args, **kwargs)
        if rpc_response is None:
            return None
        self.logger.info(f"stop node {node_name} with args {args} and kwargs {kwargs}\n"
                         f"time: {rpc_response.time}"
                         f"version: {rpc_response.version}")

    def reset_node(self, node_name, *args, **kwargs):
        rpc_response = self.rpc_call(node_name, "reset", *args, **kwargs)
        if rpc_response is None:
            return {}
        return pickle.loads(rpc_response.response)

    def exchange_node(self, node_name, *args, **kwargs):
        rpc_response = self.rpc_call(node_name, "exchange", *args, **kwargs)
        if rpc_response is None:
            return {}
        return pickle.loads(rpc_response.response)

    def close(self):
        self.channel.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        force=True,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    rpc_ip = "192.168.122.125"
    rpc_port = 50051
    comm = RpcCommunicatorClient(rpc_ip, rpc_port)
    comm.start_node("camera_node")
    data_dict = comm.exchange_node("camera_node", *["camera_rgb"])
    image = Image.fromarray(data_dict["camera_rgb"])
    image.show()
    # print(data_dict['camera_rgb'].shape)
    # comm.start_node("goal_node")
    # data_dict = comm.exchange_node("goal_node", *["goal_status"])
    # comm.start_node("lidar_node")
    # data_dict = comm.exchange_node("lidar_node", *["lidar_angles", "lidar_vector"])
    # comm.start_node("map_node")
    # data_dict = comm.exchange_node("map_node", *["map_occupation"])
    # comm.start_node("odom_node")
    # time.sleep(1)
    # data_dict = comm.exchange_node("odom_node", *["odom_pose"])
    # comm.start_node("rfid_reader_node")
    # data_dict = comm.exchange_node("rfid_reader_node", *["rfid_data"])

    # comm.start_node("unity_node")
    # unity_system_argument_dict = {
    #     "isRandom": True,
    #     "rackNum": 2,
    #     "racks": [],
    #     "rackPosXMin": 5.5,
    #     "rackPosXMax": 19.5,
    #     "rackPosZMin": 5.5,
    #     "rackPosZMax": 19.5,
    #     "rackDistanceMin": 3,
    #     "rackDistanceMax": 15,
    #     "rackRadiusMin": 0,
    #     "rackRadiusMax": 0,
    #     "rackHeightMin": 0,
    #     "rackHeightMax": 0,
    # }
    # data_dict = comm.reset_node("unity_node", **{"unity_system_argument": unity_system_argument_dict,})
    # print(data_dict)
    # time.sleep(1)
    # data_dict = comm.exchange_node("unity_node", *["unity_rack_info"])
    # print(data_dict)
    comm.close()


if __name__ == '__main__':
    main()
