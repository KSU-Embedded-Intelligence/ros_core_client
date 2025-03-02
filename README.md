# ROS Driver Manager

## Name
**ROS Driver Manager**  
A unified Python package for managing and communicating with ROS node drivers via gRPC.

## Description
ROS Driver Manager provides a streamlined API to interact with various ROS node drivers in a robotics environment. It allows you to initialize nodes, reset them, and collect observations using a consistent interface over gRPC.  
Key features include:  
- Grouping of command parameters by node driver.
- Seamless integration with ROS through gRPC-based communication.
- Support for operations such as node initialization, reset, and observation exchange.

For more background on ROS, visit the [ROS website](https://www.ros.org/).

## Badges
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

[//]: # (## Visuals)

[//]: # (*Insert screenshots or GIFs here to showcase the system in action. For example:*  )

[//]: # (![Demo Screenshot]&#40;https://via.placeholder.com/800x400?text=ROS+Driver+Manager+Demo&#41;)

## Installation
To install ROS Driver Manager, run following command:
```bash
pip install git+https://jlab-git.ayayadomain.com/gitlab/ayaya/ros_core_client.git
```

## Usage
```python
from ros_core_client import Ros1Environment

# Initialize the ROS environment with the gRPC server IP and port.
env = Ros1Environment("192.168.122.125", 50051)

# Initialize nodes (arguments should follow the expected naming convention).
init_result = env.init_nodes(
    ["camera_node", "lidar_node", "map_node"],
    {}
)
print("Initialization Result:", init_result)

# Collect observations from nodes.
observations = env.collect_observations(
    ["odom_pose", "lidar_angles"],
    {}
)
print("Observations:", observations)

# Close the environment when done.
env.close()

```

## Modules
### `Ros1Environment`
main class interface for the ros_core_client

**API Functions**
* `init_nodes(args_list, kwargs_dict)`, Initializes the specified ROS nodes
* `reset(args_list, kwargs_dict)`, Resets the specified ROS nodes
* `collect_observations(args_list, kwargs_dict)`, Collects observations from the specified ROS nodes

**Parameters:**
- **args_list (list):**  
  A list of strings representing the identifiers for the operations. Each string should follow the format `[node_name]_...`, for example `["camera_node", "lidar_node", "map_node"]`
- **kwargs_dict (dict):**  
  A dictionary of additional key-value pairs formatted as `[node_name]_...` to be sent, for example `{"goal_goal": [1, 0, 0, 0, 0, 0, 1]}`

**Returns:**  
A dictionary containing the results of the requested observations of operation information

### `CommandRequestMessage`
grpc request message class
```protobuf
message CommandRequestMessage {
    google.protobuf.Timestamp time = 1;
    string node_name = 2;
    string command = 3;
    bytes request = 4;
}
```

## Version Update Information

**v0.1.0**

- Initial release with core functionality including node initialization, reset, and observation collection.
- Grouping of parameters by node driver with gRPC communication.
- Basic error handling and logging.
