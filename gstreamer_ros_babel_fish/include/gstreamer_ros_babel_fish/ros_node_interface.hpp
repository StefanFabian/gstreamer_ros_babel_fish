/*
 *  gstreamer_ros_babel_fish - GStreamer ROS interface.
 *  Copyright (C) 2026  Stefan Fabian
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#ifndef GSTREAMER_ROS_BABEL_FISH__ROS_NODE_INTERFACE_HPP_
#define GSTREAMER_ROS_BABEL_FISH__ROS_NODE_INTERFACE_HPP_

#include <atomic>
#include <memory>
#include <mutex>
#include <rclcpp/experimental/executors/events_executor/events_executor.hpp>
#include <rclcpp/rclcpp.hpp>
#include <string>
#include <thread>

namespace gstreamer_ros_babel_fish
{

class RosNodeInterface
{
public:
  RosNodeInterface();
  ~RosNodeInterface();

  // Initialize with an optional external node
  // If external_node is nullptr, creates an internal node with the given name
  bool initialize( rclcpp::Node::SharedPtr external_node, const std::string &node_name );

  // Shutdown the interface
  void shutdown();

  // Get the node (either external or internal)
  rclcpp::Node::SharedPtr get_node() const;

  // Check if initialized
  bool is_initialized() const;

private:
  void ensure_initialized() const;

  struct Private;
  static std::unique_ptr<Private> priv_;
  static std::mutex priv_mutex_;

  rclcpp::Node::SharedPtr node_;
  bool owns_node_;
};

} // namespace gstreamer_ros_babel_fish

#endif // GSTREAMER_ROS_BABEL_FISH__ROS_NODE_INTERFACE_HPP_
