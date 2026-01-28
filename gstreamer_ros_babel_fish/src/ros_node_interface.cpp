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
#include "gstreamer_ros_babel_fish/ros_node_interface.hpp"

#include <rclcpp/version.h>

namespace gstreamer_ros_babel_fish
{

std::unique_ptr<RosNodeInterface::Private> RosNodeInterface::priv_ = nullptr;
std::mutex RosNodeInterface::priv_mutex_;

struct RosNodeInterface::Private {
  Private()
  {
    // Create our own context
    context_ = rclcpp::Context::make_shared();
    rclcpp::InitOptions init_options;
    init_options.auto_initialize_logging( false );
    context_->init( 0, nullptr, init_options );

    rclcpp::ExecutorOptions executor_options;
    executor_options.context = context_;
#if RCLCPP_VERSION_GTE( 30, 1, 1 )
    executor_ = std::make_unique<rclcpp::experimental::executors::EventsExecutor>( executor_options );
#else
    auto queue = std::make_unique<rclcpp::experimental::executors::SimpleEventsQueue>();
    executor_ = std::make_unique<rclcpp::experimental::executors::EventsExecutor>(
        std::move( queue ), false, executor_options );
#endif
    // Start executor spinning thread
    spin_thread_ = std::thread( [this]() { executor_->spin(); } );
  }

  ~Private()
  {
    rclcpp::shutdown( context_, "All gstreamer elements using RosNodeInterface are destroyed." );
    spin_thread_.join();
  }

  rclcpp::Node::SharedPtr getNode( const std::string &node_name )
  {
    auto it = nodes_.find( node_name );
    if ( it != nodes_.end() ) {
      it->second.ref_count++;
      return it->second.node;
    }

    rclcpp::NodeOptions options;
    options.context( context_ );
#if RCLCPP_VERSION_MAJOR >= 28
    options.enable_logger_service( true );
#endif
    auto node = std::make_shared<rclcpp::Node>( node_name, options );
    executor_->add_node( node );
    nodes_[node_name] = { node, 1 };
    return node;
  }

  void releaseNode( const std::string &node_name )
  {
    auto it = nodes_.find( node_name );
    if ( it != nodes_.end() ) {
      if ( --it->second.ref_count == 0 ) {
        executor_->remove_node( it->second.node );
        nodes_.erase( it );
      }
    }
  }

  struct NodeInfo {
    rclcpp::Node::SharedPtr node;
    size_t ref_count;
  };
  std::unordered_map<std::string, NodeInfo> nodes_;
  std::thread spin_thread_;
  rclcpp::Context::SharedPtr context_;
  rclcpp::experimental::executors::EventsExecutor::UniquePtr executor_;
  int ref_count_ = 0;
};

// RosNodeInterface implementation
RosNodeInterface::RosNodeInterface() : owns_node_( false ) { }

RosNodeInterface::~RosNodeInterface() { shutdown(); }

bool RosNodeInterface::initialize( rclcpp::Node::SharedPtr external_node,
                                   const std::string &node_name )
{
  std::lock_guard<std::mutex> lock( priv_mutex_ );
  if ( node_ != nullptr ) {
    return true;
  }

  if ( external_node != nullptr ) {
    // Use the provided external node - no executor needed, caller handles spinning
    node_ = external_node;
    owns_node_ = false;
  } else {
    if ( priv_ == nullptr ) {
      priv_ = std::make_unique<Private>();
    }
    priv_->ref_count_++;

    node_ = priv_->getNode( node_name );
    owns_node_ = true;
  }
  return true;
}

void RosNodeInterface::shutdown()
{
  std::lock_guard<std::mutex> lock( priv_mutex_ );
  if ( !owns_node_ || node_ == nullptr ) {
    return;
  }

  priv_->releaseNode( node_->get_name() );
  node_.reset();
  owns_node_ = false;

  priv_->ref_count_--;
  if ( priv_->ref_count_ == 0 ) {
    priv_.reset();
  }
}

rclcpp::Node::SharedPtr RosNodeInterface::get_node() const { return node_; }

bool RosNodeInterface::is_initialized() const { return node_ != nullptr; }

} // namespace gstreamer_ros_babel_fish
