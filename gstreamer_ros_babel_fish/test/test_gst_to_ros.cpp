#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/camera_info.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/srv/set_camera_info.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace enc = sensor_msgs::image_encodings;

static void write_camera_info_file( const std::string &path, int width, int height,
                                    const std::string &camera_name, double focal = 1.0,
                                    double cx = 160.0, double cy = 120.0 )
{
  std::ofstream camera_info_file( path );
  camera_info_file << "image_width: " << width << "\n"
                   << "image_height: " << height << "\n"
                   << "camera_name: " << camera_name << "\n"
                   << "camera_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [" << focal << ", 0.0, " << cx << ", 0.0, " << focal << ", " << cy
                   << ", 0.0, 0.0, 1.0]\n"
                   << "distortion_model: plumb_bob\n"
                   << "distortion_coefficients:\n"
                   << "  rows: 1\n"
                   << "  cols: 5\n"
                   << "  data: [0.0, 0.0, 0.0, 0.0, 0.0]\n"
                   << "rectification_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]\n"
                   << "projection_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 4\n"
                   << "  data: [" << focal << ", 0.0, " << cx << ", 0.0, 0.0, " << focal << ", "
                   << cy << ", 0.0, 0.0, 0.0, 1.0, 0.0]\n";
  camera_info_file.close();
  ASSERT_TRUE( camera_info_file );
}

static std::shared_ptr<sensor_msgs::srv::SetCameraInfo::Request>
make_set_camera_info_request( uint32_t width, uint32_t height, double focal, double cx, double cy )
{
  auto request = std::make_shared<sensor_msgs::srv::SetCameraInfo::Request>();
  request->camera_info.width = width;
  request->camera_info.height = height;
  request->camera_info.distortion_model = "plumb_bob";
  request->camera_info.d.resize( 5, 0.0 );
  request->camera_info.k = { focal, 0.0, cx, 0.0, focal, cy, 0.0, 0.0, 1.0 };
  request->camera_info.r = { 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 };
  request->camera_info.p = { focal, 0.0, cx, 0.0, 0.0, focal, cy, 0.0, 0.0, 0.0, 1.0, 0.0 };
  return request;
}

static std::string make_unique_camera_info_path( const std::string &file_name )
{
  return testing::TempDir() +
         std::to_string( std::chrono::steady_clock::now().time_since_epoch().count() ) + "_" +
         file_name;
}

class GstToRosTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "gst_to_ros_test_node" );
    executor_ = std::make_shared<rclcpp::executors::MultiThreadedExecutor>();
    executor_->add_node( node_ );

    // Start spinning in background
    spin_thread_ = std::thread( [this]() { executor_->spin(); } );
  }

  void TearDown() override
  {
    spinning_ = false;
    if ( executor_ ) {
      executor_->cancel();
    }
    if ( spin_thread_.joinable() ) {
      spin_thread_.join();
    }

    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    executor_->remove_node( node_ );
    executor_.reset();
    node_.reset();
  }

  void wait_for_discovery( rclcpp::PublisherBase::SharedPtr pub, int expected_subscribers = 1,
                           int timeout_ms = 10000 )
  {
    auto start = std::chrono::steady_clock::now();
    while ( std::chrono::steady_clock::now() - start < std::chrono::milliseconds( timeout_ms ) ) {
      if ( pub->get_subscription_count() >= (size_t)expected_subscribers )
        return;
      std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
    }
    RCLCPP_WARN( node_->get_logger(), "Discovery timeout for publisher on topic %s",
                 pub->get_topic_name() );
  }

  void wait_for_discovery( rclcpp::SubscriptionBase::SharedPtr sub, int expected_publishers = 1,
                           int timeout_ms = 10000 )
  {
    auto start = std::chrono::steady_clock::now();
    while ( std::chrono::steady_clock::now() - start < std::chrono::milliseconds( timeout_ms ) ) {
      if ( sub->get_publisher_count() >= (size_t)expected_publishers )
        return;
      std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
    }
    RCLCPP_WARN( node_->get_logger(), "Discovery timeout for subscriber on topic %s",
                 sub->get_topic_name() );
  }

  bool wait_for_messages( int count, std::chrono::milliseconds timeout )
  {
    auto start = std::chrono::steady_clock::now();
    while ( received_count_ < count ) {
      if ( std::chrono::steady_clock::now() - start > timeout ) {
        return false;
      }
      std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
    }
    return true;
  }

  void set_node_property( GstElement *pipeline, const char *name )
  {
    GstElement *element = gst_bin_get_by_name( GST_BIN( pipeline ), name );
    if ( element ) {
      g_object_set( element, "node", node_.get(), nullptr );
      gst_object_unref( element );
    }
  }

  rclcpp::Node::SharedPtr node_;
  rclcpp::executors::MultiThreadedExecutor::SharedPtr executor_;
  std::thread spin_thread_;

  std::atomic<bool> spinning_{ true };

  GstElement *pipeline_ = nullptr;
  std::atomic<int> received_count_{ 0 };

  std::mutex last_msg_mutex_;
  sensor_msgs::msg::Image::SharedPtr last_image_msg_;
  sensor_msgs::msg::CompressedImage::SharedPtr last_compressed_msg_;
  sensor_msgs::msg::CameraInfo::SharedPtr last_camera_info_msg_;
  std::atomic<int> received_camera_info_count_{ 0 };
};

TEST_F( GstToRosTest, RawImagePublishing )
{
  const std::string topic = "/test_raw_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=RGB,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for discovery
  wait_for_discovery( sub );

  GstState state;
  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  if ( ret == GST_STATE_CHANGE_ASYNC ) {
    ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  }
  ASSERT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->width, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->height, static_cast<uint32_t>( height ) );
    EXPECT_EQ( last_image_msg_->encoding, enc::RGB8 );
    EXPECT_EQ( last_image_msg_->step, static_cast<uint32_t>( width * 3 ) );
    EXPECT_EQ( last_image_msg_->data.size(), static_cast<size_t>( width * height * 3 ) );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, PublishesCameraInfoWhenUrlMatchesStreamResolution )
{
  const std::string topic = "/test_camera/image_raw";
  const int width = 320;
  const int height = 240;
  const int num_frames = 3;
  const std::string camera_info_path = testing::TempDir() + "rbf_camera_info.yaml";
  const std::string camera_info_url = "file://" + camera_info_path;

  std::ofstream camera_info_file( camera_info_path );
  camera_info_file << "image_width: " << width << "\n"
                   << "image_height: " << height << "\n"
                   << "camera_name: test_camera\n"
                   << "camera_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 1.0, 120.0, 0.0, 0.0, 1.0]\n"
                   << "distortion_model: plumb_bob\n"
                   << "distortion_coefficients:\n"
                   << "  rows: 1\n"
                   << "  cols: 5\n"
                   << "  data: [0.0, 0.0, 0.0, 0.0, 0.0]\n"
                   << "rectification_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]\n"
                   << "projection_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 4\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 0.0, 1.0, 120.0, 0.0, 0.0, 0.0, 1.0, 0.0]\n";
  camera_info_file.close();
  ASSERT_TRUE( camera_info_file );

  auto image_sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/test_camera/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! video/x-raw,format=RGB,width=" + std::to_string( width ) +
                             ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " camera-info-url=" + camera_info_url;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  wait_for_discovery( image_sub );
  wait_for_discovery( camera_info_sub );

  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for image messages. Received: " << received_count_;

  auto start = std::chrono::steady_clock::now();
  while ( received_camera_info_count_ < num_frames &&
          std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }
  ASSERT_GE( received_camera_info_count_.load(), num_frames )
      << "Timeout waiting for camera info messages. Received: " << received_camera_info_count_;

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_EQ( last_camera_info_msg_->width, static_cast<uint32_t>( width ) );
  EXPECT_EQ( last_camera_info_msg_->height, static_cast<uint32_t>( height ) );
  EXPECT_EQ( last_camera_info_msg_->header.frame_id, "" );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->k[0], 1.0 );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->p[2], 160.0 );
}

TEST_F( GstToRosTest, PublishesUpdatedCameraInfoFromTopicScopedService )
{
  const std::string topic = "/test_camera_update/image_raw";
  const int width = 320;
  const int height = 240;
  const int num_frames = 300;
  const std::string camera_info_path = testing::TempDir() + "rbf_camera_info_update.yaml";
  const std::string camera_info_url = "file://" + camera_info_path;

  std::ofstream camera_info_file( camera_info_path );
  camera_info_file << "image_width: " << width + 1 << "\n"
                   << "image_height: " << height << "\n"
                   << "camera_name: test_camera_update\n"
                   << "camera_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 1.0, 120.0, 0.0, 0.0, 1.0]\n"
                   << "distortion_model: plumb_bob\n"
                   << "distortion_coefficients:\n"
                   << "  rows: 1\n"
                   << "  cols: 5\n"
                   << "  data: [0.0, 0.0, 0.0, 0.0, 0.0]\n"
                   << "rectification_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]\n"
                   << "projection_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 4\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 0.0, 1.0, 120.0, 0.0, 0.0, 0.0, 1.0, 0.0]\n";
  camera_info_file.close();
  ASSERT_TRUE( camera_info_file );

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/test_camera_update/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc is-live=true num-buffers=" + std::to_string( num_frames ) +
                             " ! video/x-raw,format=RGB,width=" + std::to_string( width ) +
                             ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " camera-info-url=" + camera_info_url;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  wait_for_discovery( camera_info_sub );

  auto set_camera_info_client = node_->create_client<sensor_msgs::srv::SetCameraInfo>(
      "/test_camera_update/set_camera_info" );
  ASSERT_TRUE( set_camera_info_client->wait_for_service( std::chrono::seconds( 5 ) ) );

  auto request = make_set_camera_info_request( width, height, 2.0, 161.0, 121.0 );

  auto response_future = set_camera_info_client->async_send_request( request );
  ASSERT_EQ( response_future.wait_for( std::chrono::seconds( 5 ) ), std::future_status::ready );
  ASSERT_TRUE( response_future.get()->success );

  auto start = std::chrono::steady_clock::now();
  while ( std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    {
      std::lock_guard<std::mutex> lock( last_msg_mutex_ );
      if ( last_camera_info_msg_ && last_camera_info_msg_->k[0] == 2.0 ) {
        break;
      }
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->k[0], 2.0 );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->p[2], 161.0 );
}

TEST_F( GstToRosTest, ProvidesIndependentSetCameraInfoServicesForMultipleSinks )
{
  const int width = 320;
  const int height = 240;
  const std::string left_path = testing::TempDir() + "rbf_left_camera_info.yaml";
  const std::string right_path = testing::TempDir() + "rbf_right_camera_info.yaml";
  ASSERT_NO_FATAL_FAILURE( write_camera_info_file( left_path, width + 1, height, "left_camera" ) );
  ASSERT_NO_FATAL_FAILURE( write_camera_info_file( right_path, width + 1, height, "right_camera" ) );

  sensor_msgs::msg::CameraInfo::SharedPtr left_info;
  sensor_msgs::msg::CameraInfo::SharedPtr right_info;
  std::mutex camera_info_mutex;

  auto left_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/left/camera_info", 10, [&]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( camera_info_mutex );
        left_info = msg;
      } );
  auto right_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/right/camera_info", 10, [&]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( camera_info_mutex );
        right_info = msg;
      } );

  std::string pipeline_str =
      "videotestsrc is-live=true num-buffers=300 ! "
      "video/x-raw,format=RGB,width=" +
      std::to_string( width ) + ",height=" + std::to_string( height ) +
      ",framerate=30/1 ! tee name=t "
      "t. ! queue ! rbfimagesink name=left_sink topic=/left/image_raw camera-info-url=file://" +
      left_path +
      " "
      "t. ! queue ! rbfimagesink name=right_sink topic=/right/image_raw camera-info-url=file://" +
      right_path;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "left_sink" );
  set_node_property( pipeline_, "right_sink" );

  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  auto left_client =
      node_->create_client<sensor_msgs::srv::SetCameraInfo>( "/left/set_camera_info" );
  auto right_client =
      node_->create_client<sensor_msgs::srv::SetCameraInfo>( "/right/set_camera_info" );
  ASSERT_TRUE( left_client->wait_for_service( std::chrono::seconds( 5 ) ) );
  ASSERT_TRUE( right_client->wait_for_service( std::chrono::seconds( 5 ) ) );
  wait_for_discovery( left_sub );
  wait_for_discovery( right_sub );

  auto left_future = left_client->async_send_request(
      make_set_camera_info_request( width, height, 4.0, 162.0, 122.0 ) );
  auto right_future = right_client->async_send_request(
      make_set_camera_info_request( width, height, 5.0, 163.0, 123.0 ) );
  ASSERT_EQ( left_future.wait_for( std::chrono::seconds( 5 ) ), std::future_status::ready );
  ASSERT_EQ( right_future.wait_for( std::chrono::seconds( 5 ) ), std::future_status::ready );
  ASSERT_TRUE( left_future.get()->success );
  ASSERT_TRUE( right_future.get()->success );

  auto start = std::chrono::steady_clock::now();
  while ( std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    {
      std::lock_guard<std::mutex> lock( camera_info_mutex );
      if ( left_info && right_info && left_info->k[0] == 4.0 && right_info->k[0] == 5.0 ) {
        break;
      }
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  std::lock_guard<std::mutex> lock( camera_info_mutex );
  ASSERT_NE( left_info, nullptr );
  ASSERT_NE( right_info, nullptr );
  EXPECT_DOUBLE_EQ( left_info->k[0], 4.0 );
  EXPECT_DOUBLE_EQ( right_info->k[0], 5.0 );
}

TEST_F( GstToRosTest, PublishesCameraInfoFromPackageUrl )
{
  const std::string topic = "/package_camera/image_raw";
  const int width = 320;
  const int height = 240;
  const int num_frames = 3;
  const std::string camera_info_url =
      "package://gstreamer_ros_babel_fish/test/calibrations/package_camera_info.yaml";

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/package_camera/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! video/x-raw,format=RGB,width=" + std::to_string( width ) +
                             ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " camera-info-url=" + camera_info_url;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";
  wait_for_discovery( camera_info_sub );

  auto start = std::chrono::steady_clock::now();
  while ( received_camera_info_count_ < num_frames &&
          std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->k[0], 3.0 );
}

TEST_F( GstToRosTest, SetCameraInfoUpdatesMemoryWhenSavingFails )
{
  const std::string topic = "/readonly_camera/image_raw";
  const int width = 320;
  const int height = 240;
  const std::string camera_info_path =
      make_unique_camera_info_path( "rbf_readonly_camera_info.yaml" );
  ASSERT_NO_FATAL_FAILURE(
      write_camera_info_file( camera_info_path, width, height, "readonly_camera" ) );

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/readonly_camera/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc is-live=true num-buffers=300 "
                             "! video/x-raw,format=RGB,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! rbfimagesink name=sink topic=" + topic +
                             " camera-info-url=file://" + camera_info_path;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  auto client =
      node_->create_client<sensor_msgs::srv::SetCameraInfo>( "/readonly_camera/set_camera_info" );
  ASSERT_TRUE( client->wait_for_service( std::chrono::seconds( 5 ) ) );
  // Turn file into a directory to force save failure
  ASSERT_TRUE( std::filesystem::remove( camera_info_path ) );
  ASSERT_TRUE( std::filesystem::create_directory( camera_info_path ) );

  auto response_future =
      client->async_send_request( make_set_camera_info_request( width, height, 6.0, 164.0, 124.0 ) );
  ASSERT_EQ( response_future.wait_for( std::chrono::seconds( 5 ) ), std::future_status::ready );
  auto response = response_future.get();
  ASSERT_TRUE( response->success );
  EXPECT_NE( response->status_message.find( "saving calibration file failed" ), std::string::npos )
      << "Unexpected status message: " << response->status_message;

  auto start = std::chrono::steady_clock::now();
  while ( std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    {
      std::lock_guard<std::mutex> lock( last_msg_mutex_ );
      if ( last_camera_info_msg_ && last_camera_info_msg_->k[0] == 6.0 ) {
        break;
      }
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->k[0], 6.0 );
}

TEST_F( GstToRosTest, PreservesInMemoryCameraInfoAcrossCapsRenegotiation )
{
  const std::string topic = "/renegotiated_camera/image_raw";
  const int width = 320;
  const int height = 240;
  const std::string camera_info_path =
      make_unique_camera_info_path( "rbf_renegotiated_camera_info.yaml" );
  ASSERT_NO_FATAL_FAILURE(
      write_camera_info_file( camera_info_path, width, height, "renegotiated_camera" ) );

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/renegotiated_camera/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str =
      "videotestsrc is-live=true num-buffers=300 ! "
      "capsfilter name=filter caps=video/x-raw,format=RGB,width=320,height=240,framerate=30/1 ! "
      "rbfimagesink name=sink topic=" +
      topic + " camera-info-url=file://" + camera_info_path;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  auto client = node_->create_client<sensor_msgs::srv::SetCameraInfo>(
      "/renegotiated_camera/set_camera_info" );
  ASSERT_TRUE( client->wait_for_service( std::chrono::seconds( 5 ) ) );
  // Turn file into a directory to force save failure
  ASSERT_TRUE( std::filesystem::remove( camera_info_path ) );
  ASSERT_TRUE( std::filesystem::create_directory( camera_info_path ) );

  auto response_future =
      client->async_send_request( make_set_camera_info_request( width, height, 7.0, 165.0, 125.0 ) );
  ASSERT_EQ( response_future.wait_for( std::chrono::seconds( 5 ) ), std::future_status::ready );
  auto response = response_future.get();
  ASSERT_TRUE( response->success );
  EXPECT_NE( response->status_message.find( "saving calibration file failed" ), std::string::npos )
      << "Unexpected status message: " << response->status_message;

  GstElement *filter = gst_bin_get_by_name( GST_BIN( pipeline_ ), "filter" );
  ASSERT_NE( filter, nullptr );
  GstCaps *renegotiated_caps =
      gst_caps_from_string( "video/x-raw,format=RGB,width=320,height=240,framerate=15/1" );
  ASSERT_NE( renegotiated_caps, nullptr );
  g_object_set( filter, "caps", renegotiated_caps, nullptr );
  gst_caps_unref( renegotiated_caps );
  gst_object_unref( filter );

  auto start = std::chrono::steady_clock::now();
  while ( std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    {
      std::lock_guard<std::mutex> lock( last_msg_mutex_ );
      if ( last_camera_info_msg_ && last_camera_info_msg_->k[0] == 7.0 ) {
        break;
      }
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->k[0], 7.0 );
  EXPECT_DOUBLE_EQ( last_camera_info_msg_->p[2], 165.0 );
}

TEST_F( GstToRosTest, UnsupportedCameraInfoUrlDisablesCameraInfoOnly )
{
  const std::string topic = "/bad_camera_url/image_raw";
  const int num_frames = 3;

  auto image_sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );
  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/bad_camera_url/camera_info", 10, [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! video/x-raw,format=RGB,width=320,height=240,framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " camera-info-url=http://example.com/camera.yaml";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";
  wait_for_discovery( image_sub );

  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) );
  EXPECT_EQ( camera_info_sub->get_publisher_count(), 0u );
  EXPECT_EQ( received_camera_info_count_.load(), 0 );
}

TEST_F( GstToRosTest, CompressedJpegPublishing )
{
  const std::string topic = "/test_compressed_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber on /compressed subtopic
  auto sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      topic + "/compressed", 10, [this]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_compressed_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "videoconvert ! jpegenc ! "
                             "rbfimagesink name=sink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for discovery
  wait_for_discovery( sub );

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_compressed_msg_, nullptr );
    EXPECT_EQ( last_compressed_msg_->format, "jpeg" );
    EXPECT_FALSE( last_compressed_msg_->data.empty() );

    // Verify JPEG magic bytes
    ASSERT_GE( last_compressed_msg_->data.size(), 2u );
    EXPECT_EQ( last_compressed_msg_->data[0], 0xFF );
    EXPECT_EQ( last_compressed_msg_->data[1], 0xD8 );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, PublishesCameraInfoForCompressedJpeg )
{
  const std::string topic = "/test_compressed_camera/image_raw";
  const int width = 320;
  const int height = 240;
  const int num_frames = 3;
  const std::string camera_info_path = testing::TempDir() + "rbf_compressed_camera_info.yaml";
  const std::string camera_info_url = "file://" + camera_info_path;

  std::ofstream camera_info_file( camera_info_path );
  camera_info_file << "image_width: " << width << "\n"
                   << "image_height: " << height << "\n"
                   << "camera_name: test_compressed_camera\n"
                   << "camera_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 1.0, 120.0, 0.0, 0.0, 1.0]\n"
                   << "distortion_model: plumb_bob\n"
                   << "distortion_coefficients:\n"
                   << "  rows: 1\n"
                   << "  cols: 5\n"
                   << "  data: [0.0, 0.0, 0.0, 0.0, 0.0]\n"
                   << "rectification_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 3\n"
                   << "  data: [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]\n"
                   << "projection_matrix:\n"
                   << "  rows: 3\n"
                   << "  cols: 4\n"
                   << "  data: [1.0, 0.0, 160.0, 0.0, 0.0, 1.0, 120.0, 0.0, 0.0, 0.0, 1.0, 0.0]\n";
  camera_info_file.close();
  ASSERT_TRUE( camera_info_file );

  auto compressed_sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      topic + "/compressed", 10, [this]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_compressed_msg_ = msg;
        received_count_++;
      } );

  auto camera_info_sub = node_->create_subscription<sensor_msgs::msg::CameraInfo>(
      "/test_compressed_camera/camera_info", 10,
      [this]( sensor_msgs::msg::CameraInfo::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_camera_info_msg_ = msg;
        received_camera_info_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! video/x-raw,width=" + std::to_string( width ) +
                             ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "videoconvert ! jpegenc ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " camera-info-url=" + camera_info_url;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  wait_for_discovery( compressed_sub );
  wait_for_discovery( camera_info_sub );

  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for compressed messages. Received: " << received_count_;

  auto start = std::chrono::steady_clock::now();
  while ( received_camera_info_count_ < num_frames &&
          std::chrono::steady_clock::now() - start < std::chrono::seconds( 5 ) ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }
  ASSERT_GE( received_camera_info_count_.load(), num_frames )
      << "Timeout waiting for camera info messages. Received: " << received_camera_info_count_;

  std::lock_guard<std::mutex> lock( last_msg_mutex_ );
  ASSERT_NE( last_camera_info_msg_, nullptr );
  EXPECT_EQ( last_camera_info_msg_->width, static_cast<uint32_t>( width ) );
  EXPECT_EQ( last_camera_info_msg_->height, static_cast<uint32_t>( height ) );
}

TEST_F( GstToRosTest, CompressedPngPublishing )
{
  const std::string topic = "/test_png_image";
  const int width = 160;
  const int height = 120;
  const int num_frames = 3; // PNG encoding is slower

  // Create subscriber on /compressed subtopic
  auto sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      topic + "/compressed", 10, [this]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_compressed_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=10/1 ! "
                             "videoconvert ! pngenc ! "
                             "rbfimagesink name=sink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for discovery
  wait_for_discovery( sub );

  // Wait for messages (longer timeout for PNG)
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 10 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_compressed_msg_, nullptr );
    EXPECT_EQ( last_compressed_msg_->format, "png" );
    EXPECT_FALSE( last_compressed_msg_->data.empty() );

    // Verify PNG magic bytes
    ASSERT_GE( last_compressed_msg_->data.size(), 8u );
    EXPECT_EQ( last_compressed_msg_->data[0], 0x89 );
    EXPECT_EQ( last_compressed_msg_->data[1], 'P' );
    EXPECT_EQ( last_compressed_msg_->data[2], 'N' );
    EXPECT_EQ( last_compressed_msg_->data[3], 'G' );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 10, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, MonochromeImagePublishing )
{
  const std::string topic = "/test_mono_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=GRAY8,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  set_node_property( pipeline_, "sink" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for discovery
  wait_for_discovery( sub );

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->width, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->height, static_cast<uint32_t>( height ) );
    EXPECT_EQ( last_image_msg_->encoding, enc::MONO8 );
    EXPECT_EQ( last_image_msg_->step, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->data.size(), static_cast<size_t>( width * height ) );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, FrameIdProperty )
{
  const std::string topic = "/test_frame_id";
  const std::string frame_id = "camera_optical_frame";
  const int num_frames = 2;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=RGB,width=320,height=240,framerate=30/1 ! "
                             "rbfimagesink name=sink topic=" +
                             topic + " frame-id=" + frame_id;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  set_node_property( pipeline_, "sink" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for discovery
  wait_for_discovery( sub );

  GstState state;
  gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 10 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->header.frame_id, frame_id );
  }

  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );
  if ( msg ) {
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  rclcpp::init( argc, argv );
  int ret = RUN_ALL_TESTS();
  rclcpp::shutdown();
  return ret;
}
