// Copyright 2026 Stefan Fabian
// SPDX-License-Identifier: MIT

#ifndef GSTREAMER_ROS_BABEL_FISH__BENCHMARK_COMMON_HPP_
#define GSTREAMER_ROS_BABEL_FISH__BENCHMARK_COMMON_HPP_

#include <gst/gst.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace benchmark
{

namespace enc = sensor_msgs::image_encodings;

// Benchmark configuration constants
constexpr int DEFAULT_WARMUP_FRAMES = 50;
constexpr int DEFAULT_BENCHMARK_FRAMES = 1000;
constexpr int DEFAULT_WIDTH = 640;
constexpr int DEFAULT_HEIGHT = 480;

// Image configuration struct
struct ImageConfig {
  int width;
  int height;
  std::string gst_format;   // GStreamer format name (RGB, BGR, GRAY8, etc.)
  std::string ros_encoding; // ROS encoding string
  int bytes_per_pixel;

  std::string name() const
  {
    std::ostringstream ss;
    ss << width << "x" << height << "_" << gst_format;
    return ss.str();
  }

  size_t frame_size_bytes() const
  {
    return static_cast<size_t>( width ) * height * bytes_per_pixel;
  }
};

// Predefined standard configurations for benchmarking
inline std::vector<ImageConfig> get_standard_configs()
{
  return {
      // VGA
      { 640, 480, "RGB", enc::RGB8, 3 },
      { 640, 480, "GRAY8", enc::MONO8, 1 },
      // HD 720p
      { 1280, 720, "RGB", enc::RGB8, 3 },
      { 1280, 720, "GRAY8", enc::MONO8, 1 },
      // Full HD 1080p
      { 1920, 1080, "RGB", enc::RGB8, 3 },
      { 1920, 1080, "GRAY8", enc::MONO8, 1 },
  };
}

// Get ImageConfig from format string
inline ImageConfig get_config_for_format( int width, int height, const std::string &format )
{
  if ( format == "RGB" || format == "rgb8" ) {
    return { width, height, "RGB", enc::RGB8, 3 };
  } else if ( format == "BGR" || format == "bgr8" ) {
    return { width, height, "BGR", enc::BGR8, 3 };
  } else if ( format == "RGBA" || format == "rgba8" ) {
    return { width, height, "RGBA", enc::RGBA8, 4 };
  } else if ( format == "BGRA" || format == "bgra8" ) {
    return { width, height, "BGRA", enc::BGRA8, 4 };
  } else if ( format == "GRAY8" || format == "mono8" ) {
    return { width, height, "GRAY8", enc::MONO8, 1 };
  } else if ( format == "GRAY16_LE" || format == "mono16" ) {
    return { width, height, "GRAY16_LE", enc::MONO16, 2 };
  }
  // Default to RGB
  return { width, height, "RGB", enc::RGB8, 3 };
}

// Metrics collection class
class BenchmarkMetrics
{
public:
  void record_frame( std::chrono::nanoseconds latency, size_t bytes )
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( latency.count() > 0 )
      latencies_ns_.push_back( latency.count() );
    bytes_transferred_.push_back( bytes );
  }

  void record_latency( std::chrono::nanoseconds latency )
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    latencies_ns_.push_back( latency.count() );
  }

  void record_source_latency( std::chrono::nanoseconds latency )
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    source_latencies_ns_.push_back( latency.count() );
  }

  void record_sink_latency( std::chrono::nanoseconds latency )
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    sink_latencies_ns_.push_back( latency.count() );
  }

  void set_start_time() { start_time_ = std::chrono::steady_clock::now(); }

  void set_end_time() { end_time_ = std::chrono::steady_clock::now(); }

  size_t frame_count() const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    return bytes_transferred_.size();
  }

  double duration_seconds() const
  {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>( end_time_ - start_time_ );
    return duration.count() / 1000.0;
  }

  double frames_per_second() const
  {
    double duration = duration_seconds();
    if ( duration == 0.0 )
      return 0.0;
    return frame_count() / duration;
  }

  double latency_min_ms() const { return compute_min( latencies_ns_ ); }
  double latency_max_ms() const { return compute_max( latencies_ns_ ); }
  double latency_avg_ms() const { return compute_avg( latencies_ns_ ); }
  double latency_stddev_ms() const { return compute_stddev( latencies_ns_ ); }
  double latency_p95_ms() const { return compute_p95( latencies_ns_ ); }

  // Source latency statistics (publish -> buffer pushed)
  double source_latency_min_ms() const { return compute_min( source_latencies_ns_ ); }
  double source_latency_max_ms() const { return compute_max( source_latencies_ns_ ); }
  double source_latency_avg_ms() const { return compute_avg( source_latencies_ns_ ); }
  double source_latency_stddev_ms() const { return compute_stddev( source_latencies_ns_ ); }
  double source_latency_p95_ms() const { return compute_p95( source_latencies_ns_ ); }

  // Sink latency statistics (buffer received -> message published)
  double sink_latency_min_ms() const { return compute_min( sink_latencies_ns_ ); }
  double sink_latency_max_ms() const { return compute_max( sink_latencies_ns_ ); }
  double sink_latency_avg_ms() const { return compute_avg( sink_latencies_ns_ ); }
  double sink_latency_stddev_ms() const { return compute_stddev( sink_latencies_ns_ ); }
  double sink_latency_p95_ms() const { return compute_p95( sink_latencies_ns_ ); }

  double throughput_mbps() const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    double duration = duration_seconds();
    if ( duration == 0.0 || bytes_transferred_.empty() )
      return 0.0;
    size_t total_bytes =
        std::accumulate( bytes_transferred_.begin(), bytes_transferred_.end(), size_t( 0 ) );
    return ( total_bytes / ( 1024.0 * 1024.0 ) ) / duration;
  }

  void print_throughput_results( const ImageConfig &config ) const
  {
    std::cout << std::fixed << std::setprecision( 2 );
    std::cout << "Results:\n";
    std::cout << "  Frames processed: " << frame_count() << "\n";
    std::cout << "  Duration: " << duration_seconds() << " s\n";
    std::cout << "  FPS: " << frames_per_second() << "\n";
    std::cout << "  Throughput: " << throughput_mbps() << " MB/s\n";
  }

  void print_latency_results( const ImageConfig &config ) const
  {
    std::cout << std::fixed << std::setprecision( 2 );
    std::cout << "Results:\n";
    std::cout << "  Frames processed: " << frame_count() << "\n";
    std::cout << "  Duration: " << duration_seconds() << " s\n";
    std::cout << "  FPS: " << frames_per_second() << "\n";
    std::cout << "  Throughput: " << throughput_mbps() << " MB/s\n";
    if ( !latencies_ns_.empty() ) {
      std::cout << "  Total Latency (ms):\n";
      std::cout << "    min  \tmax \tavg \tstddev\tp95\n";
      std::cout << "    " << latency_min_ms() << "\t" << latency_max_ms() << "\t" << latency_avg_ms()
                << "\t" << latency_stddev_ms() << "\t" << latency_p95_ms() << "\n";
    }
    if ( !source_latencies_ns_.empty() ) {
      std::cout << "  Source Latency (publish -> buffer pushed) (ms):\n";
      std::cout << "    min  \tmax \tavg \tstddev\tp95\n";
      std::cout << "    " << source_latency_min_ms() << "\t" << source_latency_max_ms() << "\t"
                << source_latency_avg_ms() << "\t" << source_latency_stddev_ms() << "\t"
                << source_latency_p95_ms() << "\n";
    }
    if ( !sink_latencies_ns_.empty() ) {
      std::cout << "  Sink Latency (buffer received -> message published) (ms):\n";
      std::cout << "    min  \tmax \tavg \tstddev\tp95\n";
      std::cout << "    " << sink_latency_min_ms() << "\t" << sink_latency_max_ms() << "\t"
                << sink_latency_avg_ms() << "\t" << sink_latency_stddev_ms() << "\t"
                << sink_latency_p95_ms() << "\n";
    }
  }

  void reset()
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    latencies_ns_.clear();
    bytes_transferred_.clear();
    source_latencies_ns_.clear();
    sink_latencies_ns_.clear();
  }

private:
  // Helper functions for computing statistics on any latency vector
  double compute_min( const std::vector<int64_t> &data ) const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( data.empty() )
      return 0.0;
    return *std::min_element( data.begin(), data.end() ) / 1e6;
  }

  double compute_max( const std::vector<int64_t> &data ) const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( data.empty() )
      return 0.0;
    return *std::max_element( data.begin(), data.end() ) / 1e6;
  }

  double compute_avg( const std::vector<int64_t> &data ) const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( data.empty() )
      return 0.0;
    double sum = std::accumulate( data.begin(), data.end(), 0.0 );
    return ( sum / data.size() ) / 1e6;
  }

  double compute_stddev( const std::vector<int64_t> &data ) const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( data.size() < 2 )
      return 0.0;
    double sum = std::accumulate( data.begin(), data.end(), 0.0 );
    double mean_ns = sum / data.size();
    double sq_sum = 0.0;
    for ( auto l : data ) { sq_sum += ( l - mean_ns ) * ( l - mean_ns ); }
    return std::sqrt( sq_sum / ( data.size() - 1 ) ) / 1e6;
  }

  double compute_p95( const std::vector<int64_t> &data ) const
  {
    std::lock_guard<std::mutex> lock( mutex_ );
    if ( data.empty() )
      return 0.0;
    std::vector<int64_t> sorted = data;
    std::sort( sorted.begin(), sorted.end() );
    size_t idx = static_cast<size_t>( sorted.size() * 0.95 );
    return sorted[std::min( idx, sorted.size() - 1 )] / 1e6;
  }

  mutable std::mutex mutex_;
  std::vector<int64_t> latencies_ns_;
  std::vector<size_t> bytes_transferred_;
  std::vector<int64_t> source_latencies_ns_;
  std::vector<int64_t> sink_latencies_ns_;
  std::chrono::steady_clock::time_point start_time_;
  std::chrono::steady_clock::time_point end_time_;
};

// Base class for benchmark executables
class BenchmarkBase
{
public:
  BenchmarkBase() = default;
  virtual ~BenchmarkBase() { shutdown(); }

  bool initialize( int argc, char **argv )
  {
    // Initialize GStreamer
    gst_init( &argc, &argv );

    // Initialize ROS
    if ( !rclcpp::ok() ) {
      rclcpp::init( argc, argv );
    }

    node_ = std::make_shared<rclcpp::Node>( "benchmark_node" );
    executor_ = std::make_shared<rclcpp::experimental::executors::EventsExecutor>();
    executor_->add_node( node_ );

    // Start background spinner - use tight loop for responsiveness
    spinning_ = true;
    spin_thread_ = std::thread( [this]() { executor_->spin(); } );

    return true;
  }

  void shutdown()
  {
    // First stop the pipeline to ensure GStreamer elements are not blocking
    if ( pipeline_ ) {
      // Send EOS and wait briefly
      gst_element_send_event( pipeline_, gst_event_new_eos() );
      GstBus *bus = gst_element_get_bus( pipeline_ );
      if ( bus ) {
        gst_bus_timed_pop_filtered( bus, 100 * GST_MSECOND,
                                    static_cast<GstMessageType>( GST_MESSAGE_EOS ) );
        gst_object_unref( bus );
      }

      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    // Stop the ROS executor
    spinning_ = false;
    if ( executor_ ) {
      executor_->cancel();
      rclcpp::shutdown();
    }

    if ( spin_thread_.joinable() ) {
      spin_thread_.join();
    }

    if ( executor_ && node_ ) {
      executor_->remove_node( node_ );
    }
    executor_.reset();
    node_.reset();
  }

  // Create a test image message
  sensor_msgs::msg::Image::SharedPtr create_test_image( const ImageConfig &config )
  {
    auto msg = std::make_shared<sensor_msgs::msg::Image>();
    msg->header.stamp = node_->get_clock()->now();
    msg->header.frame_id = "benchmark_frame";
    msg->width = config.width;
    msg->height = config.height;
    msg->encoding = config.ros_encoding;
    msg->step = config.width * config.bytes_per_pixel;
    msg->data.resize( config.frame_size_bytes() );

    // Fill with deterministic test pattern
    for ( size_t i = 0; i < msg->data.size(); i++ ) {
      msg->data[i] = static_cast<uint8_t>( ( i * 17 + 31 ) % 256 );
    }

    return msg;
  }

  // Create pipeline from string
  bool create_pipeline( const std::string &pipeline_str )
  {
    GError *error = nullptr;
    pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
    if ( !pipeline_ ) {
      if ( error ) {
        std::cerr << "Pipeline error: " << error->message << std::endl;
        g_error_free( error );
      }
      return false;
    }
    return true;
  }

  // Start pipeline
  bool start_pipeline()
  {
    GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
    return ret != GST_STATE_CHANGE_FAILURE;
  }

  // Stop pipeline
  void stop_pipeline()
  {
    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
    }
  }

  // Wait for EOS or error
  bool wait_for_eos( std::chrono::seconds timeout )
  {
    GstBus *bus = gst_element_get_bus( pipeline_ );
    GstMessage *msg = gst_bus_timed_pop_filtered(
        bus, timeout.count() * GST_SECOND,
        static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

    bool success = false;
    if ( msg ) {
      success = ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_EOS );
      if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
        GError *err;
        gchar *debug;
        gst_message_parse_error( msg, &err, &debug );
        std::cerr << "Pipeline error: " << err->message << std::endl;
        g_error_free( err );
        g_free( debug );
      }
      gst_message_unref( msg );
    }
    gst_object_unref( bus );
    return success;
  }

  // Wait for atomic counter to reach target
  bool wait_for_count( std::atomic<int> &counter, int target, std::chrono::seconds timeout )
  {
    auto start = std::chrono::steady_clock::now();
    while ( counter < target ) {
      if ( std::chrono::steady_clock::now() - start > timeout || !rclcpp::ok() ) {
        return false;
      }
      std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
    }
    return true;
  }

  rclcpp::Node::SharedPtr get_node() { return node_; }

  GstElement *get_pipeline() { return pipeline_; }

protected:
  rclcpp::Node::SharedPtr node_;
  rclcpp::experimental::executors::EventsExecutor::SharedPtr executor_;
  std::thread spin_thread_;
  std::atomic<bool> spinning_{ false };
  GstElement *pipeline_ = nullptr;
};

// Command line argument parsing
struct BenchmarkOptions {
  int width = DEFAULT_WIDTH;
  int height = DEFAULT_HEIGHT;
  std::string format = "RGB";
  int frames = DEFAULT_BENCHMARK_FRAMES;
  int warmup_frames = DEFAULT_WARMUP_FRAMES;
  bool all_configs = false;
  bool help = false;

  static void print_usage( const char *program_name )
  {
    std::cout << "Usage: " << program_name << " [options]\n\n";
    std::cout << "Options:\n";
    std::cout << "  -w, --width N       Image width (default: " << DEFAULT_WIDTH << ")\n";
    std::cout << "  -h, --height N      Image height (default: " << DEFAULT_HEIGHT << ")\n";
    std::cout << "  -f, --format FMT    Image format: RGB, BGR, RGBA, BGRA, GRAY8 (default: RGB)\n";
    std::cout << "  -n, --frames N      Number of benchmark frames (default: "
              << DEFAULT_BENCHMARK_FRAMES << ")\n";
    std::cout << "  --warmup N          Number of warmup frames (default: " << DEFAULT_WARMUP_FRAMES
              << ")\n";
    std::cout << "  --all-configs       Run benchmark with all standard configurations\n";
    std::cout << "  --help              Show this help message\n";
  }

  static BenchmarkOptions parse( int argc, char **argv )
  {
    BenchmarkOptions opts;

    for ( int i = 1; i < argc; i++ ) {
      std::string arg = argv[i];

      if ( arg == "--help" ) {
        opts.help = true;
        return opts;
      } else if ( arg == "--all-configs" ) {
        opts.all_configs = true;
      } else if ( ( arg == "-w" || arg == "--width" ) && i + 1 < argc ) {
        opts.width = std::stoi( argv[++i] );
      } else if ( ( arg == "-h" || arg == "--height" ) && i + 1 < argc ) {
        opts.height = std::stoi( argv[++i] );
      } else if ( ( arg == "-f" || arg == "--format" ) && i + 1 < argc ) {
        opts.format = argv[++i];
      } else if ( ( arg == "-n" || arg == "--frames" ) && i + 1 < argc ) {
        opts.frames = std::stoi( argv[++i] );
      } else if ( arg == "--warmup" && i + 1 < argc ) {
        opts.warmup_frames = std::stoi( argv[++i] );
      }
    }

    return opts;
  }
};

} // namespace benchmark

#endif // GSTREAMER_ROS_BABEL_FISH__BENCHMARK_COMMON_HPP_
