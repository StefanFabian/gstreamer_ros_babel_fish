// Copyright 2026 Stefan Fabian
// SPDX-License-Identifier: MIT

#include "benchmark_common.hpp"
#include <map>

namespace benchmark
{

class ImageSinkBenchmark : public BenchmarkBase
{
public:
  bool run( const ImageConfig &config, int warmup_frames, int benchmark_frames )
  {
    const std::string topic = "/benchmark_sink";
    const int total_frames = warmup_frames + benchmark_frames;

    std::cout << "\n=== Image Sink Benchmark ===" << std::endl;
    std::cout << "Configuration: " << config.name() << std::endl;
    std::cout << "Frame size: " << config.frame_size_bytes() << " bytes" << std::endl;
    std::cout << "Warmup frames: " << warmup_frames << std::endl;
    std::cout << "Benchmark frames: " << benchmark_frames << std::endl;
    std::cout << std::endl;

    received_count_ = 0;
    warmup_frames_ = warmup_frames;
    metrics_.reset();
    {
      std::lock_guard<std::mutex> lock( sink_times_mutex_ );
      sink_receive_times_by_pts_.clear();
    }

    // Create subscriber to measure receive rate
    auto sub = get_node()->create_subscription<sensor_msgs::msg::Image>(
        topic, rclcpp::QoS( 100 ).reliable(),
        [this, warmup_frames]( sensor_msgs::msg::Image::SharedPtr msg ) {
          auto receive_time = std::chrono::steady_clock::now();
          int count = ++received_count_;

          // Skip warmup frames for metrics
          if ( count > warmup_frames ) {
            metrics_.record_frame( std::chrono::nanoseconds( 0 ), msg->data.size() );

            // Calculate sink latency (buffer received at sink pad -> message received by subscriber)
            int64_t msg_pts = rclcpp::Time( msg->header.stamp ).nanoseconds();
            {
              std::lock_guard<std::mutex> lock( sink_times_mutex_ );
              auto it = sink_receive_times_by_pts_.find( msg_pts );
              if ( it != sink_receive_times_by_pts_.end() ) {
                auto sink_latency =
                    std::chrono::duration_cast<std::chrono::nanoseconds>( receive_time - it->second );
                metrics_.record_sink_latency( sink_latency );
                sink_receive_times_by_pts_.erase( it );
              }
            }
          }

          // Mark start after warmup
          if ( count == warmup_frames + 1 ) {
            metrics_.set_start_time();
          }
        } );

    // Allow time for subscriber discovery
    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    // Build pipeline string
    // Note: RELIABLE QoS has throughput limits due to acknowledgment overhead,
    // so very high framerates may not be achievable
    std::ostringstream pipeline_ss;
    pipeline_ss << "videotestsrc num-buffers=" << total_frames << " ! "
                << "video/x-raw,format=" << config.gst_format << ",width=" << config.width
                << ",height=" << config.height
                << ",framerate=60/1 ! " // 60 fps - reasonable for video streams
                << "rbfimagesink topic=" << topic << " sync=false";

    if ( !create_pipeline( pipeline_ss.str() ) ) {
      std::cerr << "Failed to create pipeline" << std::endl;
      return false;
    }

    // Set up pad probe on rbfimagesink's sink pad for latency measurement
    if ( !setup_sink_pad_probe() ) {
      std::cerr << "Failed to set up sink pad probe" << std::endl;
      return false;
    }

    if ( !start_pipeline() ) {
      std::cerr << "Failed to start pipeline" << std::endl;
      return false;
    }

    bool success = true;
    // Wait for all frames with timeout
    if ( !wait_for_count( received_count_, total_frames, std::chrono::seconds( 60 ) ) ) {
      std::cerr << "Timeout waiting for frames. Received: " << received_count_ << "/"
                << total_frames << std::endl;
      metrics_.set_end_time();
      stop_pipeline();
      success = false;
    } else {
      metrics_.set_end_time();
      // Wait for EOS
      wait_for_eos( std::chrono::seconds( 5 ) );
      stop_pipeline();
    }

    // Print results
    metrics_.print_latency_results( config );

    return success;
  }

private:
  bool setup_sink_pad_probe()
  {
    GstElement *pipeline = get_pipeline();
    if ( !pipeline ) {
      return false;
    }

    // Find the rbfimagesink element
    GstIterator *it = gst_bin_iterate_elements( GST_BIN( pipeline ) );
    GValue item = G_VALUE_INIT;
    GstElement *sink_element = nullptr;

    while ( gst_iterator_next( it, &item ) == GST_ITERATOR_OK ) {
      GstElement *element = GST_ELEMENT( g_value_get_object( &item ) );

      // Get factory name to identify element type
      GstElementFactory *factory = gst_element_get_factory( element );
      if ( factory ) {
        const gchar *factory_name = gst_plugin_feature_get_name( GST_PLUGIN_FEATURE( factory ) );
        if ( g_strcmp0( factory_name, "rbfimagesink" ) == 0 ) {
          sink_element = element;
          g_value_reset( &item );
          break;
        }
      }
      g_value_reset( &item );
    }
    g_value_unset( &item );
    gst_iterator_free( it );

    if ( !sink_element ) {
      std::cerr << "Could not find rbfimagesink element" << std::endl;
      return false;
    }

    // Add probe to rbfimagesink's sink pad (buffer received)
    GstPad *sink_pad = gst_element_get_static_pad( sink_element, "sink" );
    if ( sink_pad ) {
      gst_pad_add_probe( sink_pad, GST_PAD_PROBE_TYPE_BUFFER,
                         (GstPadProbeCallback)sink_pad_probe_callback, this, nullptr );
      gst_object_unref( sink_pad );
    }

    return true;
  }

  static GstPadProbeReturn sink_pad_probe_callback( GstPad *pad, GstPadProbeInfo *info,
                                                    gpointer user_data )
  {
    auto *self = static_cast<ImageSinkBenchmark *>( user_data );
    auto probe_time = std::chrono::steady_clock::now();

    GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER( info );
    if ( buffer && GST_BUFFER_PTS_IS_VALID( buffer ) ) {
      int64_t pts = GST_BUFFER_PTS( buffer );

      std::lock_guard<std::mutex> lock( self->sink_times_mutex_ );
      // Store the time when buffer arrived at sink pad for later correlation
      self->sink_receive_times_by_pts_[pts] = probe_time;
    }

    return GST_PAD_PROBE_OK;
  }

  std::atomic<int> received_count_{ 0 };
  int warmup_frames_ = 0;
  BenchmarkMetrics metrics_;

  // Map for sink time correlation by buffer PTS
  std::map<int64_t, std::chrono::steady_clock::time_point> sink_receive_times_by_pts_;
  mutable std::mutex sink_times_mutex_;
};

} // namespace benchmark

int main( int argc, char **argv )
{
  using namespace benchmark;

  BenchmarkOptions opts = BenchmarkOptions::parse( argc, argv );

  if ( opts.help ) {
    BenchmarkOptions::print_usage( argv[0] );
    return 0;
  }

  ImageSinkBenchmark benchmark;
  if ( !benchmark.initialize( argc, argv ) ) {
    std::cerr << "Failed to initialize benchmark" << std::endl;
    return 1;
  }

  bool success = true;

  if ( opts.all_configs ) {
    // Run with all standard configurations
    auto configs = get_standard_configs();
    for ( const auto &config : configs ) {
      if ( !benchmark.run( config, opts.warmup_frames, opts.frames ) ) {
        success = false;
      }
    }
  } else {
    // Run with specified configuration
    ImageConfig config = get_config_for_format( opts.width, opts.height, opts.format );
    success = benchmark.run( config, opts.warmup_frames, opts.frames );
  }

  return success ? 0 : 1;
}
