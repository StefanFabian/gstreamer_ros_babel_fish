// Copyright 2026 Stefan Fabian
// SPDX-License-Identifier: MIT

#include "benchmark_common.hpp"
#include <map>

namespace benchmark
{

class ImageSrcBenchmark : public BenchmarkBase
{
public:
  bool run( const ImageConfig &config, int warmup_frames, int benchmark_frames )
  {
    const std::string topic = "/benchmark_src";
    const int total_frames = warmup_frames + benchmark_frames;

    std::cout << "\n=== Image Source Benchmark ===" << std::endl;
    std::cout << "Configuration: " << config.name() << std::endl;
    std::cout << "Frame size: " << config.frame_size_bytes() << " bytes" << std::endl;
    std::cout << "Warmup frames: " << warmup_frames << std::endl;
    std::cout << "Benchmark frames: " << benchmark_frames << std::endl;
    std::cout << std::endl;

    // Reset state
    buffer_count_ = 0;
    warmup_frames_ = warmup_frames;
    metrics_.reset();
    {
      std::lock_guard<std::mutex> lock( publish_times_mutex_ );
      publish_times_by_pts_.clear();
    }

    // Create publisher
    auto pub =
        get_node()->create_publisher<sensor_msgs::msg::Image>( topic, rclcpp::QoS( 100 ).reliable() );

    // Allow time for publisher discovery
    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    // Create pipeline
    std::ostringstream pipeline_ss;
    pipeline_ss << "rbfimagesrc topic=" << topic << " qos-reliability=reliable"
                << " ! fakesink name=sink sync=false";

    if ( !create_pipeline( pipeline_ss.str() ) ) {
      std::cerr << "Failed to create pipeline" << std::endl;
      return false;
    }

    // Add probe to count buffers
    GstElement *sink = gst_bin_get_by_name( GST_BIN( get_pipeline() ), "sink" );
    if ( !sink ) {
      std::cerr << "Failed to get sink element" << std::endl;
      return false;
    }

    GstPad *pad = gst_element_get_static_pad( sink, "sink" );
    if ( !pad ) {
      std::cerr << "Failed to get sink pad" << std::endl;
      gst_object_unref( sink );
      return false;
    }

    gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );

    gst_object_unref( pad );
    gst_object_unref( sink );

    if ( !start_pipeline() ) {
      std::cerr << "Failed to start pipeline" << std::endl;
      return false;
    }

    // Wait for pipeline to be ready
    std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

    // Publish frames
    auto test_image = create_test_image( config );

    for ( int i = 0; i < total_frames; i++ ) {
      test_image->header.stamp = get_node()->get_clock()->now();
      auto publish_time = std::chrono::steady_clock::now();
      int64_t msg_pts = rclcpp::Time( test_image->header.stamp ).nanoseconds();

      // Store publish time by PTS for source latency correlation
      {
        std::lock_guard<std::mutex> lock( publish_times_mutex_ );
        publish_times_by_pts_[msg_pts] = publish_time;
      }

      pub->publish( *test_image );

      // Add delay between frames to allow pipeline to process
      // The rbfimagesrc has a queue limit of 5 and uses 100ms timeouts,
      // so we need to pace publishing to avoid dropping frames
      std::this_thread::sleep_for( std::chrono::milliseconds( 5 ) );
    }

    bool success = true;
    // Wait for all buffers to be processed
    if ( !wait_for_count( buffer_count_, total_frames, std::chrono::seconds( 60 ) ) ) {
      std::cerr << "Timeout waiting for buffers. Received: " << buffer_count_ << "/" << total_frames
                << std::endl;
      metrics_.set_end_time();
      stop_pipeline();
      success = false;
    } else {
      metrics_.set_end_time();
      stop_pipeline();
    }

    // Print results
    metrics_.print_latency_results( config );

    return success;
  }

private:
  static GstPadProbeReturn buffer_probe_callback( GstPad * /*pad*/, GstPadProbeInfo *info,
                                                  gpointer user_data )
  {
    auto *self = static_cast<ImageSrcBenchmark *>( user_data );
    auto probe_time = std::chrono::steady_clock::now();

    if ( GST_PAD_PROBE_INFO_TYPE( info ) & GST_PAD_PROBE_TYPE_BUFFER ) {
      GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER( info );
      if ( buffer ) {
        int count = ++self->buffer_count_;

        // Skip warmup frames for metrics
        if ( count > self->warmup_frames_ ) {
          GstMapInfo map;
          if ( gst_buffer_map( buffer, &map, GST_MAP_READ ) ) {
            self->metrics_.record_frame( std::chrono::nanoseconds( 0 ), map.size );
            gst_buffer_unmap( buffer, &map );
          }

          // Calculate source latency (publish time -> buffer received)
          if ( GST_BUFFER_PTS_IS_VALID( buffer ) ) {
            int64_t pts = GST_BUFFER_PTS( buffer );
            std::lock_guard<std::mutex> lock( self->publish_times_mutex_ );
            auto it = self->publish_times_by_pts_.find( pts );
            if ( it != self->publish_times_by_pts_.end() ) {
              auto source_latency =
                  std::chrono::duration_cast<std::chrono::nanoseconds>( probe_time - it->second );
              self->metrics_.record_source_latency( source_latency );
              self->publish_times_by_pts_.erase( it );
            }
          }
        }

        // Mark start after warmup
        if ( count == self->warmup_frames_ + 1 ) {
          self->metrics_.set_start_time();
        }
      }
    }

    return GST_PAD_PROBE_OK;
  }

  std::atomic<int> buffer_count_{ 0 };
  int warmup_frames_ = 0;
  BenchmarkMetrics metrics_;

  // Map for publish time correlation by buffer PTS
  std::map<int64_t, std::chrono::steady_clock::time_point> publish_times_by_pts_;
  mutable std::mutex publish_times_mutex_;
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

  ImageSrcBenchmark benchmark;
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
