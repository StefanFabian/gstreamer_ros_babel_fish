// Copyright 2026 Stefan Fabian
// SPDX-License-Identifier: MIT

#include "benchmark_common.hpp"
#include <map>
#include <queue>

namespace benchmark
{

class FullPipelineBenchmark : public BenchmarkBase
{
public:
  bool run( const ImageConfig &config, int warmup_frames, int benchmark_frames )
  {
    const std::string input_topic = "/benchmark_pipeline_in";
    const std::string output_topic = "/benchmark_pipeline_out";
    const int total_frames = warmup_frames + benchmark_frames;

    std::cout << "\n=== Full Pipeline Benchmark ===" << std::endl;
    std::cout << "Configuration: " << config.name() << std::endl;
    std::cout << "Frame size: " << config.frame_size_bytes() << " bytes" << std::endl;
    std::cout << "Warmup frames: " << warmup_frames << std::endl;
    std::cout << "Benchmark frames: " << benchmark_frames << std::endl;
    std::cout << std::endl;

    // Reset state
    received_count_ = 0;
    warmup_frames_ = warmup_frames;
    metrics_.reset();
    {
      std::lock_guard<std::mutex> lock( publish_times_mutex_ );
      while ( !publish_times_.empty() ) publish_times_.pop();
    }
    {
      std::lock_guard<std::mutex> lock( latency_data_mutex_ );
      publish_times_by_pts_.clear();
      sink_receive_times_by_pts_.clear();
    }

    // Create publisher
    auto pub = get_node()->create_publisher<sensor_msgs::msg::Image>(
        input_topic, rclcpp::QoS( 100 ).reliable() );

    // Create subscriber to measure latency
    // Use FIFO ordering - assumes frames are delivered in order
    auto sub = get_node()->create_subscription<sensor_msgs::msg::Image>(
        output_topic, rclcpp::QoS( 100 ).reliable(), [&]( sensor_msgs::msg::Image::SharedPtr msg ) {
          auto receive_time = std::chrono::steady_clock::now();

          // Get publish time from queue (FIFO ordering)
          std::chrono::steady_clock::time_point publish_time;
          {
            std::lock_guard<std::mutex> lock( publish_times_mutex_ );
            if ( publish_times_.empty() ) {
              return; // No matching publish time
            }
            publish_time = publish_times_.front();
            publish_times_.pop();
          }

          int count = ++received_count_;

          // Skip warmup frames for metrics
          if ( count > warmup_frames_ ) {
            auto latency =
                std::chrono::duration_cast<std::chrono::nanoseconds>( receive_time - publish_time );
            metrics_.record_frame( latency, msg->data.size() );

            // Calculate sink latency (buffer received at sink pad -> message received by subscriber)
            int64_t msg_pts = rclcpp::Time( msg->header.stamp ).nanoseconds();
            {
              std::lock_guard<std::mutex> lock( latency_data_mutex_ );
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
          if ( count == warmup_frames_ + 1 ) {
            metrics_.set_start_time();
          }
        } );

    // Allow time for discovery
    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    // Create round-trip pipeline
    std::ostringstream pipeline_ss;
    pipeline_ss << "rbfimagesrc topic=" << input_topic << " qos-reliability=reliable ! "
                << "rbfimagesink topic=" << output_topic << " sync=false";

    if ( !create_pipeline( pipeline_ss.str() ) ) {
      std::cerr << "Failed to create pipeline" << std::endl;
      return false;
    }

    // Set up pad probes for latency measurement
    if ( !setup_pad_probes() ) {
      std::cerr << "Failed to set up pad probes" << std::endl;
      return false;
    }

    if ( !start_pipeline() ) {
      std::cerr << "Failed to start pipeline" << std::endl;
      return false;
    }

    // Wait for pipeline to be ready
    std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );

    // Publish frames
    auto test_image = create_test_image( config );

    for ( int i = 0; i < total_frames; i++ ) {
      test_image->header.stamp = get_node()->get_clock()->now();
      auto publish_time = std::chrono::steady_clock::now();
      int64_t msg_pts = rclcpp::Time( test_image->header.stamp ).nanoseconds();

      {
        std::lock_guard<std::mutex> lock( publish_times_mutex_ );
        publish_times_.push( publish_time );
      }

      // Store publish time by PTS for source latency correlation
      {
        std::lock_guard<std::mutex> lock( latency_data_mutex_ );
        publish_times_by_pts_[msg_pts] = publish_time;
      }

      pub->publish( *test_image );

      // Pace the publishing to allow pipeline to process
      std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
    }

    bool success = true;
    // Wait for frames with timeout
    if ( !wait_for_count( received_count_, total_frames, std::chrono::seconds( 60 ) ) ) {
      std::cerr << "Timeout waiting for frames. Received: " << received_count_ << "/"
                << total_frames << std::endl;
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
  bool setup_pad_probes()
  {
    GstElement *pipeline = get_pipeline();
    if ( !pipeline ) {
      return false;
    }

    // Find the rbfimagesrc element
    GstIterator *it = gst_bin_iterate_elements( GST_BIN( pipeline ) );
    GValue item = G_VALUE_INIT;
    GstElement *src_element = nullptr;
    GstElement *sink_element = nullptr;

    while ( gst_iterator_next( it, &item ) == GST_ITERATOR_OK ) {
      GstElement *element = GST_ELEMENT( g_value_get_object( &item ) );
      gchar *name = gst_element_get_name( element );
      std::string element_name( name );
      g_free( name );

      // Get factory name to identify element type
      GstElementFactory *factory = gst_element_get_factory( element );
      if ( factory ) {
        const gchar *factory_name = gst_plugin_feature_get_name( GST_PLUGIN_FEATURE( factory ) );
        if ( g_strcmp0( factory_name, "rbfimagesrc" ) == 0 ) {
          src_element = element;
        } else if ( g_strcmp0( factory_name, "rbfimagesink" ) == 0 ) {
          sink_element = element;
        }
      }
      g_value_reset( &item );
    }
    g_value_unset( &item );
    gst_iterator_free( it );

    if ( !src_element || !sink_element ) {
      std::cerr << "Could not find rbfimagesrc or rbfimagesink elements" << std::endl;
      return false;
    }

    // Add probe to rbfimagesrc's src pad (buffer pushed)
    GstPad *src_pad = gst_element_get_static_pad( src_element, "src" );
    if ( src_pad ) {
      gst_pad_add_probe( src_pad, GST_PAD_PROBE_TYPE_BUFFER,
                         (GstPadProbeCallback)src_pad_probe_callback, this, nullptr );
      gst_object_unref( src_pad );
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

  static GstPadProbeReturn src_pad_probe_callback( GstPad *pad, GstPadProbeInfo *info,
                                                   gpointer user_data )
  {
    auto *self = static_cast<FullPipelineBenchmark *>( user_data );
    auto probe_time = std::chrono::steady_clock::now();

    GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER( info );
    if ( buffer && GST_BUFFER_PTS_IS_VALID( buffer ) ) {
      int64_t pts = GST_BUFFER_PTS( buffer );

      std::lock_guard<std::mutex> lock( self->latency_data_mutex_ );
      auto it = self->publish_times_by_pts_.find( pts );
      if ( it != self->publish_times_by_pts_.end() ) {
        // Only record after warmup
        int count = self->received_count_.load();
        if ( count >= self->warmup_frames_ ) {
          auto source_latency =
              std::chrono::duration_cast<std::chrono::nanoseconds>( probe_time - it->second );
          self->metrics_.record_source_latency( source_latency );
        }
        self->publish_times_by_pts_.erase( it );
      }
    }

    return GST_PAD_PROBE_OK;
  }

  static GstPadProbeReturn sink_pad_probe_callback( GstPad *pad, GstPadProbeInfo *info,
                                                    gpointer user_data )
  {
    auto *self = static_cast<FullPipelineBenchmark *>( user_data );
    auto probe_time = std::chrono::steady_clock::now();

    GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER( info );
    if ( buffer && GST_BUFFER_PTS_IS_VALID( buffer ) ) {
      int64_t pts = GST_BUFFER_PTS( buffer );

      std::lock_guard<std::mutex> lock( self->latency_data_mutex_ );
      // Store the time when buffer arrived at sink pad for later correlation
      self->sink_receive_times_by_pts_[pts] = probe_time;
    }

    return GST_PAD_PROBE_OK;
  }

  std::atomic<int> received_count_{ 0 };
  int warmup_frames_ = 0;
  BenchmarkMetrics metrics_;

  // Queue to track publish times for latency calculation (FIFO ordering)
  std::queue<std::chrono::steady_clock::time_point> publish_times_;
  std::mutex publish_times_mutex_;

  // Maps for pad probe latency correlation by buffer PTS
  std::map<int64_t, std::chrono::steady_clock::time_point> publish_times_by_pts_;
  std::map<int64_t, std::chrono::steady_clock::time_point> sink_receive_times_by_pts_;
  std::mutex latency_data_mutex_;
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

  FullPipelineBenchmark benchmark;
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
