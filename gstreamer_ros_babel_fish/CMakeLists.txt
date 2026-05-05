cmake_minimum_required(VERSION 3.5.1)
project(gstreamer_ros_babel_fish)

set(CMAKE_CXX_STANDARD 17)
add_definitions(-Wall -Wpedantic -Wno-deprecated-declarations)

find_package(ament_cmake REQUIRED)
find_package(ament_index_cpp REQUIRED)
find_package(camera_calibration_parsers REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)

include(FindPkgConfig)
pkg_check_modules(GST REQUIRED gstreamer-1.0 gstreamer-base-1.0
                  gstreamer-video-1.0 gstreamer-codecparsers-1.0)

# ##############################################################################
# Build ##
# ##############################################################################

set(SOURCES
    src/compressed_header_parser.cpp
    src/format_conversion.cpp
    src/ros_node_interface.cpp
    src/rbfimagesink.cpp
    src/rbfimagesrc.cpp
    src/plugin.cpp)

# Build as a GStreamer plugin (shared library)
add_library(gstrosbabelfish SHARED ${SOURCES})

target_include_directories(
  gstrosbabelfish PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                         $<INSTALL_INTERFACE:include> ${GST_INCLUDE_DIRS})

target_link_libraries(
  gstrosbabelfish PUBLIC ${GST_LIBRARIES}
                         ament_index_cpp::ament_index_cpp
                         camera_calibration_parsers::camera_calibration_parsers
                         rclcpp::rclcpp
                         ${sensor_msgs_TARGETS}
                         sensor_msgs::sensor_msgs_library)

# Remove lib prefix for GStreamer plugins
set_target_properties(gstrosbabelfish PROPERTIES PREFIX "")

# ##############################################################################
# Install ##
# ##############################################################################

# Install to GStreamer plugin directory
install(TARGETS gstrosbabelfish LIBRARY DESTINATION lib/gstreamer-1.0-plugins)

install(DIRECTORY include/ DESTINATION include)

# Export for downstream packages
ament_export_include_directories(include)
ament_export_dependencies(ament_index_cpp camera_calibration_parsers rclcpp sensor_msgs)

if(BUILD_TESTING)
  install(DIRECTORY test/calibrations DESTINATION share/${PROJECT_NAME}/test)
  find_package(ament_cmake_gtest REQUIRED)

  function(add_gst_test name source)
    ament_add_gtest(${name} ${source})
    if(TARGET ${name})
      target_link_libraries(${name} gstrosbabelfish)
      set_tests_properties(${name} PROPERTIES ENVIRONMENT
                           "GST_PLUGIN_PATH=${CMAKE_CURRENT_BINARY_DIR}")
    endif()
  endfunction()

  add_gst_test(test_format_conversion test/test_format_conversion.cpp)
  add_gst_test(test_gst_to_ros test/test_gst_to_ros.cpp)
  add_gst_test(test_ros_to_gst test/test_ros_to_gst.cpp)
  add_gst_test(test_timestamp_sync test/test_timestamp_sync.cpp)
  add_gst_test(test_validation test/test_validation.cpp)
  add_gst_test(test_shutdown_behavior test/test_shutdown_behavior.cpp)
  add_gst_test(test_subscription_count test/test_subscription_count.cpp)
  add_gst_test(test_rbfimagesrc_standalone test/test_rbfimagesrc_standalone.cpp)
  add_gst_test(test_rbfimagesink_standalone test/test_rbfimagesink_standalone.cpp)
  add_gst_test(test_pts_generation test/test_pts_generation.cpp)
  add_gst_test(test_rbfimagesrc_framerate test/test_rbfimagesrc_framerate.cpp)
  add_gst_test(test_nv_formats test/test_nv_formats.cpp)
endif()

# ##############################################################################
# Benchmark executables ##
# ##############################################################################

add_executable(benchmark_imagesink benchmark/benchmark_imagesink.cpp)
target_link_libraries(benchmark_imagesink gstrosbabelfish)
target_include_directories(benchmark_imagesink
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/benchmark)

add_executable(benchmark_imagesrc benchmark/benchmark_imagesrc.cpp)
target_link_libraries(benchmark_imagesrc gstrosbabelfish)
target_include_directories(benchmark_imagesrc
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/benchmark)

add_executable(benchmark_full_pipeline benchmark/benchmark_full_pipeline.cpp)
target_link_libraries(benchmark_full_pipeline gstrosbabelfish)
target_include_directories(benchmark_full_pipeline
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/benchmark)

install(TARGETS benchmark_imagesink benchmark_imagesrc benchmark_full_pipeline
        DESTINATION lib/${PROJECT_NAME})

ament_environment_hooks(register_gstreamer_plugin.dsv)
ament_package()
