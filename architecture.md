# Architecture Overview
This document serves as a critical, living template designed to equip agents with a rapid and comprehensive understanding of the codebase's architecture, enabling efficient navigation and effective contribution from day one. Update this document as the codebase evolves.

## 1. Project Structure
This section provides a high-level overview of the project's directory and file structure, categorised by architectural layer or major functional area. It is essential for quickly navigating the codebase, locating relevant files, and understanding the overall organization and separation of concerns.


```
gstreamer_ros_babel_fish/
├── .github/
│   └── workflows/
│       └── lint_build_test.yaml    # CI/CD pipeline configuration
├── gstreamer_ros_babel_fish/       # Main ROS 2 package
│   ├── include/
│   │   └── gstreamer_ros_babel_fish/
│   │       ├── format_conversion.hpp   # GStreamer <-> ROS format conversion utilities
│   │       ├── rbfimagesink.hpp        # GStreamer sink element header
│   │       ├── rbfimagesrc.hpp         # GStreamer source element header
│   │       └── ros_node_interface.hpp  # ROS node lifecycle management
│   ├── src/
│   │   ├── format_conversion.cpp   # Format conversion implementation
│   │   ├── plugin.cpp              # GStreamer plugin registration
│   │   ├── rbfimagesink.cpp        # Sink element (GStreamer -> ROS)
│   │   ├── rbfimagesrc.cpp         # Source element (ROS -> GStreamer)
│   │   └── ros_node_interface.cpp  # ROS node management implementation
│   ├── test/
│   │   ├── test_format_conversion.cpp  # Format conversion unit tests
│   │   ├── test_gst_to_ros.cpp         # GStreamer to ROS integration tests
│   │   └── test_ros_to_gst.cpp         # ROS to GStreamer integration tests
│   ├── CMakeLists.txt              # Build configuration
│   └── package.xml                 # ROS 2 package manifest
├── .clang-format                   # C++ code formatting rules
├── .pre-commit-config.yaml         # Pre-commit hook configuration
└── architecture.md                 # This document
```


## 2. High-Level System Diagram
The plugin acts as a bridge between GStreamer multimedia pipelines and ROS 2 image topics, enabling bidirectional image streaming.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GStreamer Pipeline                               │
└─────────────────────────────────────────────────────────────────────────┘
        │                                              ▲
        │ (video frames)                               │ (video frames)
        ▼                                              │
┌───────────────────┐                        ┌───────────────────┐
│   rbfimagesink    │                        │   rbfimagesrc     │
│  (GstBaseSink)    │                        │  (GstPushSrc)     │
└───────────────────┘                        └───────────────────┘
        │                                              ▲
        │ publishes                                    │ subscribes
        ▼                                              │
┌─────────────────────────────────────────────────────────────────────────┐
│                           ROS 2 Topics                                   │
│  sensor_msgs/msg/Image          sensor_msgs/msg/CompressedImage         │
└─────────────────────────────────────────────────────────────────────────┘
        │                                              ▲
        │                                              │
        ▼                                              │
┌─────────────────────────────────────────────────────────────────────────┐
│                    ROS 2 Nodes / Applications                            │
│          (image processing, visualization, recording, etc.)              │
└─────────────────────────────────────────────────────────────────────────┘
```

## 3. Core Components

### 3.1. GStreamer Plugin (gstrosbabelfish)

**Name:** gstrosbabelfish

**Description:** A GStreamer plugin that registers two elements (`rbfimagesrc` and `rbfimagesink`) for bidirectional ROS 2 image streaming. The plugin enables GStreamer pipelines to consume images from ROS 2 topics and publish processed video back to ROS 2.

**Technologies:** C++17, GStreamer 1.0, GLib/GObject

**Build Output:** `gstrosbabelfish.so` (installed to `lib/gstreamer-1.0/`)

### 3.2. GStreamer Elements

#### 3.2.1. rbfimagesrc (ROS -> GStreamer)

**Name:** ROS Babel Fish Image Source

**Description:** A GStreamer push source element that subscribes to ROS 2 image topics (`sensor_msgs/msg/Image` or `sensor_msgs/msg/CompressedImage`) and outputs video frames into a GStreamer pipeline. Supports automatic topic type detection, configurable QoS settings, and both raw and compressed image formats.

**Key Properties:**
- `topic`: ROS topic name to subscribe to (default: `/image`)
- `node-name`: Name for internal ROS node (default: `rbfimagesrc`)
- `node`: External ROS node pointer for integration with existing ROS applications
- `qos-reliability`: QoS reliability (best-effort/reliable)
- `qos-durability`: QoS durability (volatile/transient-local)
- `qos-history-depth`: QoS history depth
- `determine-framerate`: Whether to determine framerate from input topics (default: `false`)
- `wait-frame-count`: Number of frames to wait for framerate detection
- `framerate`: Manual framerate override (e.g. "30/1")

**Buffer Metadata:** Attaches `GstReferenceTimestampMeta` with `timestamp/x-unix` caps to each output buffer, carrying the original ROS header timestamp in nanoseconds. This allows downstream elements (including `rbfimagesink`) to recover the exact source timestamp even if the buffer PTS is modified by intermediate pipeline elements.

**Base Class:** GstPushSrc

#### 3.2.2. rbfimagesink (GStreamer -> ROS)

**Name:** ROS Babel Fish Image Sink

**Description:** A GStreamer sink element that receives video frames from a GStreamer pipeline and publishes them to ROS 2 image topics. Supports both raw images (published to `topic`) and compressed images (published to `topic/compressed`).

**Key Properties:**
- `topic`: Base ROS topic name (default: `/image`)
- `node-name`: Name for internal ROS node (default: `rbfimagesink`)
- `node`: External ROS node pointer
- `frame-id`: frame_id for ROS message headers
- `prefer-compressed`: Prefer compressed formats during caps negotiation
- `enable-nv-formats`: Enable NV formats (NV21, NV24)
- `subscription-count`: Number of accessible subscriptions (Read-only)

**Timestamp Resolution:** When determining the ROS message timestamp, the sink first checks for a `GstReferenceTimestampMeta` with `timestamp/x-unix` caps on the buffer. If present and valid (> 0), this timestamp is used directly. Otherwise, falls back to computing the absolute time from `base_time + buffer PTS`, or uses the current wall clock time as a last resort.

**Base Class:** GstBaseSink

### 3.3. Support Components

#### 3.3.1. RosNodeInterface

**Description:** Manages ROS 2 node lifecycle, supporting both internal node creation and external node injection. Handles rclcpp initialization, executor spinning, and graceful shutdown.

#### 3.3.2. RosContext

**Description:** Singleton class for global ROS context management with reference counting, ensuring proper rclcpp initialization and shutdown across multiple plugin instances.

#### 3.3.3. Format Conversion Utilities

**Description:** Provides bidirectional conversion between GStreamer video formats and ROS image encodings. Supports common formats (RGB, BGR, GRAY, etc.) and compressed formats (JPEG, PNG).

**Key Functions:**
- `gst_format_to_ros_encoding()`: Convert GstVideoFormat to ROS encoding string
- `ros_encoding_to_gst_format()`: Convert ROS encoding to GstVideoFormat
- `create_caps_from_ros_image()`: Create GStreamer caps from ROS image metadata
- `caps_to_compression_format()`: Detect compression format from GStreamer caps

## 4. Data Stores

This project does not use persistent data stores. All data flows through in-memory message queues between GStreamer buffers and ROS 2 messages.

## 5. External Integrations / APIs

### 5.1. ROS 2

**Purpose:** Robot Operating System middleware for inter-process communication

**Integration Method:** rclcpp (ROS 2 C++ client library)

**Message Types:**
- `sensor_msgs/msg/Image`: Raw image data
- `sensor_msgs/msg/CompressedImage`: Compressed image data (JPEG, PNG)

### 5.2. GStreamer

**Purpose:** Multimedia framework for building media pipelines

**Integration Method:** GStreamer C API, GstBase library

**Dependencies:**
- gstreamer-1.0
- gstreamer-base-1.0
- gstreamer-video-1.0

## 6. Deployment & Infrastructure

**Build System:** ament_cmake (ROS 2 build system)

**Supported ROS Distributions:**
- ROS 2 Jazzy (Ubuntu 24.04)
- ROS 2 Rolling (Ubuntu latest)

**CI/CD Pipeline:** GitHub Actions
- Linting job: pre-commit hooks (clang-format, cppcheck, cmake-format, etc.)
- Build & Test job: colcon build/test on multiple ROS distributions

**Installation:** The plugin is installed to `lib/gstreamer-1.0/` and registered via ament environment hooks for automatic GStreamer plugin path configuration.

## 7. Security Considerations

**Authentication:** N/A (relies on ROS 2 DDS security if configured)

**Authorization:** N/A (relies on ROS 2 DDS security if configured)

**Data Encryption:** Depends on underlying DDS implementation and ROS 2 security configuration

**Key Security Practices:**
- Input validation on ROS message data
- Bounds checking on image dimensions and data sizes
- Thread-safe access to shared state via mutexes

## 8. Development & Testing Environment

**Local Setup Instructions:**
1. Install ROS 2 (Jazzy or Rolling)
2. Install GStreamer development packages: `sudo apt install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev`
3. Clone repository into a ROS 2 workspace
4. Build with colcon: `colcon build`
5. Source the workspace: `source install/setup.bash`

**Testing Frameworks:**
- ament_cmake_gtest (Google Test integration for ROS 2)

**Code Quality Tools:**
- clang-format (C++ formatting)
- cppcheck (C++ static analysis)
- cmake-format / cmake-lint (CMake formatting and linting)
- ruff (Python linting, if Python files are added)
- pre-commit (automated hook execution)

**Pre-commit Setup:**
```bash
pip install pre-commit
pre-commit install
pre-commit run -a  # Run on all files
```

## 9. Future Considerations / Roadmap

- Support for additional image encodings (YUV formats, Bayer patterns)
- Zero-copy buffer sharing between GStreamer and ROS 2
- Support for camera_info topics
- Hardware-accelerated format conversion

## 10. Project Identification

**Project Name:** gstreamer_ros_babel_fish

**Repository URL:** https://github.com/StefanFabian/gstreamer_ros_babel_fish

**Primary Contact/Team:** Stefan Fabian

**Date of Last Update:** 2026-02-06

## 11. Glossary / Acronyms

**ROS:** Robot Operating System - middleware for robotics applications

**ROS 2:** Second generation of ROS with DDS-based communication

**GStreamer:** Open-source multimedia framework for building media pipelines

**DDS:** Data Distribution Service - publish-subscribe middleware standard

**QoS:** Quality of Service - configuration for reliability/durability of message delivery

**Caps:** GStreamer capabilities - describe the type of data that can flow through a pad

**rclcpp:** ROS 2 C++ client library

**sensor_msgs:** ROS 2 package containing common sensor message definitions

**ament:** ROS 2 build system and package management tools

**colcon:** Command-line tool for building ROS 2 workspaces
