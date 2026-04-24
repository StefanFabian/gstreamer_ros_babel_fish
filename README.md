[![License](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=StefanFabian_gstreamer_ros_babel_fish&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=StefanFabian_gstreamer_ros_babel_fish)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=StefanFabian_gstreamer_ros_babel_fish&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=StefanFabian_gstreamer_ros_babel_fish)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=StefanFabian_gstreamer_ros_babel_fish&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=StefanFabian_gstreamer_ros_babel_fish)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=StefanFabian_gstreamer_ros_babel_fish&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=StefanFabian_gstreamer_ros_babel_fish)

# GStreamer ROS 2 Babel Fish

This project provides GStreamer elements for bridging ROS 2 image topics with GStreamer pipelines.
It allows for high-performance streaming of image data between ROS 2 and GStreamer, supporting both raw and compressed image formats.

For a detailed understanding of the system's design and component interactions, please refer to the [Architecture Overview](architecture.md).

## Features

- **rbfimagesrc**: GStreamer source element that subscribes to ROS 2 image topics.
    - Supports `sensor_msgs/msg/Image` (raw) and `sensor_msgs/msg/CompressedImage`.
    - Automatically handles format conversion.
    - Configurable QoS settings.
- **rbfimagesink**: GStreamer sink element that publishes to ROS 2 image topics.
    - Automatically determines whether to publish `sensor_msgs/msg/Image` (raw) or `sensor_msgs/msg/CompressedImage` (e.g., JPEG, PNG) based on the input caps.
    - Zero-copy publishing where possible (using shared memory / loaned messages if supported).

> [!NOTE]
> Both elements use `GstReferenceTimestampMeta` with `timestamp/x-unix` to attach and recover the timestamp of the ROS image,
> enabling the processing of image buffers without losing time information (as long as all processing elements preserve it).

## Installation

This package is a ROS 2 package. You can build it using `colcon`.

1.  Create a ROS 2 workspace (if you haven't already):
    ```bash
    mkdir -p ~/ros2_ws/src
    cd ~/ros2_ws/src
    ```

2.  Clone the repository:
    ```bash
    git clone https://github.com/StefanFabian/gstreamer_ros_babel_fish
    ```

3.  Install dependencies using `rosdep`:
    ```bash
    cd ~/ros2_ws
    rosdep update
    rosdep install --from-paths src --ignore-src -r -y
    ```

4.  Build the package:
    ```bash
    colcon build --packages-select gstreamer_ros_babel_fish
    ```

5.  Source the workspace:
    ```bash
    source install/setup.bash
    ```

## Usage

After sourcing the workspace again, the GStreamer elements `rbfimagesrc` and `rbfimagesink` should be available. You can verify this with:

```bash
gst-inspect-1.0 rbfimagesrc
gst-inspect-1.0 rbfimagesink
```

### Example Pipelines

#### 1. Display a ROS Image Topic

Subscribe to a ROS topic `/camera/image_raw` and display it using `autovideosink`.

```bash
gst-launch-1.0 rbfimagesrc topic=/camera/image_raw ! videoconvert ! autovideosink
```

#### 2. Stream Video to ROS

Generate a test video pattern and publish it to `/test_video`.

```bash
gst-launch-1.0 videotestsrc ! video/x-raw,width=640,height=480,framerate=30/1 ! rbfimagesink topic=/test_video sync=true
```

#### 3. Publish Compressed Images

Encode video as JPEG and publish to `/compressed_video/compressed`.

```bash
gst-launch-1.0 videotestsrc ! video/x-raw,width=640,height=480,framerate=30/1 ! jpegenc ! rbfimagesink topic=/compressed_video sync=true
```

> [!NOTE]
> When receiving compressed data (image/jpeg, image/png), `rbfimagesink` automatically publishes to `<topic>/compressed`.

#### 4. Roundtrip (ROS -> GStreamer -> ROS)

Subscribe to `/input_topic`, process it (e.g., flip), and publish to `/output_topic`.

```bash
gst-launch-1.0 rbfimagesrc topic=/input_topic ! videoflip method=clockwise ! rbfimagesink topic=/output_topic
```

#### 5. Using with Compressed Input

Subscribe to a compressed topic `/compressed_video/compressed` (JPEG), decode it, and display.

```bash
gst-launch-1.0 rbfimagesrc determine-framerate=true topic=/compressed_video/compressed ! jpegdec ! videoconvert ! autovideosink
```

> [!NOTE]
> `rbfimagesrc` automatically detects if the topic is compressed or raw.

#### 6. Stream camera

Stream a V4L2 compatible USB or MIPI-CSI camera.

```bash
gst-launch-1.0 v4l2src ! rbfimagesink topic=/camera/image_raw
```

> [!NOTE]
> On my laptop, this automatically chooses the webcam with the highest resolution as jpeg stream
> and publishes the jpeg stream directly as `/camera/image_raw/compressed` resulting in sub-ms latency.  
> For information on how to configure v4l2src to use a different device, use `gst-inspect-1.0 v4l2src`.

## Configuration

### rbfimagesrc Properties

- `determine-framerate`: Whether to determine the framerate from the topic (default: `false`)
- `topic`: ROS topic to subscribe to (default: `/image`)
- `node`: (Pointer) External `rclcpp::Node` pointer (optional)
- `node-name`: Name of the internal node if no external node is provided (default: `rbfimagesrc`)
- `qos-reliability`: `best-effort` (default) or `reliable`
- `qos-durability`: `volatile` (default) or `transient-local`
- `qos-history-depth`: History depth for subscription (default: 1)
- `wait-frame-count`: Number of frames to use for framerate detection (default: 5). Only used if `determine-framerate` is `true`.
- `framerate`: Manual framerate to set on the source caps (e.g. "30/1")

### rbfimagesink Properties

- `topic`: Base ROS topic name (default: `/image`)
- `node`: (Pointer) External `rclcpp::Node` pointer (optional)
- `node-name`: Name of the internal node if no external node is provided (default: `rbfimagesink`)
- `frame-id`: `frame_id` to set in the ROS message header
- `camera-info-url`: `file://` or `package://` URL for camera calibration data. When set, the sink publishes the sibling `camera_info` topic and exposes a sibling `set_camera_info` service.
- `prefer-compressed`: Whether to prefer compressed formats during negotiation if both available (default: `true`)
- `subscription-count`: Number of current subscriptions to the topic (read-only)
- `enable-nv-formats`: Enable NV formats (NV21, NV24) (default: `false`)

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) - see the [LICENSE](LICENSE) file for details.
