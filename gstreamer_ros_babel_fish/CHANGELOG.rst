^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package gstreamer_ros_babel_fish
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Forthcoming
-----------
* Use framerate in caps if known.
* Round in framerate determination and improve PTS robustness.
  This avoids exotic framerates that are incompatible with most pipeline elements.
  Previously the pts was starting at 0 from the first pushed frame but when determining, we drop a few frames and when we continue, the running time of the pipeline may already >100ms and the buffers are dropped due to lateness if sync is true on the sink. Now the ROS <-> gstreamer offset is computed to the running time.
* Register plugin path as gstreamer plugin subdirectory instead of lib dir.
* Added enable-nv-formats property to sink which defaults to false as cv bridge can not handle those formats.
* Initial release.
* Contributors: Stefan Fabian
