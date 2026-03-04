#!/usr/bin/env python3
import rospy
from sensor_msgs.msg import PointCloud2, PointField
import struct

# Input layout from your topic:
# x(float32)@0, y(float32)@4, z(float32)@8, intensity(float32)@12,
# ring(uint16)@16, timestamp(float64)@18, point_step=26

IN_STEP = 26
OUT_STEP = 22  # 4*4 + 2 + 4

def cb(msg: PointCloud2):
    npts = msg.width * msg.height
    if npts <= 0:
        return

    data = msg.data
    if len(data) < npts * IN_STEP:
        rospy.logwarn_throttle(2.0, "Input data size smaller than expected.")
        return

    # Read first timestamp as t0
    # little-endian assumed (common for ROS on x86)
    t0 = struct.unpack_from('<d', data, 18)[0]

    out_data = bytearray(npts * OUT_STEP)

    for i in range(npts):
        in0 = i * IN_STEP

        x = struct.unpack_from('<f', data, in0 + 0)[0]
        y = struct.unpack_from('<f', data, in0 + 4)[0]
        z = struct.unpack_from('<f', data, in0 + 8)[0]
        intensity = struct.unpack_from('<f', data, in0 + 12)[0]
        ring = struct.unpack_from('<H', data, in0 + 16)[0]
        ts = struct.unpack_from('<d', data, in0 + 18)[0]

        t_rel = float(ts - t0)  # seconds (relative within this scan)

        out0 = i * OUT_STEP
        struct.pack_into('<f', out_data, out0 + 0, x)
        struct.pack_into('<f', out_data, out0 + 4, y)
        struct.pack_into('<f', out_data, out0 + 8, z)
        struct.pack_into('<f', out_data, out0 + 12, intensity)
        struct.pack_into('<H', out_data, out0 + 16, ring)
        struct.pack_into('<f', out_data, out0 + 18, t_rel)



    out = PointCloud2()
    out.header = msg.header

    npts = msg.width * msg.height
    out.height = 1
    out.width  = npts

    out.is_bigendian = False
    out.is_dense = msg.is_dense

    out.fields = [
        PointField(name="x",         offset=0,  datatype=PointField.FLOAT32, count=1),
        PointField(name="y",         offset=4,  datatype=PointField.FLOAT32, count=1),
        PointField(name="z",         offset=8,  datatype=PointField.FLOAT32, count=1),
        PointField(name="intensity", offset=12, datatype=PointField.FLOAT32, count=1),
        PointField(name="ring",      offset=16, datatype=PointField.UINT16,  count=1),
        PointField(name="time",      offset=18, datatype=PointField.FLOAT32, count=1),
    ]
    out.point_step = OUT_STEP
    out.row_step   = OUT_STEP * out.width
    out.data = bytes(out_data)

    pub.publish(out)

if __name__ == "__main__":
    rospy.init_node("vanjee_to_fastlio_cloud")
    in_topic  = rospy.get_param("~in",  "/vanjee_points722")
    out_topic = rospy.get_param("~out", "/vanjee_points722_time")
    pub = rospy.Publisher(out_topic, PointCloud2, queue_size=2)
    rospy.Subscriber(in_topic, PointCloud2, cb, queue_size=2)
    rospy.loginfo("Converting %s -> %s : timestamp(float64)->time(float32, relative)", in_topic, out_topic)
    rospy.spin()