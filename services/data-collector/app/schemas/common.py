"""data-collector 共享常量与枚举（data-collector.yaml components.schemas 共享部分）。"""
from __future__ import annotations

from enum import Enum

#: 车辆标识模式（契约：^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$）
VEHICLE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
#: 时序表保留天数（契约固定 90：contracts/database/ddl/05_timeseries.sql）
TELEMETRY_RETENTION_DAYS = 90
#: sensor_file 通知 Topic（x-hunter-file-upload-flow.notify_topic，命名不可更改）
SENSOR_FILE_TOPIC = "sensor_file"
#: 5.5 节对象路径命名规范（相对 Key，不含 bucket 段）
OBJECT_KEY_TEMPLATE = "{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}{ext}"


class UploadBucket(str, Enum):
    """车端上传类 Bucket（仅此三个允许车辆写入；生命周期见 MinIO Bucket 规划）。"""

    RAW_DATA = "hunter-raw-data"    # 30 天自动删除（点云/图像）
    ROSBAG = "hunter-rosbag"        # 事件数据永久，常规 30 天
    VIDEO = "hunter-video"          # 90 天（远程操控录像）


class FileDataType(str, Enum):
    """数据类型（命名规范 {data_type} 段与 Bucket 映射；契约暂定值）。"""

    POINT_CLOUD = "point_cloud"
    CAMERA_IMAGE = "camera_image"
    ROSBAG = "rosbag"
    VIDEO = "video"
    OTHER = "other"


class UploadMethod(str, Enum):
    """上传方式（put 单次直传 / multipart 分片上传，大文件 ROS Bag 用分片）。"""

    PUT = "put"
    MULTIPART = "multipart"
