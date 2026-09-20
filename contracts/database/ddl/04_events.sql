-- =====================================================================
-- HunterCore 数据库契约 — 04 事件
-- schema：data_collector
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- 约束：event_type 取值 = 设计文档“事件类型定义”（19 种，触发阈值不可更改；G-22② 新增 collision_pre_warning）；
--       event_level ∈ (info, warning, critical)
-- =====================================================================

CREATE TABLE IF NOT EXISTS data_collector.events (
    event_id         BIGSERIAL   PRIMARY KEY,
    vehicle_id       TEXT        NOT NULL,          -- 逻辑外键 → vehicle_svc.vehicles.vehicle_id
    event_type       TEXT        NOT NULL
        CHECK (event_type IN ('harsh_acceleration', 'harsh_braking', 'harsh_turning', 'over_speed',
                              'collision_pre_warning', 'collision_warning', 'manual_takeover', 'emergency_stop', 'battery_low',
                              'battery_critical', 'communication_loss', 'sensor_fault', 'perception_fault',
                              'planning_fault', 'control_fault', 'ota_start', 'ota_success',
                              'ota_failed', 'ota_rollback')),
    event_level      TEXT        NOT NULL
        CHECK (event_level IN ('info', 'warning', 'critical')),
    event_time       TIMESTAMPTZ NOT NULL,          -- 车端事件发生时间（非入库时间）
    description      TEXT,
    data_json        JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 事件上下文（阈值、实测值、轨迹片段摘要）
    data_file_url    TEXT,                          -- 关联文件（MinIO hunter-rosbag / hunter-raw-data / hunter-video）
    acknowledged     BOOLEAN     NOT NULL DEFAULT FALSE,
    acknowledged_by  UUID,                          -- 逻辑外键 → user_svc.users.user_id
    acknowledge_time TIMESTAMPTZ
);
COMMENT ON TABLE data_collector.events IS
    '车辆事件；event_level 由 event_type 决定（阈值见设计文档“事件类型定义”，代码不得放宽）';
COMMENT ON COLUMN data_collector.events.data_file_url IS 'MinIO 对象地址（下载用 15 分钟预签名 URL，不落长期签名）';

-- 单车事件时间线（车辆详情页）
CREATE INDEX IF NOT EXISTS idx_events_vehicle_time
    ON data_collector.events (vehicle_id, event_time DESC);
-- 按类型统计（分类报表 / Corner Case 挖掘）
CREATE INDEX IF NOT EXISTS idx_events_type_time
    ON data_collector.events (event_type, event_time DESC);
-- 告警中心：仅未确认事件（部分索引，critical 优先展示）
CREATE INDEX IF NOT EXISTS idx_events_unacknowledged
    ON data_collector.events (event_level, event_time DESC) WHERE acknowledged = FALSE;
-- 事件上下文 JSONB 检索
CREATE INDEX IF NOT EXISTS idx_events_data_json
    ON data_collector.events USING GIN (data_json);
-- 消费幂等保护：Kafka at-least-once 重放时同一 (vehicle_id, event_type, event_time) 不重复入库
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_vehicle_type_time
    ON data_collector.events (vehicle_id, event_type, event_time);
