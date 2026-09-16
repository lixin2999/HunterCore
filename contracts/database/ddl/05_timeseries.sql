-- =====================================================================
-- HunterEdge 数据库契约 — 05 时序数据（TimescaleDB hypertable）
-- schema：data_collector（vehicle_telemetry）/ data_analytics（algorithm_metrics）
-- 来源：设计文档第 9 章 + 遥测消息格式（字段即消息字段的扁平化映射）
-- 约束（不可更改）：
--   - chunk_time_interval = 1 day，保留策略 90 天
--   - 主键必须包含分区键 time（TimescaleDB 要求）
--   - 写入吞吐 ≥ 10000 点/秒（批量插入），入库延迟 ≤ 1s
-- ⚠ 需核对：
--   - seq 列（消息幂等去重用）是否在表结构中；字段是否需按模块拆分（当前按遥测消息扁平化）
--   - 是否启用压缩策略（compression）与连续聚合（continuous aggregate）
--   - 跨服务只读例外：data-analytics / Spark 离线分析需读 data_collector.vehicle_telemetry，
--     采用受限只读账号（详见 er.md「跨 schema 访问例外」）
-- =====================================================================

-- ---------- data_collector.vehicle_telemetry：车辆遥测时序 ----------
CREATE TABLE IF NOT EXISTS data_collector.vehicle_telemetry (
    time                TIMESTAMPTZ      NOT NULL,               -- 分区键（车端上报时间）
    vehicle_id          TEXT             NOT NULL,
    seq                 BIGINT,                                  -- 车端消息序号（幂等/丢包检测）
    -- ---- chassis（底盘） ----
    velocity            DOUBLE PRECISION,
    steering_angle      DOUBLE PRECISION,
    battery_voltage     DOUBLE PRECISION,
    battery_soc         SMALLINT         CHECK (battery_soc IS NULL OR (battery_soc BETWEEN 0 AND 100)),
    battery_current     DOUBLE PRECISION,
    battery_temp        DOUBLE PRECISION,
    control_mode        TEXT,
    vehicle_state       TEXT,
    fault_code          INTEGER,
    motor_rpm           INTEGER[],
    motor_current       DOUBLE PRECISION[],
    motor_temp          INTEGER[],
    -- ---- localization（定位） ----
    x                   DOUBLE PRECISION,
    y                   DOUBLE PRECISION,
    z                   DOUBLE PRECISION,
    roll                DOUBLE PRECISION,
    pitch               DOUBLE PRECISION,
    heading             DOUBLE PRECISION,
    linear_velocity     DOUBLE PRECISION[],
    angular_velocity    DOUBLE PRECISION[],
    position_std        DOUBLE PRECISION,
    heading_std         DOUBLE PRECISION,
    -- ---- perception（感知） ----
    detected_objects    INTEGER,
    fps                 DOUBLE PRECISION,
    latency_ms          DOUBLE PRECISION,
    object_types        JSONB,
    -- ---- planning（规划） ----
    trajectory_length   DOUBLE PRECISION,
    trajectory_points   INTEGER,
    planning_latency_ms DOUBLE PRECISION,
    current_behavior    TEXT,
    -- ---- control（控制） ----
    target_velocity     DOUBLE PRECISION,
    target_steer        DOUBLE PRECISION,
    velocity_error      DOUBLE PRECISION,
    steer_error         DOUBLE PRECISION,
    control_latency_ms  DOUBLE PRECISION,
    -- ---- system（车载计算平台） ----
    cpu_usage           DOUBLE PRECISION,
    gpu_usage           DOUBLE PRECISION,
    memory_usage_mb     BIGINT,
    gpu_temp            DOUBLE PRECISION,
    cpu_temp            DOUBLE PRECISION,
    network_rssi        INTEGER,
    network_latency_ms  DOUBLE PRECISION,
    PRIMARY KEY (time, vehicle_id)
);
COMMENT ON TABLE data_collector.vehicle_telemetry IS
    '遥测时序表（hypertable，按天分块、保留 90 天）；由 data-collector 批量写入，禁止逐条 INSERT';

-- 单车时间区间查询（轨迹回放 / 指标计算，设计文档指定索引）
CREATE INDEX IF NOT EXISTS idx_vehicle_telemetry_vehicle_time
    ON data_collector.vehicle_telemetry (vehicle_id, time DESC);

-- hypertable 化（若已存在则跳过）
SELECT create_hypertable('data_collector.vehicle_telemetry', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

-- 保留策略：90 天后自动删除（chunk 级 drop，避免 DELETE 膨胀）
SELECT add_retention_policy('data_collector.vehicle_telemetry', INTERVAL '90 days',
                            if_not_exists => TRUE);

-- ---------- data_analytics.algorithm_metrics：算法模块指标时序 ----------
CREATE TABLE IF NOT EXISTS data_analytics.algorithm_metrics (
    time         TIMESTAMPTZ      NOT NULL,   -- 分区键
    vehicle_id   TEXT             NOT NULL,
    module       TEXT             NOT NULL
        CHECK (module IN ('perception', 'planning', 'control')),   -- 模块枚举（不可新增）
    metric_name  TEXT             NOT NULL,   -- 指标名（如 detection_latency_ms / planning_success_rate）
    metric_value DOUBLE PRECISION NOT NULL,
    tags         JSONB            NOT NULL DEFAULT '{}'::jsonb,    -- 维度标签（版本/路段/场景类型等）
    PRIMARY KEY (time, vehicle_id, module, metric_name)
);
COMMENT ON TABLE data_analytics.algorithm_metrics IS
    '算法模块指标时序（hypertable，按天分块、保留 90 天）；由 data-analytics 写入';

CREATE INDEX IF NOT EXISTS idx_algorithm_metrics_vehicle_time
    ON data_analytics.algorithm_metrics (vehicle_id, time DESC);
-- 指标趋势查询（指定模块 + 指标名）
CREATE INDEX IF NOT EXISTS idx_algorithm_metrics_module_metric_time
    ON data_analytics.algorithm_metrics (module, metric_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_algorithm_metrics_tags
    ON data_analytics.algorithm_metrics USING GIN (tags);

SELECT create_hypertable('data_analytics.algorithm_metrics', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

SELECT add_retention_policy('data_analytics.algorithm_metrics', INTERVAL '90 days',
                            if_not_exists => TRUE);

-- =====================================================================
-- 写入规范（性能指标：时序数据写入 ≥ 10000 点/秒）
--   1) 使用 COPY / executemany 批量写入（禁止逐条 INSERT + 逐条 commit）
--   2) vehicle_id 与 time 必须由车端时间戳提供（禁止用入库时间替代，保证回放一致性）
--   3) 重复时间点写入使用 ON CONFLICT (time, vehicle_id) DO NOTHING 保证幂等
-- =====================================================================

