-- =====================================================================
-- HunterEdge 数据库契约 — 01 车辆主数据 + 用户与 RBAC
-- schema：vehicle_svc（车辆）/ user_svc（用户与权限五表）
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- ⚠ 需核对：status（users）取值域、email/phone 长度约束按最小必要原则定义
-- =====================================================================

-- ---------- vehicle_svc.vehicles：车辆台账 ----------
CREATE TABLE IF NOT EXISTS vehicle_svc.vehicles (
    vehicle_id       TEXT        PRIMARY KEY,   -- 车辆唯一标识（如 HUNTER-001）= 设备证书 CommonName = Kafka 消息 key
    vehicle_name     TEXT        NOT NULL,
    model            TEXT        NOT NULL DEFAULT 'HUNTER_SE',  -- 硬件基线：HUNTER SE 阿克曼 UGV
    firmware_version TEXT,
    software_version TEXT,
    status           TEXT        NOT NULL DEFAULT 'offline'
        CHECK (status IN ('offline', 'online_idle', 'auto_driving', 'remote_controlled',
                          'upgrading', 'charging', 'fault', 'emergency')),
    last_online_time TIMESTAMPTZ,                               -- 遥测/心跳最近上报时间
    register_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    device_cert_sn   TEXT,                                      -- X.509 设备证书序列号
    description      TEXT
);
COMMENT ON TABLE vehicle_svc.vehicles IS
    '车辆台账；status 取值见设计文档“车辆状态定义”（8 态，不可新增/更改）';

-- 设备证书序列号唯一（部分唯一索引：允许未签发证书的车辆先登记）
CREATE UNIQUE INDEX IF NOT EXISTS uq_vehicles_device_cert_sn
    ON vehicle_svc.vehicles (device_cert_sn) WHERE device_cert_sn IS NOT NULL;
-- 车队状态看板查询：按状态过滤 + 最近在线倒序
CREATE INDEX IF NOT EXISTS idx_vehicles_status_last_online
    ON vehicle_svc.vehicles (status, last_online_time DESC NULLS LAST);

-- ---------- user_svc.users：用户 ----------
CREATE TABLE IF NOT EXISTS user_svc.users (
    user_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    username        TEXT        NOT NULL,
    password_hash   TEXT        NOT NULL,       -- bcrypt 哈希，禁止明文/可逆加密（安全机制）
    real_name       TEXT,
    email           TEXT,
    phone           TEXT,
    status          TEXT        NOT NULL DEFAULT 'enabled'
        CHECK (status IN ('enabled', 'disabled', 'locked')),  -- ⚠ 需核对：locked 用于登录失败锁定
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_time TIMESTAMPTZ
);
COMMENT ON TABLE user_svc.users IS '平台用户；password_hash 使用 bcrypt，禁止在日志/接口中输出';

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_username ON user_svc.users (username);
-- 邮箱唯一（大小写不敏感，且允许为空）
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email
    ON user_svc.users (lower(email)) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_status ON user_svc.users (status);

-- ---------- user_svc.roles：角色 ----------
CREATE TABLE IF NOT EXISTS user_svc.roles (
    role_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    role_code   TEXT        NOT NULL,        -- 角色编码（如 admin / operator / analyst / viewer）
    role_name   TEXT        NOT NULL,
    description TEXT,
    status      TEXT        NOT NULL DEFAULT 'enabled'
        CHECK (status IN ('enabled', 'disabled')),
    create_time TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE user_svc.roles IS '角色（RBAC）：role_code 为对外稳定标识，禁止用 role_id 做业务判断';
CREATE UNIQUE INDEX IF NOT EXISTS uq_roles_role_code ON user_svc.roles (role_code);

-- ---------- user_svc.permissions：权限点 ----------
CREATE TABLE IF NOT EXISTS user_svc.permissions (
    permission_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    permission_code TEXT        NOT NULL,    -- 权限编码，形如 <resource>:<action>（如 scene:create / ota:release）
    permission_name TEXT        NOT NULL,
    resource        TEXT        NOT NULL,    -- 资源域：scene / data / analytics / ota / remote / vehicle / user
    action          TEXT        NOT NULL,    -- 动作：create / read / update / delete / execute
    description     TEXT,
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE user_svc.permissions IS
    '权限点（RBAC）；resource 与网关路由表资源域一一对应，便于前端 v-permission 指令校验';
CREATE UNIQUE INDEX IF NOT EXISTS uq_permissions_permission_code
    ON user_svc.permissions (permission_code);
CREATE INDEX IF NOT EXISTS idx_permissions_resource_action
    ON user_svc.permissions (resource, action);

-- ---------- user_svc.user_roles：用户-角色关联 ----------
CREATE TABLE IF NOT EXISTS user_svc.user_roles (
    user_id     UUID        NOT NULL REFERENCES user_svc.users (user_id) ON DELETE CASCADE,
    role_id     UUID        NOT NULL REFERENCES user_svc.roles (role_id) ON DELETE CASCADE,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, role_id)
);
COMMENT ON TABLE user_svc.user_roles IS '用户-角色关联（多对多）；删除用户/角色级联清理';
CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_svc.user_roles (role_id);

-- ---------- user_svc.role_permissions：角色-权限关联 ----------
CREATE TABLE IF NOT EXISTS user_svc.role_permissions (
    role_id       UUID        NOT NULL REFERENCES user_svc.roles (role_id) ON DELETE CASCADE,
    permission_id UUID        NOT NULL REFERENCES user_svc.permissions (permission_id) ON DELETE CASCADE,
    create_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, permission_id)
);
COMMENT ON TABLE user_svc.role_permissions IS '角色-权限关联（多对多）；鉴权时按 user → roles → permissions 展开';
CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id
    ON user_svc.role_permissions (permission_id);

