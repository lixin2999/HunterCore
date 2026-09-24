-- =====================================================================
-- HunterCore 单机部署包 — 初始数据 init-data.sql
-- 目标库：hunter_core（业务库，schema：user_svc / vehicle_svc）
-- 依赖：先执行 schema.sql（表与索引已创建）
-- 幂等：全部 INSERT 使用 ON CONFLICT DO NOTHING，可重复执行
--
-- 数据来源（禁止自行发明）：
--   1) 角色编码：contracts/openapi/api-gateway.yaml（role_code 如 admin / operator / analyst / viewer）
--   2) 权限编码：<resource>:<action>，词表见 contracts/database/enums.md 第 8 节
--      （resource ∈ scene/data/analytics/ota/remote/vehicle/user；action ∈ create/read/update/delete/execute）
--   3) 角色→权限矩阵：取自各服务 RBAC 配置默认值（服务侧既有事实，非本文件发明）：
--        scene-service   : SCENE_READ_ROLES=admin,analyst,operator
--                          SCENE_WRITE_ROLES=admin,operator / SCENE_EXECUTE_ROLES=admin,operator
--        data-collector  : DATA_READ_ROLES=admin,analyst,operator / DATA_EXECUTE_ROLES=admin,operator
--        data-analytics  : ANALYTICS_READ_ROLES=admin,analyst,operator
--                          ANALYTICS_EXECUTE_ROLES=admin,analyst
--        ota-service     : OTA_READ_ROLES=admin,analyst,operator / OTA_CREATE_ROLES=admin,operator
--                          OTA_EXECUTE_ROLES=admin,operator
--        remote-control  : RC_READ_ROLES=admin,operator,viewer / RC_CREATE_ROLES=admin,operator
--                          RC_EXECUTE_ROLES=admin,operator
--      ⚠ 待人工确认：设计文档未给出完整 RBAC 权限矩阵；vehicle/user 资源域权限点待
--        vehicle-service / user-service 契约落地后补充（见文件末尾注释块）
--
-- 执行：
--   $ docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 \
--         < /opt/hunter-core/sql/init-data.sql
--   注入自定义管理员口令哈希（推荐，G-06 后为必选路径）：
--   $ docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 \
--         -v admin_password_hash="<bcrypt hash>" < /opt/hunter-core/sql/init-data.sql
-- =====================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------
-- 0. 管理员初始口令哈希（bcrypt，12 轮）
--    ⚠ G-06（设计文档 14.1）：不再内置已知初始口令（历史 Hunter@2025 已废弃）。
--   必须经 psql -v admin_password_hash="<bcrypt hash>" 注入（install/init-db 自动生成随机口令）；
--   未注入时 admin 以「不可登录占位哈希 + disabled + 强制改密」创建，防止已知口令上线。
--    外部可用 psql -v admin_password_hash="<bcrypt hash>" 覆盖（生成方式见第四章）
-- ---------------------------------------------------------------------
\if :{?admin_password_hash}
\echo '[init-data] 使用外部传入的 admin_password_hash（psql -v）'
\else
\echo '[init-data] ⚠ 未传入 admin_password_hash：admin 将以禁用+占位哈希创建（G-06：不携带已知口令），需运维重置口令后启用'
\set admin_password_hash 'G06-NO-DEFAULT-PASSWORD-RESET-REQUIRED'
\endif

-- ---------------------------------------------------------------------
-- 1. 角色（user_svc.roles）——4 个角色编码为契约值
-- ---------------------------------------------------------------------
INSERT INTO user_svc.roles (role_code, role_name, description, status) VALUES
    ('admin',    '系统管理员', '平台全量权限（含 OTA 发布、远程操控强制结束、数据权限豁免）', 'enabled'),
    ('operator', '运维操作员', '场景编辑与执行、OTA 任务执行、远程操控（不含用户管理）',       'enabled'),
    ('analyst',  '数据分析师', '数据/场景/OTA/分析结果只读 + 分析作业执行（不可远程操控）',   'enabled'),
    ('viewer',   '只读访客',   '只读访问（支持操控记录与录像回看）',                          'enabled')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- 2. 权限点（user_svc.permissions）——按各服务契约声明的 action 集合落库
--    scene     : create / read / update / delete / execute（scene-service.yaml 第 1223 行）
--    data      : create / read / execute（data-collector.yaml 第 467 行）
--    analytics : read / execute（data-analytics.yaml 第 54 行）
--    ota       : create / read / execute（ota-service.yaml x-hunter-endpoints 契约表）
--    remote    : create / read / execute（remote-control.yaml RBAC 声明）
-- ---------------------------------------------------------------------
INSERT INTO user_svc.permissions (permission_code, permission_name, resource, action, description) VALUES
    ('scene:create',     '场景创建',       'scene',     'create',  '创建场景 / 复制场景'),
    ('scene:read',       '场景查看',       'scene',     'read',    '场景列表、详情、模板、导出下载'),
    ('scene:update',     '场景编辑',       'scene',     'update',  '编辑草稿场景、参数化覆盖'),
    ('scene:delete',     '场景删除',       'scene',     'delete',  '删除场景（软删除）'),
    ('scene:execute',    '场景下发',       'scene',     'execute', '发布场景 / 下发 Carla 仿真运行'),
    ('data:create',      '数据上传',       'data',      'create',  '申请预签名 URL、上传传感器文件/ROS Bag'),
    ('data:read',        '数据查看',       'data',      'read',    '遥测查询、事件列表、文件下载'),
    ('data:execute',     '数据处置',       'data',      'execute', '事件确认、数据导出与重处理'),
    ('analytics:read',   '分析查看',       'analytics', 'read',    '指标看板、报告与 Corner Case 查询'),
    ('analytics:execute','分析执行',       'analytics', 'execute', '触发报告生成 / 分析作业提交'),
    ('ota:create',       'OTA 版本任务创建','ota',      'create',  '上传版本、创建升级任务'),
    ('ota:read',         'OTA 查看',       'ota',       'read',    '版本仓库、任务列表、升级记录查询'),
    ('ota:execute',      'OTA 执行',       'ota',       'execute', '发布版本、启动/暂停/回滚任务'),
    ('remote:create',    '操控会话创建',   'remote',    'create',  '发起远程操控会话（车辆互斥）'),
    ('remote:read',      '操控查看',       'remote',    'read',    '会话状态、操控历史与录像下载'),
    ('remote:execute',   '操控执行',       'remote',    'execute', '下发控制指令、结束会话、紧急停车')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- 3. 角色-权限绑定（user_svc.role_permissions）
-- ---------------------------------------------------------------------
-- admin：全量权限（与各服务对 admin 的豁免语义一致）
INSERT INTO user_svc.role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM user_svc.roles r
CROSS JOIN user_svc.permissions p
WHERE r.role_code = 'admin'
ON CONFLICT DO NOTHING;

-- operator：scene 全部 + data(read/create/execute) + analytics:read
--           + ota(read/create/execute) + remote(read/create/execute)
INSERT INTO user_svc.role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM user_svc.roles r
JOIN user_svc.permissions p ON p.permission_code IN (
        'scene:create', 'scene:read', 'scene:update', 'scene:delete', 'scene:execute',
        'data:create', 'data:read', 'data:execute',
        'analytics:read',
        'ota:create', 'ota:read', 'ota:execute',
        'remote:create', 'remote:read', 'remote:execute')
WHERE r.role_code = 'operator'
ON CONFLICT DO NOTHING;

-- analyst：只读 + analytics:execute（可执行分析作业；不可编辑场景/OTA、不可远程操控）
INSERT INTO user_svc.role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM user_svc.roles r
JOIN user_svc.permissions p ON p.permission_code IN (
        'scene:read', 'data:read', 'analytics:read', 'analytics:execute', 'ota:read', 'remote:read')
WHERE r.role_code = 'analyst'
ON CONFLICT DO NOTHING;

-- viewer：仅只读（含操控历史与录像回看）
INSERT INTO user_svc.role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM user_svc.roles r
JOIN user_svc.permissions p ON p.permission_code IN (
        'scene:read', 'data:read', 'analytics:read', 'ota:read', 'remote:read')
WHERE r.role_code = 'viewer'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- 4. 默认管理员（user_svc.users + user_roles）
--    ⚠ G-06：不内置已知口令；哈希由 psql -v 注入（占位哈希 → disabled 不可登录）；
--   无论何种路径创建，must_change_password 恒为 true（首次登录强制改密，前端/网关拦截）
-- ---------------------------------------------------------------------
INSERT INTO user_svc.users (username, password_hash, real_name, email, phone, status, must_change_password)
VALUES (
    'admin',
    :'admin_password_hash',
    '系统管理员', NULL, NULL,
    -- 仅合法 bcrypt 哈希（$2a$/$2b$/$2y$ 前缀）才直接启用；占位哈希保持 disabled
    CASE WHEN :'admin_password_hash' LIKE '$2a$%'
           OR :'admin_password_hash' LIKE '$2b$%'
           OR :'admin_password_hash' LIKE '$2y$%'
         THEN 'enabled' ELSE 'disabled' END,
    true
)
ON CONFLICT DO NOTHING;

INSERT INTO user_svc.user_roles (user_id, role_id)
SELECT u.user_id, r.role_id
FROM user_svc.users u
JOIN user_svc.roles r ON r.role_code = 'admin'
WHERE u.username = 'admin'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- 5. 测试车辆台账（vehicle_svc.vehicles）
--    用途：数据链路验证（第五章以 HUNTER-001 发送遥测/事件消息）
--    vehicle_id = 设备证书 CommonName = Kafka 消息 key（契约）：正式车端接入前请按实际车辆替换
-- ---------------------------------------------------------------------
INSERT INTO vehicle_svc.vehicles (vehicle_id, vehicle_name, model, firmware_version, software_version,
                                  status, device_cert_sn, description) VALUES
    ('HUNTER-001', 'HUNTER-001 测试车', 'HUNTER_SE', NULL, NULL, 'offline', NULL,
     '部署验证用测试车辆（可删除）；实测车辆须与设备证书 CommonName 一致')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- 6. 待补充（vehicle-service / user-service 契约落地后按同一来源补录，禁止自行发明编码）
--    vehicle 资源域权限点：vehicle:create / vehicle:read / vehicle:update / vehicle:delete / vehicle:execute
--    user    资源域权限点：user:create / user:read / user:update / user:delete / user:execute
-- ---------------------------------------------------------------------

-- 校验（人工执行）：
--   SELECT role_code, count(*) FROM user_svc.role_permissions rp
--     JOIN user_svc.roles r USING (role_id) GROUP BY 1 ORDER BY 1;
--   SELECT username, status, create_time FROM user_svc.users;
--   SELECT vehicle_id, status FROM vehicle_svc.vehicles;

