# Relay 消息转发平台

Go + Vue 3 + PostgreSQL。独立接入 Kafka、RabbitMQ，将消息构造为目标所需的 HTTP 请求，支持签名、可信管理员脚本、持久化重试、失败补投与管理控制台。

## 启动

依赖：Go ≥1.26、Node.js ≥24、PostgreSQL ≥17。Docker 可选。

```bash
# 构建前端，嵌入后端程序
cd web
npm ci
npm run build
cd ..
go build -trimpath -o bin/relay ./cmd/relay

# 分别生成管理员令牌和加密主密钥；保存主密钥用于后续恢复
export RELAY_ADMIN_TOKEN="$(openssl rand -hex 24)"
export RELAY_MASTER_KEY="$(openssl rand -hex 32)"
export DATABASE_URL='postgres://relay:password@localhost:5432/relay?sslmode=disable'
export APP_PREFIX=relay
./bin/relay
```

页面：`http://127.0.0.1:8080/relay/`。API：`/api/relay/v1/`。使用管理员令牌登录；CLI 可携带 `Authorization: Bearer ...`。令牌最少24字符，主密钥必须是64位十六进制。前端无需单独运行 Node 服务。

macOS 本地开发也可运行 `./scripts/dev.sh`：使用 Homebrew PostgreSQL 17或指定的 `PG_BIN`，在项目 `.local/` 创建仅本地监听的开发数据库和私密配置。当前工作区下载的 Go 位于 `.tools/go/bin/go`，脚本会自动识别。令牌和主密钥在 `.local/dev.env`，不会提交。

Docker：复制 `.env.example` 为 `.env`，填写随机令牌、主密钥、数据库密码，然后 `docker compose up --build -d`。数据库密码在示例DSN中直接拼接，建议使用随机十六进制值；其他特殊字符须正确URL编码。当前环境没有Docker，Compose文件提供为部署入口，需在目标Docker环境验证。

## 最短验证流程

1. 创建 `manual` 消息源并启用。
2. 创建 Webhook 目标，配置 URL、请求格式和成功条件并启用。
3. 创建转发规则，选择源 ID 和目标 ID 并启用。
4. 在“请求调试”预览请求；选择手动消息源接收样例，触发真实转发。
5. 查看“投递记录”和“详情”；终止任务可补投，等待任务可取消。

配置编辑器采用 JSON，示例见 [配置说明](docs/配置与接口说明.md)。页面列表显示短 ID，编辑对应对象可复制完整 ID。保存即生效，暂停优先作为日常停用方式。目标编辑不改变已经接收任务的快照。

## 已实现

- Kafka 独立 `relay.*` 消费组，手动位点提交、事务已提交读取、位点失效报错、SASL Plain/SCRAM、TLS。
- RabbitMQ `relay.*` 专用队列，手动ACK、预取限制、重连、可声明并绑定具备容量上限的Quorum队列。
- 每条消息和所有目标任务同事务写入，失败回滚，落库后才确认源端；源位置去重保留30天。
- raw字节透传、Go模板JSON/XML/表单/文本、自定义脚本请求、静态与动态Header/路径/Query。
- Basic/Bearer/API Key、OAuth2 client_credentials、HMAC-SHA256、RSA-SHA256；可组合鉴权链，签名可放Header或Query；支持mTLS和自定义CA。
- 脚本可用受绑定密钥保护的HMAC宿主函数，将签名放在Body或自定义结构。响应可用状态/JSON条件或脚本判定。
- 外部连接器RPC可接入任意语言的专有SDK；平台验证返回请求仍指向授权目标，连接器受相同出站策略和请求期限限制。示例见examples/connector。
- 持久化任务租约、退避抖动、Retry-After、次数与年龄限制、失败补投、未知结果记录。
- 单活调度选举，目标级并发/QPS、轮转公平调度、失败熔断、Worker完成唤醒；多副本不会各自获得完整额度。
- 登录会话、同源写请求检查、加密凭证、审计、SSRF拨号校验、禁止重定向、正文大小限制。
- 页面、资源、API、健康检查使用统一前缀；同一构建产物可切换前缀。
- 7天终态任务清理，未完成引用不清理；默认10GiB数据库预算检查，达到上限停止新接入。

## 运行边界

- 这是可运行的核心版本，不等同于设计文档全部扩展能力都已交付。尚未内置各厂商SDK连接器（可按已实现的外部连接器协议接入）、Avro/Protobuf Schema服务、异步业务回执、严格顺序Lane、OIDC/多角色多租户、独立远程Worker、批量重放作业和历史配置回滚界面。
- 首期每条消息在同一事务直接完成有界扇出；没有单独的FanoutJob服务。筛选/映射错误会让来源重试并显示ERROR，原消息不被错误确认，需要修正规则。
- 页面JSON配置是当前产品配置，**不是设计文档中拟定DSL的直接实现**。实现字段和模板语法以配置说明为准。
- `RELAY_ENABLE_TRUSTED_SCRIPTS=true` 仅对可信管理员开放脚本。独立持久子进程、超时、输出和调用栈限制已实现；`GOMEMLIMIT`是软目标，不是每脚本硬内存沙箱。不可信多租户脚本上线前必须增加OS/容器硬隔离。
- 外部业务效果至少一次；目标未提供幂等时可能重复。RabbitMQ未启用Message ID去重，ACK丢失可能重复接入，不按内容哈希误去重。
- Kafka/RabbitMQ的消费语义隔离不代表共享Broker零资源影响；需配置Broker ACL、配额和容量。RabbitMQ溢出策略必须显式选择，`drop-head`丢弃平台副本，`reject-publish`可能影响发布者确认。
- 超过1MiB的消息会阻止确认并触发来源重试。源码/请求体/响应体均有大小边界，不承诺无上限处理。
- 任务顺序默认不保证，不能用于要求严格串行的目标。
- 当前调度是单活，HTTP并发在本进程执行；扩容不同接入/API副本可用，但不能宣称投递吞吐会随实例线性增长。
- 限流按目标设置；多个目标指向同一对方服务时，需要自行分配总额度或使用统一出站限流网关。单活租约接管仍可能存在远端未知请求，不能代替目标幂等。
- DB预算每5秒检查一次，是软背压，可能短暂超出；需要独立数据库、磁盘预留、告警和磁盘配额。默认保留窗口不是无期限归档。
- 尚未完成生产容量SLA验证、跨节点数据库灾备演练和Docker运行验证，不承诺某个TPS或“资源最佳”。

## 配置

| 环境变量 | 默认值/含义 |
|---|---|
| DATABASE_URL | 必填；建议独立数据库 |
| RELAY_ADMIN_TOKEN | 必填；至少24字符 |
| RELAY_MASTER_KEY | 必填；32字节十六进制主密钥，必须持久保存 |
| APP_PREFIX | relay；单个非空路径段 |
| RELAY_LISTEN | 127.0.0.1:8080 |
| RELAY_ROLE | all / api / ingest / dispatcher |
| RELAY_MAX_INFLIGHT | 256；范围1..4096 |
| RELAY_ALLOWED_CIDRS | 默认空；显式允许的内网目标CIDR，目标仍须allow_private=true |
| RELAY_ENABLE_TRUSTED_SCRIPTS | false |
| RELAY_SECURE_COOKIE | HTTPS代理后设true，代理需保留Host |
| RELAY_STORAGE_BUDGET_BYTES | 10737418240，数据库总体软预算 |

API和ingest分进程时，来源运行状态当前仅在接入进程内维护；优先使用all模式查看完整运行状态。多API实例的会话未共享，使用粘性会话或CLI Bearer认证。生产需要共享会话/身份服务再横向扩展浏览器会话。

## 验证

```bash
# 单元测试与静态检查
go test -race ./...
go vet ./...
cd web && npm test && npm run build && npm audit && cd ..

# 真实持久化与Broker独立消费测试（不提供环境变量时相应测试会明确跳过）
TEST_DATABASE_URL='postgres://relay:password@localhost:5432/relay?sslmode=disable' \
TEST_KAFKA_BROKER='localhost:9092' \
TEST_AMQP_URL='amqp://localhost:5672/' \
go test -race -count=1 -timeout=180s -v ./...

# 已启动本地应用后运行浏览器测试；令牌来自环境或.local/dev.env
cd web
RELAY_BROWSER_URL=http://127.0.0.1:18080 APP_PREFIX=bridge npx playwright test
cd ..

# 已启动本地应用、允许127.0.0.0/8时运行真实投递基准
APP_PREFIX=bridge RELAY_LOAD_URL=http://127.0.0.1:18080 node scripts/load.mjs
```

测试会创建随机测试Schema、Topic、专用队列，结束后清理。Broker测试需要创建/删除测试对象权限，只对测试环境执行。浏览器与负载脚本会创建有明确测试名称的来源/目标/规则，不向外部接口发送测试消息。

详细设计：[原始设计方案](docs/消息中间件Webhook转发平台-详细设计方案.md)。运行验证记录另见 [实现与验证记录](docs/实现与验证记录.md)。
