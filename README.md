# Telegram 私聊转发与防骚扰机器人

运行在 Cloudflare Workers 上的 Telegram 私聊中转机器人。用户在私聊中给机器人发消息,机器人会把消息转发给（主）管理员；管理员在机器人会话里对**转发过来的那条消息进行「回复」**，机器人再把回复内容转回对应用户。

回复定位基于「转发消息 → 用户」的 KV 映射，不依赖 `forward_from`，因此即使用户开启了「转发时隐藏账户」也能正常回复。

## 功能

- 私聊消息自动转发给主管理员
- 管理员通过「回复转发消息」直接回复用户
- 双重人机验证：频道订阅（可选，加入后自动放行）+ 四选项按钮问答；问答有效期可配置，默认 3 小时
- 用户屏蔽 / 解除屏蔽 / 查询状态 / 重置验证 / 查看信息
- 本地关键词 + 远程关键词词表过滤（远程词表支持 ETag 缓存与降级回退）
- 用户消息频率限制（固定窗口）与等待提醒节流
- 多管理员（`ADMIN_UID` 逗号分隔）
- 可自定义 `/start` 欢迎语（可选远程加载）
- Webhook 注册、注销、调试和命令菜单设置
- 管理 HTTP 接口强制鉴权

## 环境变量

| 变量名 | 必填 | 说明 |
| --- | --- | --- |
| `BOT_TOKEN` | 是 | Telegram Bot Token。兼容旧名 `ENV_BOT_TOKEN` |
| `BOT_SECRET` | 是 | Telegram webhook secret token。兼容旧名 `ENV_BOT_SECRET` |
| `ADMIN_UID` | 是 | 管理员 Telegram 用户 ID。逗号分隔可配置多个，**第一个为主管理员（接收转发）**。兼容旧名 `ENV_ADMIN_UID` |
| `ADMIN_SECRET` | 建议 | HTTP 管理接口密钥。未设置时回退使用 `BOT_SECRET` |
| `ADMIN_PATH` | 否 | HTTP 管理接口路径前缀，默认 `admin_path` |
| `WEBHOOK_PATH` | 否 | Telegram webhook 路径，默认 `/endpoint` |
| `DEFAULT_BLOCKLIST_URL` | 否 | 远程关键词词表地址 |
| `VERIFIED_TTL_SECONDS` | 否 | 验证通过有效期，默认 `10800` 秒（3 小时） |
| `RATE_LIMIT_MESSAGE` | 否 | 限流窗口内允许的用户消息数，默认 `45` |
| `RATE_LIMIT_WINDOW_SECONDS` | 否 | 限流窗口秒数，默认 `60` |
| `START_MSG_ZH_URL` | 否 | 中文 `/start` 欢迎语远程地址；未设置时使用内置默认文案 |
| `START_MSG_EN_URL` | 否 | 英文 `/start` 欢迎语远程地址；未设置时使用内置默认文案 |
| `REQUIRED_CHANNEL` | 否 | 强制订阅频道：`@username` 或 `-100…` 数字 ID。**设置后启用「订阅 + 问答」双重验证**，不设则仅问答。Bot 须为该频道管理员 |
| `REQUIRED_CHANNEL_URL` | 否 | 频道加入链接。`@username` 会自动推导；私有频道需显式提供，否则不显示「加入」按钮 |
| `REQUIRED_CHANNEL_TITLE` | 否 | 频道显示名称，用于提示文案，默认使用 `REQUIRED_CHANNEL` 的值 |

还需要绑定一个 KV namespace，绑定变量名固定为 `nfd`。

> 提示：仓库 `data/` 下的 `startMessage.zh.md` / `startMessage.en.md` 可作为欢迎语模板。把它们部署到任意可公开访问的 URL（如 GitHub Raw），再配置 `START_MSG_*_URL` 即可启用；否则机器人使用内置默认欢迎语。

## 部署步骤

1. 在 Cloudflare Workers 新建 Worker。
2. 粘贴或部署 `worker.js`。
3. 绑定 KV namespace，变量名设为 `nfd`。
4. 设置上面的环境变量。
5. （可选）若设置了 `REQUIRED_CHANNEL`，请把 Bot 加入该频道并设为管理员，否则无法校验订阅状态。
6. 使用管理接口注册 webhook。

管理接口都在 `ADMIN_PATH` 前缀下，且必须带鉴权头：

```bash
# 注册 webhook（POST）
curl -X POST "https://你的域名/admin_path/registerWebhook" \
  -H "Authorization: Bearer 你的_ADMIN_SECRET"

# 查看 webhook 状态（GET）
curl "https://你的域名/admin_path/debugWebhook" \
  -H "Authorization: Bearer 你的_ADMIN_SECRET"

# 设置 Telegram 命令菜单（POST）
curl -X POST "https://你的域名/admin_path/setMenu" \
  -H "Authorization: Bearer 你的_ADMIN_SECRET"

# 注销 webhook（POST）
curl -X POST "https://你的域名/admin_path/unRegisterWebhook" \
  -H "Authorization: Bearer 你的_ADMIN_SECRET"
```

如果你修改了 `ADMIN_PATH`，把上面 URL 中的 `admin_path` 替换为实际值。鉴权头也可以用 `X-Admin-Secret: 你的_ADMIN_SECRET` 代替 `Authorization`。

注册 webhook 时默认**不会**丢弃待处理 update；如确实需要，可在注册 URL 后追加 `?drop_pending_updates=true`。

## 管理员命令

以下命令可在与机器人的私聊中执行。涉及具体用户的命令，既可以**回复某条转发消息**，也可以在命令后**追加用户 ID**。

| 命令 | 说明 |
| --- | --- |
| `/help` | 显示管理员命令帮助 |
| `/block [uid]` | 屏蔽用户 |
| `/unblock [uid]` | 解除屏蔽 |
| `/checkblock [uid]` | 查询屏蔽状态 |
| `/reset [uid]` | 重置用户验证状态 |
| `/info [uid]` | 查看用户信息（ID / 屏蔽 / 验证状态） |
| `/addkw <关键词>` | 添加本地关键词 |
| `/rmkw <关键词>` | 删除本地关键词 |
| `/listkw` | 查看本地关键词 |
| `/listkw_remote` | 查看远程关键词前 100 条 |
| `/listkw_all` | 查看本地和远程合并关键词前 100 条 |
| `/reloadblock` | 强制刷新远程关键词词表 |
| `/resetnotify [uid]` | 清理某个用户的等待提醒节流 |
| `/version` | 查看机器人版本 |
| `/notifytest` | 预览用户等待提醒文案 |

示例：

```text
/block 123456789
/resetnotify 123456789
```

直接**回复**用户的转发消息（不带命令）即可把内容发回该用户。

## 多管理员说明

`ADMIN_UID` 支持用逗号分隔配置多个管理员，例如 `111111,222222`。

- 所有管理员都能执行上述命令，命令回执会发回执行命令的那位管理员。
- 但用户消息只会转发给**主管理员**（列表中的第一个），因此「回复用户」只能由主管理员完成（其余管理员的会话里没有对应的转发消息）。

## 验证流程

新用户首次发言需通过验证后，消息才会转发给管理员：

1. **频道订阅（可选）**：若配置了 `REQUIRED_CHANNEL`，机器人会先检查用户是否已订阅该频道。未订阅则发送带「加入频道 / 我已加入」按钮的提示。**用户加入频道后，机器人会自动检测并推进到问答验证**（依赖 Telegram 的 `chat_member` 更新），无需手动点按钮；「我已加入，点此验证」按钮仍保留作为兜底。**订阅状态在每条消息时都会复查**，退订会被立即拦截。
2. **随机问答**：订阅确认后（或未启用频道验证时），机器人发送一道随机问题并给出**四个候选选项按钮**，用户点击作答即可；答错会自动换一题。通过后在有效期内（`VERIFIED_TTL_SECONDS`，默认 3 小时）无需重复验证。

补充说明：

- **已通过验证的用户再次发送 `/start` 不会被要求重新答题**，机器人只回欢迎语；仅在首次使用、会话过期或验证过期时才重新验证。
- 未配置 `REQUIRED_CHANNEL` 时退化为纯问答验证。
- 自动检测加入需要 Bot 是频道管理员，且 webhook 已开启 `chat_member` 更新（见下方升级提醒）。
- 若机器人不是频道管理员导致校验失败，会放行用户到问答阶段并向主管理员告警（fail-open，避免因配置问题误锁所有用户）。
- 管理员本人豁免验证。

> ⚠️ **升级提醒**：本版本的 webhook `allowed_updates` 新增了 `chat_member`（用于「加入频道后自动验证」）。从旧版升级后，请**重新注册 webhook**（`POST /<ADMIN_PATH>/registerWebhook`）使其生效，否则自动检测不会触发；活跃的大频道会因此增加 Worker 请求量。

## 远程词表格式

远程词表是普通文本文件，每行一个关键词。空行和以 `#` 开头的行会被忽略。也支持 JSON 数组（`["a","b"]`）或 `{"words": ["a","b"]}` 格式。

```text
# 注释
spam
广告
```

纯英文 / 数字关键词按**单词边界**匹配（如 `av` 不会误伤 `available`）；含中文或符号的关键词按子串匹配。

## 安全说明

- Telegram webhook 路径只接受 `POST`，并校验 `X-Telegram-Bot-Api-Secret-Token`。
- 管理 HTTP 接口需要 `Authorization: Bearer <ADMIN_SECRET>` 或 `X-Admin-Secret`，且写操作（register / unRegister / setMenu）只接受 `POST`。
- 默认不会在注册 webhook 时丢弃待处理 update；如确实需要，可在注册 URL 后追加 `?drop_pending_updates=true`。
- 不建议把 `ADMIN_SECRET` 放进 URL 查询参数，避免被日志记录。
