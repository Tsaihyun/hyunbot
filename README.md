


# 轻量Telegram群聊私信机器人

## 📖 项目简介

一个基于 Cloudflare Workers 的 Telegram 双向私信机器人。用户私聊机器人发消息，管理员在群组话题中一对一回复。

---

## 🏗️ 部署教程

### 1. 准备工作

| 需要 | 获取方式 |
|------|---------|
| Cloudflare 账号 | [注册](https://dash.cloudflare.com/sign-up) |
| 机器人 Token | [@BotFather](https://t.me/BotFather) 发送 `/newbot` |
| 你的用户 ID | [@userinfobot](https://t.me/userinfobot) 发送 `/start` |
| 一个话题群组 | Telegram 创建并开启话题模式 |

### 2. 创建话题群组

```
1. Telegram 新建群组
2. 群组设置 → 话题 → 启用话题
3. 将机器人拉入群组并设为管理员
4. 权限至少开启：管理话题、发送消息、删除消息
```

### 3. 获取群组 ID

```
方法：把机器人拉进群组后，浏览器访问：
https://api.telegram.org/bot你的TOKEN/getUpdates
在返回内容中找到 "chat":{"id":-100xxxxxxxxx}
```

### 4. Cloudflare 部署


#### 4.1 创建 KV 命名空间
```
Workers & Pages → KV → 创建命名空间
名称：nfd
```

#### 4.2 创建 Worker
```
Workers & Pages → 创建 → 创建 Worker
名称随意，粘贴代码，点击部署
```

#### 4.3 绑定 KV
```
Worker 设置 → 变量 → KV 命名空间绑定
变量名：nfd
命名空间：nfd
```

#### 4.4 设置环境变量
```
Worker 设置 → 变量 → 环境变量

BOT_TOKEN              你的机器人Token
BOT_SECRET             自定义密钥（纯字母数字，如 tsaihyun）
SUPERGROUP_ID          群组ID（-100开头）
ADMIN_UID              你的用户ID
```

#### 4.5 注册 Webhook
```
浏览器访问：
https://你的worker域名/registerWebhook

看到 {"ok":true,"result":true} 即成功
```

---

## 👤 用户使用说明

### 开始使用
```
1. 搜索机器人，发送 /start
2. 看到欢迎消息
3. 点击验证按钮完成验证
4. 直接发送消息，等待管理员回复
```

### 注意事项
- 首次发消息需要点击验证
- 验证有效期 30 天
- 每分钟最多 45 条消息
- 被屏蔽后无法发送消息

---

## 👨‍💼 管理员使用说明

### 群组结构
```
群组话题列表：
├── 📋 使用说明    
├── [U0001] 张三   ← 用户1的对话
├── [U0002] 李四   ← 用户2的对话
```

### 使用命令（在📋使用说明话题执行）

| 命令 | 功能 |
|------|------|
| `/reloadblock` | 刷新远程敏感词库 |
| `/help` | 查看使用说明 |

### 用户话题命令（在用户话题内执行）

| 命令 | 功能 |
|------|------|
| `/close` | 关闭对话，用户无法发消息 |
| `/open` | 重新打开对话 |
| `/block` | 屏蔽用户 |
| `/unblock` | 解除屏蔽 |
| `/reset` | 重置验证状态 |
| `/retopic` | 重建用户话题 |
| `/info` | 查看用户详细信息 |

### 回复用户
```
进入用户话题 → 直接发送消息 → 自动转发给用户
```

---

## ⚙️ 配置说明

### 欢迎消息
```
修改代码中的链接为你自己的：
START_MSG_ZH_URL: '你的中文欢迎消息URL'
START_MSG_EN_URL: '你的英文欢迎消息URL'
```

### 敏感词库
```
修改代码中的链接：
DEFAULT_BLOCKLIST_URL: '你的词库URL'

词库格式（每行一个词）：
广告
spam
诈骗
```

### 验证有效期
```
VERIFIED_EXPIRE_SECONDS: 2592000  // 30天
```

### 通知间隔
```
NOTIFY_INTERVAL: 3600000  // 1小时
```

---

## 🔄 日常维护

| 操作 | 方法 |
|------|------|
| 重新注册 | 访问 `/registerWebhook` |
| 取消注册 | 访问 `/unRegisterWebhook` |
| 重建说明 | 访问 `/init` |
| 查看日志 | Cloudflare → Worker → 日志 |
| 更新代码 | 编辑 Worker 代码 → 部署 |

---

## ❓ 常见问题

**Q：管理命令不生效？**
必须在📋使用说明话题中执行 `/reloadblock` 和 `/help`，用户命令如 `/close` `/block` 等必须在用户话题中执行。

**Q：机器人不响应？**
重新注册 Webhook，检查环境变量是否正确。

**Q：如何添加管理员？**
在环境变量中修改 `ADMIN_UID`，或在 KV 中手动修改 `admin-list` 键值。

**Q：用户消息被拦截？**
检查远程词库内容，或在📋使用说明话题执行 `/reloadblock` 刷新。

**Q：话题丢失？**
在用户话题执行 `/retopic` 重建。

---

## 📂 项目结构

```
轻量Telegram群聊私信机器人
├── Cloudflare Worker（主程序）
├── Cloudflare KV（数据存储）
├── Telegram Bot（消息接口）
└── Telegram Group（管理端）
```

---

## 🛠️ 技术栈

- Cloudflare Workers
- Cloudflare KV
- Telegram Bot API
- JavaScript

---





如遇问题，请检查 Cloudflare Worker 日志获取详细错误信息。
