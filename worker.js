/**
 * Telegram 私聊中转机器人 (Cloudflare Workers / Service Worker 语法)
 *
 * 用户在私聊中发送消息 -> 机器人转发给主管理员;
 * 主管理员对转发消息「回复」-> 机器人把内容转回对应用户。
 *
 * 运行依赖:
 *   - KV namespace 绑定,变量名固定为 `nfd`
 *   - 环境变量见下方「配置区」(兼容新旧命名)
 */

// ========================= 配置区 =========================

/**
 * 安全读取全局环境变量。Service Worker 模式下 vars/secrets 注入为全局变量;
 * 通过 globalThis 访问,未配置时返回 undefined 而不抛 ReferenceError,从而兼容新旧命名。
 */
function envOf(...names) {
  for (const n of names) {
    try {
      const v = globalThis[n];
      if (v !== undefined && v !== null && v !== '') return v;
    } catch (_) { /* ignore */ }
  }
  return undefined;
}

function intEnv(name, def) {
  const v = parseInt(envOf(name), 10);
  return Number.isFinite(v) && v > 0 ? v : def;
}

function normalizeWebhookPath(p) {
  const s = String(p || '').trim();
  if (!s) return '/endpoint';
  return s.startsWith('/') ? s : '/' + s;
}

const BOT_VERSION = '2.1.0';

const TOKEN = envOf('BOT_TOKEN', 'ENV_BOT_TOKEN');
const SECRET = envOf('BOT_SECRET', 'ENV_BOT_SECRET');
// HTTP 管理接口密钥;未单独配置时回退使用 BOT_SECRET
const ADMIN_SECRET = envOf('ADMIN_SECRET') || SECRET;

// 管理员 UID 列表(逗号分隔支持多管理员)。第一个为「主管理员」,接收转发并持有回复映射。
const ADMIN_UIDS = String(envOf('ADMIN_UID', 'ENV_ADMIN_UID') || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
const PRIMARY_ADMIN_UID = ADMIN_UIDS[0] || '';

const WEBHOOK = normalizeWebhookPath(envOf('WEBHOOK_PATH') || '/endpoint');
const ADMIN_PATH = String(envOf('ADMIN_PATH') || 'admin_path').replace(/^\/+|\/+$/g, '');

const DEFAULT_BLOCKLIST_URL = envOf('DEFAULT_BLOCKLIST_URL')
  || 'https://raw.githubusercontent.com/Tsaihyun/hyunbot/refs/heads/main/data/blocklist.txt';

const START_MSG_ZH_URL = envOf('START_MSG_ZH_URL');
const START_MSG_EN_URL = envOf('START_MSG_EN_URL');

// 频道订阅验证（可选）。配置后启用「订阅频道 + 问答」双重验证；未配置则仅问答（向后兼容）。
// REQUIRED_CHANNEL 为 @username 或 -100... 数字 ID；Bot 必须是该频道的管理员，否则无法查询成员。
const REQUIRED_CHANNEL = envOf('REQUIRED_CHANNEL');
const REQUIRED_CHANNEL_TITLE = envOf('REQUIRED_CHANNEL_TITLE') || REQUIRED_CHANNEL || '';
const CHANNEL_GATE_ENABLED = !!REQUIRED_CHANNEL;
// 加入链接：显式配置优先；@username 自动推导；私有频道（数字 ID）无链接则不显示「加入」按钮。
const CHANNEL_JOIN_URL = envOf('REQUIRED_CHANNEL_URL')
  || (typeof REQUIRED_CHANNEL === 'string' && REQUIRED_CHANNEL.startsWith('@')
    ? `https://t.me/${REQUIRED_CHANNEL.slice(1)}`
    : '');
const CHANNEL_WARN_KEY = 'chan-check-warn';
const CHANNEL_WARN_INTERVAL_MS = 10 * 60 * 1000;
// 待订阅用户标记 TTL(秒):用户加入频道后由 chat_member 更新自动推进验证
const PENDING_CHANNEL_TTL_SECONDS = 3600;

// 验证通过有效期
const VERIFIED_TTL_SECONDS = intEnv('VERIFIED_TTL_SECONDS', 3 * 60 * 60); // 默认 3 小时
const VERIFIED_TTL_MS = VERIFIED_TTL_SECONDS * 1000;

// 用户消息频率限制(固定窗口)
const RATE_LIMIT_MESSAGE = intEnv('RATE_LIMIT_MESSAGE', 45);
const RATE_LIMIT_WINDOW_SECONDS = intEnv('RATE_LIMIT_WINDOW_SECONDS', 60);

// 行为开关
const NOTIFY_INTERVAL = 3600 * 1000;
const ENABLE_INSTANT_CONFIRM = false;
const ENABLE_NOTIFICATION = true;
const ENABLE_KEYWORD_FILTER = true;

// 验证出题后作答有效期
const VERIFY_QUESTION_TTL_MS = 10 * 60 * 1000;

// 远程词表刷新间隔
const BLOCKLIST_REFRESH_MS = 15 * 60 * 1000;

// KV 键 TTL(秒),防止键无限增长
const MSG_MAP_TTL_SECONDS = 30 * 24 * 3600;   // 转发映射保留 30 天
const SESSION_TTL_SECONDS = 30 * 24 * 3600;   // 会话标识保留 30 天
const STARTMSG_CACHE_TTL_SECONDS = 3600;      // 欢迎语缓存 1 小时

// KV 键名
const KEYWORD_STORE_KEY = 'kw-list';
const REMOTE_CACHE_KEY = 'blocked-words-cache';
const REMOTE_ETAG_KEY = 'blocked-words-etag';
const REMOTE_LASTFETCH_KEY = 'blocked-words-lastfetch';
const VERIFY_STORE_KEY = (uid) => `verify-${uid}`;
const CHAT_SESSION_KEY = (uid) => `chat-session-${uid}`;
const RATE_KEY = (uid) => `rate-${uid}`;
const PENDING_CHANNEL_KEY = (uid) => `pending-chan-${uid}`;
const START_MSG_CACHE_KEY = (isZh) => isZh ? 'startmsg-zh' : 'startmsg-en';

// ========================= 文案 =========================

const ADMIN_START_MSG_ZH = '👋 您好,管理员!此机器人正在正常运行。';
const ADMIN_START_MSG_EN = '👋 Hello, Admin! The bot is running normally.';
const DEFAULT_START_MSG_ZH = '👋 你好!我是私聊中转助手,会把你的消息转发给管理员,并把管理员的回复带回给你。\n\n请直接发送你的消息即可。';
const DEFAULT_START_MSG_EN = "👋 Hi! I'm a relay assistant. I'll forward your messages to the admin and bring their replies back to you.\n\nJust send your message.";

const VERIFY_REQUIRED_ZH = '🛡 为了防止骚扰,请先完成一次验证。\n\n❓ ';
const VERIFY_REQUIRED_EN = '🛡 To prevent spam, please complete a quick verification.\n\n❓ ';
const VERIFY_PICK_ZH = '\n\n👇 请点击下方正确答案';
const VERIFY_PICK_EN = '\n\n👇 Please tap the correct answer below';
const VERIFIED_SUCCESS_ZH = '✅ 验证通过!现在您可以正常发送消息了。';
const VERIFIED_SUCCESS_EN = '✅ Verified! You can now send messages normally.';
const VERIFY_USE_BUTTONS_ZH = '👆 请点击上方问题的选项按钮完成验证。';
const VERIFY_USE_BUTTONS_EN = '👆 Please tap one of the option buttons above to complete verification.';
const VERIFY_CB_WRONG_ZH = '❌ 答案错误,换一题';
const VERIFY_CB_WRONG_EN = '❌ Wrong answer, here is another one';
const VERIFY_CB_OK_ZH = '✅ 验证通过';
const VERIFY_CB_OK_EN = '✅ Verified';
const VERIFY_EXPIRED_CB_ZH = '⚠️ 验证已过期,请重新发送 /start';
const VERIFY_EXPIRED_CB_EN = '⚠️ Verification expired. Please send /start again';
const SESSION_EXPIRED_ZH = '⚠️ 会话已过期或被清除,请使用 /start 重新开始。';
const SESSION_EXPIRED_EN = '⚠️ Session expired or cleared. Please use /start to begin again.';
const ADMIN_REPLY_PROMPT_ZH = '🙅 请点击**转发的用户消息**进行回复,这样我才能知道您是想回复哪位用户。直接发送消息我无法识别目标用户。';
const ADMIN_REPLY_PROMPT_EN = '🙅 Please click **reply to the forwarded user message** so I know which user you want to reply to. I cannot identify the target user if you send a message directly.';
const USER_BLOCKED_PROMPT_ZH = '🚫 您已被管理员屏蔽,无法发送消息。';
const USER_BLOCKED_PROMPT_EN = '🚫 You have been blocked by the administrator and cannot send messages.';
const MESSAGE_FORWARD_FAIL_PROMPT_ZH = '抱歉,您的消息未能成功转发给管理员,请稍后再试或联系管理员。';
const MESSAGE_FORWARD_FAIL_PROMPT_EN = 'Sorry, your message could not be forwarded to the administrator. Please try again later or contact the administrator.';
const MESSAGE_FORWARDED_NOTIF_ZH = '🔔 您好,您的消息已转发给管理员,请耐心等待回复。如长时间未收到答复,可适当再次留言。';
const MESSAGE_FORWARDED_NOTIF_EN = "🔔 Hello, your message has been forwarded to the administrator. Please wait patiently for a reply. If there's no response for a long time, feel free to send another message.";
const MESSAGE_FORWARDED_OK_ZH = '💬 您的消息已成功转发,管理员将尽快回复您。';
const MESSAGE_FORWARDED_OK_EN = '💬 Your message has been successfully forwarded. The admin will reply soon.';
const USER_UNBLOCKED_PROMPT_ZH = '🎉 您已被管理员解除屏蔽,现在可以正常发送消息了。';
const USER_UNBLOCKED_PROMPT_EN = '🎉 You have been unblocked by the administrator. You can now send messages normally.';
const ADMIN_BLOCK_SELF_PROMPT_ZH = '⚠️ 不能屏蔽自己!';
const ADMIN_BLOCK_SELF_PROMPT_EN = '⚠️ You cannot block yourself!';
const ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH = '❌ 无法识别要操作的用户。请确保您回复的是用户转发给您的消息,或在命令后追加用户ID。';
const ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN = '❌ Cannot identify the user to operate on. Reply to a forwarded message, or append a user ID to the command.';
const ADMIN_CANNOT_FIND_USER_ID_PROMPT_ZH = '⚠️ 无法找到对应的用户ID。可能是旧的转发消息或非转发消息。请检查。';
const ADMIN_CANNOT_FIND_USER_ID_PROMPT_EN = '⚠️ Cannot find the corresponding user ID. This may be an old forwarded message or a non-forwarded message. Please check.';
const USER_KEYWORD_BLOCKED_PROMPT_ZH = '⚠️ 您的消息包含被屏蔽的关键词,未被转发给管理员。';
const USER_KEYWORD_BLOCKED_PROMPT_EN = '⚠️ Your message contains blocked keywords and was not forwarded to the admin.';
const RATE_LIMITED_ZH = '⏳ 您发送得太频繁了,请稍后再试。';
const RATE_LIMITED_EN = '⏳ You are sending messages too frequently. Please try again later.';
const CHANNEL_REQUIRED_ZH = (title) => `📢 请先订阅频道 ${title} 后再联系管理员。\n\n订阅完成后请点击下方按钮完成验证。`;
const CHANNEL_REQUIRED_EN = (title) => `📢 Please subscribe to the channel ${title} before contacting the admin.\n\nAfter subscribing, tap the button below to verify.`;
const CHANNEL_BTN_JOIN_ZH = '📢 加入频道';
const CHANNEL_BTN_JOIN_EN = '📢 Join channel';
const CHANNEL_BTN_CHECK_ZH = '✅ 我已加入,点此验证';
const CHANNEL_BTN_CHECK_EN = "✅ I've joined, verify";
const CHANNEL_CB_OK_ZH = '✅ 已确认订阅';
const CHANNEL_CB_OK_EN = '✅ Subscription confirmed';
const CHANNEL_CB_FAIL_ZH = '❌ 未检测到订阅,请先加入频道后再试。';
const CHANNEL_CB_FAIL_EN = '❌ Subscription not detected. Please join the channel first.';
const CHANNEL_AUTO_OK_ZH = '✅ 已检测到您加入频道,请完成最后一步验证:';
const CHANNEL_AUTO_OK_EN = "✅ Detected that you've joined the channel. One last step:";
const CHANNEL_CHECK_FAIL_ADMIN = (ch, err) => `❗频道订阅校验失败,已暂时放行用户到问答验证。\n请确认 Bot 是频道 ${ch} 的管理员。\n${String(err || '')}`;

const ADMIN_KEYWORD_ADDED_ZH = kw => `✅ 已添加屏蔽关键词:\`${kw}\``;
const ADMIN_KEYWORD_ADDED_EN = kw => `✅ Added blocked keyword: \`${kw}\``;
const ADMIN_KEYWORD_REMOVED_ZH = kw => `✅ 已移除屏蔽关键词:\`${kw}\``;
const ADMIN_KEYWORD_REMOVED_EN = kw => `✅ Removed blocked keyword: \`${kw}\``;
const ADMIN_KEYWORD_LIST_TITLE_ZH = '📃 当前屏蔽关键词列表:';
const ADMIN_KEYWORD_LIST_TITLE_EN = '📃 Current blocked keywords:';
const ADMIN_KEYWORD_EMPTY_ZH = '(空)尚未添加任何关键词。';
const ADMIN_KEYWORD_EMPTY_EN = '(empty) no keywords yet.';
const ADMIN_KEYWORD_USAGE_ZH = '用法:/addkw 关键词 ｜ /rmkw 关键词 ｜ /listkw';
const ADMIN_KEYWORD_USAGE_EN = 'Usage: /addkw <keyword> | /rmkw <keyword> | /listkw';
const ADMIN_BLOCKLIST_RELOADED_ZH = (source, updated, count, url = undefined) => `✅ 词表已刷新(${source}${updated ? ', 已更新' : ''})。` + (url ? `\n🌐 远程地址:${url}` : '') + `\n📦 当前共 ${count} 条。`;
const ADMIN_BLOCKLIST_RELOADED_EN = (source, updated, count, url = undefined) => `✅ Blocklist refreshed (${source}${updated ? ', updated' : ''}).` + (url ? `\n🌐 Remote URL: ${url}` : '') + `\n📦 Now ${count} items.`;
const ADMIN_BLOCKLIST_REMOTE_TITLE_ZH = (total, shown) => `🌐 远程词表信息:共 ${total} 条,前 ${shown} 条:`;
const ADMIN_BLOCKLIST_REMOTE_TITLE_EN = (total, shown) => `🌐 Remote blocklist: total ${total}, first ${shown}:`;
const ADMIN_BLOCKLIST_ALL_TITLE_ZH = (total, shown) => `🧩 合并列表(本地 + 远程):共 ${total} 条,前 ${shown} 条:`;
const ADMIN_BLOCKLIST_ALL_TITLE_EN = (total, shown) => `🧩 Merged list (local + remote): total ${total}, first ${shown}:`;
const ADMIN_KV_ERROR_ZH = (ctx, err) => `❌ KV操作失败(${ctx}):\n\`${String(err?.message || err)}\``;
const ADMIN_KV_ERROR_EN = (ctx, err) => `❌ KV operation failed (${ctx}):\n\`${String(err?.message || err)}\``;
const USER_TEMP_ERROR_ZH = '⚠️ 系统临时故障,请稍后再试。';
const USER_TEMP_ERROR_EN = '⚠️ Temporary system issue, please try again later.';
const DELIVERY_FAIL_ZH = (uid) => `⚠️ 消息未能送达用户 \`${uid}\`(对方可能已停用或拉黑机器人)。`;
const DELIVERY_FAIL_EN = (uid) => `⚠️ Failed to deliver the message to user \`${uid}\` (they may have blocked or stopped the bot).`;
const RESET_DONE_ZH = (uid) => `✅ 已重置用户 \`${uid}\` 的验证状态。`;
const RESET_DONE_EN = (uid) => `✅ Verification state reset for user \`${uid}\`.`;

const HELP_TEXT_ZH = [
  '🛠 *管理员命令*',
  '',
  '*以下命令可回复转发消息,或在命令后追加用户ID:*',
  '`/block [uid]` 屏蔽用户',
  '`/unblock [uid]` 解除屏蔽',
  '`/checkblock [uid]` 查询屏蔽状态',
  '`/reset [uid]` 重置验证状态',
  '`/info [uid]` 查看用户信息',
  '',
  '*关键词:*',
  '`/addkw <词>` 添加本地关键词',
  '`/rmkw <词>` 移除本地关键词',
  '`/listkw` 查看本地关键词',
  '`/listkw_remote` 查看远程词表(前100条)',
  '`/listkw_all` 查看合并词表(前100条)',
  '`/reloadblock` 强制刷新远程词表',
  '',
  '*其他:*',
  '`/resetnotify [uid]` 清理等待提醒节流',
  '`/version` 查看版本',
  '',
  '💬 直接*回复*用户的转发消息即可把内容发回该用户。'
].join('\n');
const HELP_TEXT_EN = [
  '🛠 *Admin commands*',
  '',
  '*These work by replying to a forwarded message, or by appending a user ID:*',
  '`/block [uid]` block a user',
  '`/unblock [uid]` unblock a user',
  '`/checkblock [uid]` check block status',
  '`/reset [uid]` reset verification state',
  '`/info [uid]` show user info',
  '',
  '*Keywords:*',
  '`/addkw <word>` add a local keyword',
  '`/rmkw <word>` remove a local keyword',
  '`/listkw` list local keywords',
  '`/listkw_remote` show remote list (first 100)',
  '`/listkw_all` show merged list (first 100)',
  '`/reloadblock` force-refresh the remote list',
  '',
  '*Misc:*',
  '`/resetnotify [uid]` clear the wait-notice throttle',
  '`/version` show version',
  '',
  '💬 Simply *reply* to a forwarded user message to send your reply back to that user.'
].join('\n');

// ========================= 验证题库 =========================

// 每题提供中英文候选项(约定第 0 项为正确答案);出题时打乱顺序并记录正确项的新位置。
const VERIFY_QUESTIONS = [
  { q_zh: '1 + 1 = ?', q_en: '1 + 1 = ?', optsZh: ['2', '3', '4', '1'], optsEn: ['2', '3', '4', '1'], correct: 0 },
  { q_zh: '3 + 5 = ?', q_en: '3 + 5 = ?', optsZh: ['8', '7', '9', '6'], optsEn: ['8', '7', '9', '6'], correct: 0 },
  { q_zh: '10 - 3 = ?', q_en: '10 - 3 = ?', optsZh: ['7', '6', '8', '5'], optsEn: ['7', '6', '8', '5'], correct: 0 },
  { q_zh: '2 × 4 = ?', q_en: '2 × 4 = ?', optsZh: ['8', '6', '9', '10'], optsEn: ['8', '6', '9', '10'], correct: 0 },
  { q_zh: '15 ÷ 3 = ?', q_en: '15 ÷ 3 = ?', optsZh: ['5', '3', '6', '4'], optsEn: ['5', '3', '6', '4'], correct: 0 },
  { q_zh: '一个星期有几天?', q_en: 'How many days in a week?', optsZh: ['7', '5', '6', '8'], optsEn: ['7', '5', '6', '8'], correct: 0 },
  { q_zh: '一年有几个月?', q_en: 'How many months in a year?', optsZh: ['12', '10', '11', '6'], optsEn: ['12', '10', '11', '6'], correct: 0 },
  { q_zh: '太阳从哪个方向升起?', q_en: 'Which direction does the sun rise?', optsZh: ['东', '西', '南', '北'], optsEn: ['east', 'west', 'south', 'north'], correct: 0 },
  { q_zh: '🐱 是什么动物?', q_en: 'What animal is 🐱?', optsZh: ['猫', '狗', '鸟', '鱼'], optsEn: ['cat', 'dog', 'bird', 'fish'], correct: 0 },
  { q_zh: '1 小时有多少分钟?', q_en: 'How many minutes in 1 hour?', optsZh: ['60', '30', '100', '90'], optsEn: ['60', '30', '100', '90'], correct: 0 }
];

/** Fisher-Yates 原地洗牌。 */
function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

/** 随机出题:按语言取候选项、打乱顺序,返回题面、选项与正确项打乱后的位置。 */
function buildVerifyQuestion(lang) {
  const q = VERIFY_QUESTIONS[Math.floor(Math.random() * VERIFY_QUESTIONS.length)];
  const isZh = !!(lang && lang.startsWith('zh'));
  const options = (isZh ? q.optsZh : q.optsEn).slice();
  const correctVal = options[q.correct];
  shuffle(options);
  return {
    id: Math.random().toString(36).slice(2, 10),
    question: isZh ? q.q_zh : q.q_en,
    options,
    correctIndex: options.indexOf(correctVal)
  };
}

// ========================= Telegram API 封装 =========================

function apiUrl(method, params = null) {
  let query = '';
  if (params) query = '?' + new URLSearchParams(params).toString();
  return `https://api.telegram.org/bot${TOKEN}/${method}${query}`;
}

function makeReqBody(body) {
  return {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  };
}

async function requestTelegram(method, body, params = null) {
  try {
    const response = await fetch(apiUrl(method, params), makeReqBody(body));
    if (!response.ok) {
      const errorBody = await response.text().catch(() => '');
      console.error(`Telegram API请求失败 (${method}): ${response.status} ${response.statusText}`, errorBody);
      return { ok: false, description: `API请求失败: ${response.status} ${response.statusText}`, errorDetails: errorBody };
    }
    return response.json();
  } catch (error) {
    console.error(`执行 ${method} 方法时发生Fetch错误:`, error);
    return { ok: false, description: `网络或未知错误: ${error.message}` };
  }
}

const sendMessage = (msg) => requestTelegram('sendMessage', msg);
const copyMessage = (msg) => requestTelegram('copyMessage', msg);
const forwardMessage = (msg) => requestTelegram('forwardMessage', msg);
const answerCallbackQuery = (msg) => requestTelegram('answerCallbackQuery', msg);
const editMessageText = (msg) => requestTelegram('editMessageText', msg);

function setMyCommands(commands, scope = null) {
  const body = { commands };
  if (scope && Object.keys(scope).length > 0) body.scope = scope;
  return requestTelegram('setMyCommands', body);
}

function setWebhook(url, secret_token, opts = {}) {
  return requestTelegram('setWebhook', {
    url,
    secret_token,
    allowed_updates: opts.allowed_updates || ['message', 'callback_query', 'chat_member'],
    drop_pending_updates: !!opts.drop_pending_updates
  });
}

// ========================= 鉴权与通用工具 =========================

/** 定长时间字符串比较,降低时序侧信道风险。 */
function timingSafeEqual(a, b) {
  a = String(a);
  b = String(b);
  if (a.length !== b.length) return false;
  let result = 0;
  for (let i = 0; i < a.length; i++) result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return result === 0;
}

function isAdminRequestAuthorized(request) {
  if (!ADMIN_SECRET) return false;
  const auth = request.headers.get('Authorization') || '';
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (m && timingSafeEqual(m[1].trim(), ADMIN_SECRET)) return true;
  const x = request.headers.get('X-Admin-Secret');
  if (x && timingSafeEqual(x.trim(), ADMIN_SECRET)) return true;
  return false;
}

function isAdminId(id) {
  return id !== undefined && id !== null && ADMIN_UIDS.includes(String(id));
}

function getLocalizedPrompt(langCode, prompts) {
  if (langCode && langCode.startsWith('zh')) return prompts.zh;
  return prompts.en;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function formatUserForAdmin(u) {
  const id = u?.id;
  const uname = u?.username;
  const name = [u?.first_name, u?.last_name].filter(Boolean).join(' ') || 'user';
  if (uname) return `@${uname}`;
  if (id) return `<a href="tg://user?id=${id}">${escapeHtml(name)}</a>`;
  return escapeHtml(name);
}

// ========================= HTTP 入口与路由 =========================

addEventListener('fetch', event => {
  event.respondWith(handleRequest(event));
});

function methodNotAllowed(allow) {
  return new Response('Method Not Allowed', { status: 405, headers: { Allow: allow } });
}

async function handleRequest(event) {
  const request = event.request;
  const url = new URL(request.url);
  const path = url.pathname;

  // Telegram webhook
  if (path === WEBHOOK) {
    return handleWebhook(event);
  }

  // 管理 HTTP 接口:/{ADMIN_PATH}/<action>,全部需要鉴权
  const adminPrefix = `/${ADMIN_PATH}/`;
  if (path.startsWith(adminPrefix)) {
    if (!isAdminRequestAuthorized(request)) {
      return new Response('Unauthorized', { status: 401, headers: { 'WWW-Authenticate': 'Bearer' } });
    }
    const action = path.slice(adminPrefix.length);
    const method = request.method.toUpperCase();
    switch (action) {
      case 'registerWebhook':
        if (method !== 'POST') return methodNotAllowed('POST');
        return registerWebhook(url);
      case 'unRegisterWebhook':
        if (method !== 'POST') return methodNotAllowed('POST');
        return unRegisterWebhook();
      case 'setMenu':
        if (method !== 'POST') return methodNotAllowed('POST');
        return handleSetMenu();
      case 'debugWebhook':
        return debugWebhook();
      default:
        return new Response('Not found', { status: 404 });
    }
  }

  return new Response('请求路径未找到处理程序', { status: 404 });
}

async function handleWebhook(event) {
  if (event.request.method !== 'POST') {
    return methodNotAllowed('POST');
  }
  if (event.request.headers.get('X-Telegram-Bot-Api-Secret-Token') !== SECRET) {
    return new Response('未经授权', { status: 403 });
  }
  try {
    const update = await event.request.json();
    event.waitUntil(onUpdate(update));
    return new Response('Ok');
  } catch (error) {
    console.error('解析Webhook更新数据时出错:', error);
    return new Response('错误请求,JSON解析失败', { status: 400 });
  }
}

async function onUpdate(update) {
  try {
    if (update && 'message' in update) {
      await onMessage(update.message);
    } else if (update && 'callback_query' in update) {
      await onCallbackQuery(update.callback_query);
    } else if (update && 'chat_member' in update) {
      await onChatMemberUpdate(update.chat_member);
    }
  } catch (err) {
    console.error('处理 update 时发生未捕获错误:', err);
    try {
      if (PRIMARY_ADMIN_UID) {
        await sendMessage({ chat_id: parseInt(PRIMARY_ADMIN_UID), text: `❗机器人处理消息时发生错误:\n${String(err?.message || err)}` });
      }
    } catch (_) { /* ignore */ }
  }
}

// ========================= KV 工具 =========================

async function kvPutJson(key, value, opts = {}) {
  await nfd.put(key, JSON.stringify(value), opts);
}

async function notifyAdminKvError(lang, context, error, target = PRIMARY_ADMIN_UID) {
  const text = getLocalizedPrompt(lang, { zh: ADMIN_KV_ERROR_ZH(context, error), en: ADMIN_KV_ERROR_EN(context, error) });
  try {
    await sendMessage({ chat_id: parseInt(target), text, parse_mode: 'Markdown' });
  } catch (e) {
    console.error('通知管理员KV错误时再次失败:', e);
  }
}

// ========================= 关键词过滤 =========================

async function loadKeywordsLocal() {
  const arr = await nfd.get(KEYWORD_STORE_KEY, { type: 'json' });
  return Array.isArray(arr) ? arr : [];
}

async function saveKeywords(list) {
  const cleaned = Array.from(new Set(list.map(s => String(s || '').trim()).filter(Boolean)));
  await kvPutJson(KEYWORD_STORE_KEY, cleaned);
  return cleaned;
}

async function addKeyword(kw) {
  const list = await loadKeywordsLocal();
  list.push(kw);
  return saveKeywords(list);
}

async function removeKeyword(kw) {
  const list = await loadKeywordsLocal();
  const lowered = String(kw).toLowerCase();
  const filtered = list.filter(x => String(x).toLowerCase() !== lowered);
  return saveKeywords(filtered);
}

function extractSearchableText(message) {
  const segs = [];
  if (typeof message.text === 'string') segs.push(message.text);
  if (typeof message.caption === 'string') segs.push(message.caption);
  return segs.join('\n').trim();
}

function hitBlockedKeyword(text, keywords) {
  if (!text) return null;
  const low = text.toLowerCase();
  for (const kw of keywords) {
    const k = String(kw || '').trim().toLowerCase();
    if (!k) continue;
    if (/^[a-z0-9]+$/.test(k)) {
      // 纯英文/数字关键词:用单词边界,避免 "av" 误伤 "available"
      const re = new RegExp(`\\b${escapeRegExp(k)}\\b`, 'i');
      if (re.test(low)) return kw;
    } else if (low.includes(k)) {
      // 含中文或符号的关键词:子串匹配
      return kw;
    }
  }
  return null;
}

// ========================= 远程词表 =========================

function parseBlocklist(text) {
  const trimmed = (text || '').trim();
  if (!trimmed) return [];
  if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
    try {
      const data = JSON.parse(trimmed);
      if (Array.isArray(data)) return data.map(s => String(s).trim()).filter(Boolean);
      if (data && Array.isArray(data.words)) return data.words.map(s => String(s).trim()).filter(Boolean);
    } catch (_) { /* 非 JSON,按行解析 */ }
  }
  return trimmed.split(/\r?\n/).map(l => l.trim()).filter(l => l && !l.startsWith('#'));
}

async function getRemoteCachedWords() {
  try {
    const txt = await nfd.get(REMOTE_CACHE_KEY, { type: 'text' });
    if (!txt) return [];
    const obj = JSON.parse(txt);
    if (obj && Array.isArray(obj.words)) return obj.words;
  } catch (_) { /* ignore */ }
  return [];
}

async function saveRemoteCache(words) {
  const payload = { words, updatedAt: Date.now() };
  await nfd.put(REMOTE_CACHE_KEY, JSON.stringify(payload));
  await nfd.put(REMOTE_LASTFETCH_KEY, String(payload.updatedAt));
}

async function fetchRemoteBlocklist({ force = false } = {}) {
  const url = DEFAULT_BLOCKLIST_URL;
  const lastFetchTxt = await nfd.get(REMOTE_LASTFETCH_KEY, { type: 'text' });
  const lastFetch = lastFetchTxt ? parseInt(lastFetchTxt, 10) : 0;
  if (!force && lastFetch && (Date.now() - lastFetch) < BLOCKLIST_REFRESH_MS) {
    const words = await getRemoteCachedWords();
    return { words, updated: false, source: 'cache-fresh', url };
  }

  const etag = await nfd.get(REMOTE_ETAG_KEY, { type: 'text' });
  const headers = {};
  if (etag) headers['If-None-Match'] = etag;

  let res;
  try {
    res = await fetch(url, { headers });
  } catch (e) {
    const words = await getRemoteCachedWords();
    return { words, updated: false, source: 'cache-fallback', url };
  }

  if (res.status === 304) {
    await nfd.put(REMOTE_LASTFETCH_KEY, String(Date.now()));
    const words = await getRemoteCachedWords();
    return { words, updated: false, source: 'not-modified', url };
  }
  if (!res.ok) {
    const words = await getRemoteCachedWords();
    return { words, updated: false, source: 'cache-on-error', url };
  }

  const text = await res.text();
  const words = parseBlocklist(text);
  await saveRemoteCache(words);
  const newEtag = res.headers.get('ETag');
  if (newEtag) await nfd.put(REMOTE_ETAG_KEY, newEtag);
  return { words, updated: true, source: 'remote', url };
}

async function getBlockedWordsRemote({ force = false } = {}) {
  const { words } = await fetchRemoteBlocklist({ force });
  return words;
}

async function getAllBlockedWords() {
  const local = await loadKeywordsLocal();
  const remote = await getBlockedWordsRemote();
  const set = new Set(local.map(x => String(x).toLowerCase()));
  for (const w of remote) set.add(String(w).toLowerCase());
  return Array.from(set);
}

// ========================= 会话与验证 =========================

async function initChatSession(userId) {
  const sessionId = Math.random().toString(36).slice(2, 15);
  await kvPutJson(CHAT_SESSION_KEY(userId), { sessionId, startedAt: Date.now() }, { expirationTtl: SESSION_TTL_SECONDS });
  return sessionId;
}

async function validateChatSession(userId) {
  return nfd.get(CHAT_SESSION_KEY(userId), { type: 'json' }).catch(() => null);
}

async function clearUserVerification(userId) {
  await nfd.delete(VERIFY_STORE_KEY(userId)).catch(() => {});
}

/** 验证记录是否「已通过且仍在有效期内」(可选校验 sessionId 一致)。 */
function isVerificationFresh(state, sessionId = null) {
  if (!state || state.verified !== true || !state.verifiedAt) return false;
  if ((Date.now() - state.verifiedAt) > VERIFIED_TTL_MS) return false;
  if (sessionId !== null && state.sessionId && state.sessionId !== sessionId) return false;
  return true;
}

/** 由出题数据构造 inline 选项键盘(每行两个按钮)。callback_data: vrf:<questionId>:<index>。 */
function buildVerifyKeyboard(question) {
  const buttons = question.options.map((opt, idx) => ({ text: String(opt), callback_data: `vrf:${question.id}:${idx}` }));
  const rows = [];
  for (let i = 0; i < buttons.length; i += 2) rows.push(buttons.slice(i, i + 2));
  return { inline_keyboard: rows };
}

/** 验证题面文案:提示 + 题目 + 点击提示。 */
function verifyQuestionText(question, lang) {
  const promptText = getLocalizedPrompt(lang, { zh: VERIFY_REQUIRED_ZH, en: VERIFY_REQUIRED_EN });
  const pickPrompt = getLocalizedPrompt(lang, { zh: VERIFY_PICK_ZH, en: VERIFY_PICK_EN });
  return `${promptText}${question.question}${pickPrompt}`;
}

/** 生成一道题并写入 KV(待答状态),返回 question 供发送/编辑消息使用。 */
async function issueVerifyQuestion(userId, lang, sessionId) {
  const question = buildVerifyQuestion(lang);
  await kvPutJson(VERIFY_STORE_KEY(userId), {
    sessionId,
    questionId: question.id,
    correctIndex: question.correctIndex,
    options: question.options,
    question: question.question,
    exp: Date.now() + VERIFY_QUESTION_TTL_MS,
    verified: false,
    verifiedAt: null,
    waitingAnswer: true
  }, { expirationTtl: Math.max(60, VERIFIED_TTL_SECONDS) });
  return question;
}

async function ensureVerified(userId, lang) {
  const session = await validateChatSession(userId);
  if (!session) {
    const expiredText = getLocalizedPrompt(lang, { zh: SESSION_EXPIRED_ZH, en: SESSION_EXPIRED_EN });
    await sendMessage({ chat_id: userId, text: expiredText });
    return false;
  }

  const state = await nfd.get(VERIFY_STORE_KEY(userId), { type: 'json' }).catch(() => null);
  if (isVerificationFresh(state, session.sessionId)) {
    return true;
  }

  const question = await issueVerifyQuestion(userId, lang, session.sessionId);
  await sendMessage({
    chat_id: userId,
    text: verifyQuestionText(question, lang),
    reply_markup: buildVerifyKeyboard(question)
  });
  return false;
}

// ========================= 频率限制 =========================

async function checkRateLimit(chatId) {
  if (RATE_LIMIT_MESSAGE <= 0) return { limited: false };
  const key = RATE_KEY(chatId);
  const now = Date.now();
  const windowMs = RATE_LIMIT_WINDOW_SECONDS * 1000;

  let data = null;
  try { data = await nfd.get(key, { type: 'json' }); } catch (_) { /* ignore */ }
  if (!data || typeof data.start !== 'number' || (now - data.start) >= windowMs) {
    data = { start: now, count: 0 };
  }
  data.count += 1;
  const limited = data.count > RATE_LIMIT_MESSAGE;
  try {
    await nfd.put(key, JSON.stringify(data), { expirationTtl: Math.max(60, RATE_LIMIT_WINDOW_SECONDS) });
  } catch (_) { /* ignore */ }
  return { limited, count: data.count };
}

// ========================= 欢迎语 =========================

async function getStartMessage(lang) {
  const isZh = !!(lang && lang.startsWith('zh'));
  const url = isZh ? START_MSG_ZH_URL : START_MSG_EN_URL;
  const fallback = isZh ? DEFAULT_START_MSG_ZH : DEFAULT_START_MSG_EN;
  if (!url) return fallback;

  const cacheKey = START_MSG_CACHE_KEY(isZh);
  try {
    const cached = await nfd.get(cacheKey, { type: 'json' });
    if (cached && cached.text && (Date.now() - cached.at) < STARTMSG_CACHE_TTL_SECONDS * 1000) {
      return cached.text;
    }
  } catch (_) { /* ignore */ }

  try {
    const res = await fetch(url);
    if (res.ok) {
      const text = await res.text();
      if (text && text.trim()) {
        await nfd.put(cacheKey, JSON.stringify({ text, at: Date.now() }), { expirationTtl: STARTMSG_CACHE_TTL_SECONDS }).catch(() => {});
        return text;
      }
    }
  } catch (_) { /* 远程失败,回退 */ }

  try {
    const cached = await nfd.get(cacheKey, { type: 'json' });
    if (cached && cached.text) return cached.text;
  } catch (_) { /* ignore */ }
  return fallback;
}

// ========================= 频道订阅门 =========================

/** 查询用户是否已订阅指定频道。Bot 须为该频道管理员。 */
async function isUserSubscribed(userId) {
  if (!CHANNEL_GATE_ENABLED) return { subscribed: true, disabled: true };
  const res = await requestTelegram('getChatMember', { chat_id: REQUIRED_CHANNEL, user_id: parseInt(userId) });
  if (!res || !res.ok) {
    return { subscribed: false, checkFailed: true, error: res?.description };
  }
  const status = res.result?.status;
  const subscribed = ['creator', 'administrator', 'member'].includes(status)
    || (status === 'restricted' && res.result?.is_member === true);
  return { subscribed, status };
}

/** 发送「请先订阅频道」提示 + Inline 按钮。 */
async function sendChannelGate(chatId, lang) {
  // 标记为「待订阅」,使用户加入频道后可由 chat_member 更新自动推进验证
  await kvPutJson(PENDING_CHANNEL_KEY(chatId), { lang, at: Date.now() }, { expirationTtl: PENDING_CHANNEL_TTL_SECONDS }).catch(() => {});
  const title = REQUIRED_CHANNEL_TITLE || REQUIRED_CHANNEL;
  const text = getLocalizedPrompt(lang, { zh: CHANNEL_REQUIRED_ZH(title), en: CHANNEL_REQUIRED_EN(title) });
  const rows = [];
  if (CHANNEL_JOIN_URL) {
    rows.push([{ text: getLocalizedPrompt(lang, { zh: CHANNEL_BTN_JOIN_ZH, en: CHANNEL_BTN_JOIN_EN }), url: CHANNEL_JOIN_URL }]);
  }
  rows.push([{ text: getLocalizedPrompt(lang, { zh: CHANNEL_BTN_CHECK_ZH, en: CHANNEL_BTN_CHECK_EN }), callback_data: 'check_sub' }]);
  await sendMessage({ chat_id: chatId, text, reply_markup: { inline_keyboard: rows } });
}

/** 频道校验失败时向主管理员告警（带节流，避免刷屏）。 */
async function maybeWarnChannelCheck(error) {
  try {
    const last = await nfd.get(CHANNEL_WARN_KEY, { type: 'text' });
    const t = last ? parseInt(last, 10) : 0;
    if (t && (Date.now() - t) < CHANNEL_WARN_INTERVAL_MS) return;
    await nfd.put(CHANNEL_WARN_KEY, String(Date.now()), { expirationTtl: Math.max(60, Math.ceil(CHANNEL_WARN_INTERVAL_MS / 1000)) });
  } catch (_) { /* ignore */ }
  if (PRIMARY_ADMIN_UID) {
    try {
      await sendMessage({ chat_id: parseInt(PRIMARY_ADMIN_UID), text: CHANNEL_CHECK_FAIL_ADMIN(REQUIRED_CHANNEL, error) });
    } catch (_) { /* ignore */ }
  }
}

/** 比对 chat_member 更新里的 chat 是否为强制订阅频道(兼容 @username 与 -100… 数字 ID)。 */
function isRequiredChannelChat(chat) {
  if (!chat) return false;
  const req = String(REQUIRED_CHANNEL || '');
  if (!req) return false;
  if (req.startsWith('@')) {
    return !!chat.username && ('@' + chat.username).toLowerCase() === req.toLowerCase();
  }
  return String(chat.id) === req;
}

/** 处理频道成员变化:目标频道内「待订阅」用户加入后,自动推进到问答验证(无需点按钮)。 */
async function onChatMemberUpdate(cm) {
  if (!CHANNEL_GATE_ENABLED || !cm) return;
  if (!isRequiredChannelChat(cm.chat)) return;

  const user = cm.new_chat_member?.user;
  const userId = user?.id;
  if (!userId || isAdminId(userId)) return;

  const status = cm.new_chat_member?.status;
  const joined = ['member', 'administrator', 'creator'].includes(status)
    || (status === 'restricted' && cm.new_chat_member?.is_member === true);
  if (!joined) return;

  // 仅对正在等待订阅的私聊用户自动推进,避免处理频道的全部成员变动
  const pending = await nfd.get(PENDING_CHANNEL_KEY(userId), { type: 'json' }).catch(() => null);
  if (!pending) return;
  await nfd.delete(PENDING_CHANNEL_KEY(userId)).catch(() => {});

  const lang = pending.lang || user.language_code || 'en';
  const session = await validateChatSession(userId);
  if (!session) await initChatSession(userId);

  await sendMessage({ chat_id: userId, text: getLocalizedPrompt(lang, { zh: CHANNEL_AUTO_OK_ZH, en: CHANNEL_AUTO_OK_EN }) });
  const verified = await ensureVerified(userId, lang);
  if (verified) {
    await sendMessage({ chat_id: userId, text: getLocalizedPrompt(lang, { zh: VERIFIED_SUCCESS_ZH, en: VERIFIED_SUCCESS_EN }) });
  }
}

// ========================= 消息处理入口 =========================

async function onMessage(message) {
  if (!message || !message.chat) return;
  const chatId = message.chat.id;
  const fromId = message.from?.id;
  const isAdmin = isAdminId(fromId);
  const lang = message.from?.language_code || 'en';
  const text = message.text || '';

  // /start
  if (text === '/start') {
    if (isAdmin) {
      const adminMsg = getLocalizedPrompt(lang, { zh: ADMIN_START_MSG_ZH, en: ADMIN_START_MSG_EN });
      await sendMessage({ chat_id: chatId, text: adminMsg });
    } else {
      // 已通过验证且仍在有效期内 → 不重置会话/验证,直接回欢迎语,避免重复答题
      const existingSession = await validateChatSession(chatId);
      if (existingSession) {
        const vstate = await nfd.get(VERIFY_STORE_KEY(chatId), { type: 'json' }).catch(() => null);
        if (isVerificationFresh(vstate, existingSession.sessionId)) {
          await sendMessage({ chat_id: chatId, text: await getStartMessage(lang) });
          return;
        }
      }
      // 首次 / 会话过期 / 验证过期 → 完整初始化并进入验证流程
      await initChatSession(chatId);
      await clearUserVerification(chatId);
      const welcome = await getStartMessage(lang);
      await sendMessage({ chat_id: chatId, text: welcome });
      if (CHANNEL_GATE_ENABLED) {
        const sub = await isUserSubscribed(chatId);
        if (sub.checkFailed) {
          await maybeWarnChannelCheck(sub.error);
          await ensureVerified(chatId, lang);
        } else if (!sub.subscribed) {
          await sendChannelGate(chatId, lang);
        } else {
          await ensureVerified(chatId, lang);
        }
      } else {
        await ensureVerified(chatId, lang);
      }
    }
    return;
  }

  if (isAdmin) {
    await onAdminMessage(message, lang);
    return;
  }

  await handleGuestMessage(message, lang);
}

// ========================= 回调查询处理 =========================

async function onCallbackQuery(cbq) {
  const userId = cbq.from?.id;
  const lang = cbq.from?.language_code || 'en';
  const data = cbq.data || '';

  // 问答验证:点击选项按钮作答
  if (data.startsWith('vrf:')) {
    await handleVerifyCallback(cbq, lang);
    return;
  }

  if (data === 'check_sub') {
    if (isAdminId(userId)) {
      await answerCallbackQuery({ callback_query_id: cbq.id });
      return;
    }
    const sub = await isUserSubscribed(userId);
    if (sub.checkFailed) await maybeWarnChannelCheck(sub.error);
    if (sub.subscribed || sub.checkFailed) {
      // 订阅已确认（或校验降级放行）→ 进入问答验证
      await answerCallbackQuery({ callback_query_id: cbq.id, text: getLocalizedPrompt(lang, { zh: CHANNEL_CB_OK_ZH, en: CHANNEL_CB_OK_EN }) });
      await nfd.delete(PENDING_CHANNEL_KEY(userId)).catch(() => {}); // 手动按钮路径:清理待订阅标记
      const session = await validateChatSession(userId);
      if (!session) await initChatSession(userId);
      const verified = await ensureVerified(userId, lang);
      if (verified) {
        await sendMessage({ chat_id: userId, text: getLocalizedPrompt(lang, { zh: VERIFIED_SUCCESS_ZH, en: VERIFIED_SUCCESS_EN }) });
      }
    } else {
      await answerCallbackQuery({ callback_query_id: cbq.id, text: getLocalizedPrompt(lang, { zh: CHANNEL_CB_FAIL_ZH, en: CHANNEL_CB_FAIL_EN }), show_alert: true });
    }
    return;
  }

  await answerCallbackQuery({ callback_query_id: cbq.id });
}

/** 处理问答选项点击:校验题目有效性、判分,通过则编辑为成功提示,答错则换一题。 */
async function handleVerifyCallback(cbq, lang) {
  const userId = cbq.from?.id;
  const msg = cbq.message;
  const chatId = msg?.chat?.id;
  const messageId = msg?.message_id;
  const parts = (cbq.data || '').split(':');
  const qid = parts[1];
  const choiceIndex = parseInt(parts[2], 10);

  const session = await validateChatSession(userId);
  const state = await nfd.get(VERIFY_STORE_KEY(userId), { type: 'json' }).catch(() => null);

  // 题目失效:会话不符 / 非当前题 / 已答过 / 过期
  const stale = !session || !state || !state.waitingAnswer || state.verified
    || state.questionId !== qid || state.sessionId !== session.sessionId
    || (state.exp && Date.now() > state.exp);
  if (stale) {
    await answerCallbackQuery({
      callback_query_id: cbq.id,
      text: getLocalizedPrompt(lang, { zh: VERIFY_EXPIRED_CB_ZH, en: VERIFY_EXPIRED_CB_EN }),
      show_alert: true
    });
    return;
  }

  if (choiceIndex === state.correctIndex) {
    await kvPutJson(VERIFY_STORE_KEY(userId), {
      sessionId: session.sessionId,
      verified: true,
      verifiedAt: Date.now(),
      waitingAnswer: false
    }, { expirationTtl: Math.max(60, VERIFIED_TTL_SECONDS) });
    await answerCallbackQuery({ callback_query_id: cbq.id, text: getLocalizedPrompt(lang, { zh: VERIFY_CB_OK_ZH, en: VERIFY_CB_OK_EN }) });
    const successText = getLocalizedPrompt(lang, { zh: VERIFIED_SUCCESS_ZH, en: VERIFIED_SUCCESS_EN });
    if (chatId && messageId) {
      await editMessageText({ chat_id: chatId, message_id: messageId, text: successText });
    } else {
      await sendMessage({ chat_id: userId, text: successText });
    }
    return;
  }

  // 答错:换一题,防止枚举选项
  await answerCallbackQuery({ callback_query_id: cbq.id, text: getLocalizedPrompt(lang, { zh: VERIFY_CB_WRONG_ZH, en: VERIFY_CB_WRONG_EN }) });
  const question = await issueVerifyQuestion(userId, lang, session.sessionId);
  const text = verifyQuestionText(question, lang);
  const reply_markup = buildVerifyKeyboard(question);
  if (chatId && messageId) {
    await editMessageText({ chat_id: chatId, message_id: messageId, text, reply_markup });
  } else {
    await sendMessage({ chat_id: userId, text, reply_markup });
  }
}

// ========================= 管理员消息处理 =========================

/** 解析命令目标用户:优先显式 uid,其次回复的转发消息映射。 */
async function resolveTargetUserId(message, explicitId) {
  if (explicitId) return String(explicitId).trim();
  if (message.reply_to_message) {
    const g = await nfd.get('msg-map-' + message.reply_to_message.message_id, { type: 'text' }).catch(() => null);
    if (g) return g;
  }
  return null;
}

async function onAdminMessage(message, lang) {
  const adminChat = message.chat.id; // 回执发给发起命令的管理员
  const text = message.text || '';

  if (/^\/help$/i.test(text)) {
    const help = getLocalizedPrompt(lang, { zh: HELP_TEXT_ZH, en: HELP_TEXT_EN });
    await sendMessage({ chat_id: adminChat, text: help, parse_mode: 'Markdown' });
    return;
  }

  if (/^\/addkw(?:\s+(.+))?$/i.test(text)) {
    const m = text.match(/^\/addkw(?:\s+(.+))?$/i);
    const kw = ((m && m[1]) || '').trim();
    if (!kw) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_USAGE_ZH, en: ADMIN_KEYWORD_USAGE_EN }) });
      return;
    }
    try {
      await addKeyword(kw);
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_ADDED_ZH(kw), en: ADMIN_KEYWORD_ADDED_EN(kw) }), parse_mode: 'Markdown' });
    } catch (err) {
      await notifyAdminKvError(lang, 'addKeyword', err, adminChat);
    }
    return;
  }

  if (/^\/rmkw(?:\s+(.+))?$/i.test(text)) {
    const m = text.match(/^\/rmkw(?:\s+(.+))?$/i);
    const kw = ((m && m[1]) || '').trim();
    if (!kw) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_USAGE_ZH, en: ADMIN_KEYWORD_USAGE_EN }) });
      return;
    }
    try {
      await removeKeyword(kw);
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_REMOVED_ZH(kw), en: ADMIN_KEYWORD_REMOVED_EN(kw) }), parse_mode: 'Markdown' });
    } catch (err) {
      await notifyAdminKvError(lang, 'removeKeyword', err, adminChat);
    }
    return;
  }

  if (/^\/listkw$/i.test(text)) {
    try {
      const list = await loadKeywordsLocal();
      const title = getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_LIST_TITLE_ZH, en: ADMIN_KEYWORD_LIST_TITLE_EN });
      const empty = getLocalizedPrompt(lang, { zh: ADMIN_KEYWORD_EMPTY_ZH, en: ADMIN_KEYWORD_EMPTY_EN });
      const body = list.length ? list.map((x, i) => `${i + 1}. \`${x}\``).join('\n') : empty;
      await sendMessage({ chat_id: adminChat, text: `${title}\n${body}`, parse_mode: 'Markdown' });
    } catch (err) {
      await notifyAdminKvError(lang, 'listKeywordsLocal', err, adminChat);
    }
    return;
  }

  if (/^\/reloadblock$/i.test(text)) {
    try {
      const { words, updated, source, url } = await fetchRemoteBlocklist({ force: true });
      const t = getLocalizedPrompt(lang, { zh: ADMIN_BLOCKLIST_RELOADED_ZH(source, updated, words.length, url), en: ADMIN_BLOCKLIST_RELOADED_EN(source, updated, words.length, url) });
      await sendMessage({ chat_id: adminChat, text: t });
    } catch (err) {
      await notifyAdminKvError(lang, 'reloadblock', err, adminChat);
    }
    return;
  }

  if (/^\/listkw_remote$/i.test(text)) {
    try {
      const words = await getBlockedWordsRemote();
      const sample = words.slice(0, 100);
      const t = getLocalizedPrompt(lang, { zh: ADMIN_BLOCKLIST_REMOTE_TITLE_ZH(words.length, sample.length), en: ADMIN_BLOCKLIST_REMOTE_TITLE_EN(words.length, sample.length) }) + '\n' + sample.join(', ');
      await sendMessage({ chat_id: adminChat, text: t });
    } catch (err) {
      await notifyAdminKvError(lang, 'listkw_remote', err, adminChat);
    }
    return;
  }

  if (/^\/listkw_all$/i.test(text)) {
    try {
      const local = await loadKeywordsLocal();
      const remote = await getBlockedWordsRemote();
      const merged = Array.from(new Set([...local.map(String), ...remote.map(String)]));
      const sample = merged.slice(0, 100);
      const t = getLocalizedPrompt(lang, { zh: ADMIN_BLOCKLIST_ALL_TITLE_ZH(merged.length, sample.length), en: ADMIN_BLOCKLIST_ALL_TITLE_EN(merged.length, sample.length) }) + '\n' + sample.join(', ');
      await sendMessage({ chat_id: adminChat, text: t });
    } catch (err) {
      await notifyAdminKvError(lang, 'listkw_all', err, adminChat);
    }
    return;
  }

  if (/^\/version$/i.test(text)) {
    await sendMessage({ chat_id: adminChat, text: `🤖 Bot version: ${BOT_VERSION}` });
    return;
  }

  if (/^\/notifytest$/i.test(text)) {
    const notificationText = getLocalizedPrompt(lang, { zh: MESSAGE_FORWARDED_NOTIF_ZH, en: MESSAGE_FORWARDED_NOTIF_EN });
    await sendMessage({ chat_id: adminChat, text: notificationText });
    return;
  }

  if (/^\/resetnotify(?:\s+(\d+))?$/i.test(text)) {
    const m = text.match(/^\/resetnotify(?:\s+(\d+))?$/i);
    const targetId = await resolveTargetUserId(message, m && m[1]);
    if (!targetId) {
      await sendMessage({ chat_id: adminChat, text: '用法: /resetnotify <userId> 或对转发消息回复 /resetnotify' });
      return;
    }
    try {
      await nfd.delete(`notify:until:${targetId}`);
      await nfd.delete(`notify:last:${targetId}`);
      await nfd.delete(`lastmsg-${targetId}`);
    } catch (_) { /* ignore */ }
    await sendMessage({ chat_id: adminChat, text: `已清理节流键:${targetId}` });
    return;
  }

  // 需要定位目标用户的命令
  let m;
  if ((m = text.match(/^\/block(?:\s+(\d+))?$/i))) { return handleBlock(message, lang, m[1]); }
  if ((m = text.match(/^\/unblock(?:\s+(\d+))?$/i))) { return handleUnblock(message, lang, m[1]); }
  if ((m = text.match(/^\/checkblock(?:\s+(\d+))?$/i))) { return checkBlock(message, lang, m[1]); }
  if ((m = text.match(/^\/reset(?:\s+(\d+))?$/i))) { return handleReset(message, lang, m[1]); }
  if ((m = text.match(/^\/info(?:\s+(\d+))?$/i))) { return handleInfo(message, lang, m[1]); }

  // 非命令:回复转发消息 -> 转回用户;否则提示
  if (message.reply_to_message) {
    try {
      const guestId = await nfd.get('msg-map-' + message.reply_to_message.message_id, { type: 'text' });
      if (guestId) {
        const r = await copyMessage({ chat_id: guestId, from_chat_id: message.chat.id, message_id: message.message_id });
        if (!r || !r.ok) {
          await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: DELIVERY_FAIL_ZH(guestId), en: DELIVERY_FAIL_EN(guestId) }), parse_mode: 'Markdown' });
        }
      } else {
        await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_FIND_USER_ID_PROMPT_ZH, en: ADMIN_CANNOT_FIND_USER_ID_PROMPT_EN }) });
      }
    } catch (err) {
      await notifyAdminKvError(lang, 'admin_reply_lookup_msg_map', err, adminChat);
    }
  } else {
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_REPLY_PROMPT_ZH, en: ADMIN_REPLY_PROMPT_EN }) });
  }
}

// ========================= 访客消息处理 =========================

async function handleGuestMessage(message, lang) {
  const chatId = message.chat.id;

  // 屏蔽检查
  const blocked = await nfd.get(`isblocked-${chatId}`, { type: 'json' }).catch(() => false);
  if (blocked) {
    await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: USER_BLOCKED_PROMPT_ZH, en: USER_BLOCKED_PROMPT_EN }) });
    return;
  }

  // 会话有效性
  const session = await validateChatSession(chatId);
  if (!session) {
    await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: SESSION_EXPIRED_ZH, en: SESSION_EXPIRED_EN }) });
    return;
  }

  // 频道订阅门（每条消息复查）
  if (CHANNEL_GATE_ENABLED) {
    const sub = await isUserSubscribed(chatId);
    if (sub.checkFailed) {
      await maybeWarnChannelCheck(sub.error); // 校验失败：告警并放行到问答（fail-open）
    } else if (!sub.subscribed) {
      await sendChannelGate(chatId, lang);
      return;
    }
  }

  // 验证状态(按钮模式:作答通过 inline 按钮回调完成,这里不再接收文本答案)
  let verifyState = await nfd.get(VERIFY_STORE_KEY(chatId), { type: 'json' }).catch(() => null);
  if (verifyState && verifyState.sessionId !== session.sessionId) {
    await clearUserVerification(chatId);
    verifyState = null;
  }
  if (!isVerificationFresh(verifyState, session.sessionId)) {
    // 未验证:已有未过期的待答题 → 提示点击按钮;否则出新题
    if (verifyState && verifyState.waitingAnswer && verifyState.questionId
        && verifyState.exp && Date.now() < verifyState.exp) {
      await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: VERIFY_USE_BUTTONS_ZH, en: VERIFY_USE_BUTTONS_EN }) });
    } else {
      await ensureVerified(chatId, lang);
    }
    return;
  }

  // 频率限制(仅对通过验证的正常消息计数)
  const rl = await checkRateLimit(chatId);
  if (rl.limited) {
    await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: RATE_LIMITED_ZH, en: RATE_LIMITED_EN }) });
    return;
  }

  // 关键词过滤
  if (ENABLE_KEYWORD_FILTER) {
    try {
      const text = extractSearchableText(message);
      const allWords = await getAllBlockedWords();
      const hit = hitBlockedKeyword(text, allWords);
      if (hit) {
        await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: USER_KEYWORD_BLOCKED_PROMPT_ZH, en: USER_KEYWORD_BLOCKED_PROMPT_EN }) });
        const actor = formatUserForAdmin(message.from || {});
        const adminAlert = getLocalizedPrompt(lang, {
          zh: `⚠️ ${actor} 的消息命中被屏蔽关键词:<code>${escapeHtml(hit)}</code>,已拦截。`,
          en: `⚠️ Message from ${actor} contained blocked keyword: <code>${escapeHtml(hit)}</code> and was intercepted.`
        });
        if (PRIMARY_ADMIN_UID) await sendMessage({ chat_id: parseInt(PRIMARY_ADMIN_UID), text: adminAlert, parse_mode: 'HTML' });
        return;
      }
    } catch (err) {
      const adminDegrade = getLocalizedPrompt(lang, {
        zh: `❗关键词过滤出现故障,已降级直转。\n<code>${escapeHtml(String(err?.message || err))}</code>`,
        en: `❗Keyword filter failed; falling back to forward.\n<code>${escapeHtml(String(err?.message || err))}</code>`
      });
      if (PRIMARY_ADMIN_UID) await sendMessage({ chat_id: parseInt(PRIMARY_ADMIN_UID), text: adminDegrade, parse_mode: 'HTML' });
    }
  }

  // 转发给主管理员
  const forwardResult = await forwardMessage({ chat_id: parseInt(PRIMARY_ADMIN_UID), from_chat_id: chatId, message_id: message.message_id });
  if (forwardResult.ok) {
    await nfd.put('msg-map-' + forwardResult.result.message_id, chatId.toString(), { expirationTtl: MSG_MAP_TTL_SECONDS })
      .catch(err => notifyAdminKvError(lang, 'write_msg_map', err));
    if (ENABLE_INSTANT_CONFIRM) {
      await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: MESSAGE_FORWARDED_OK_ZH, en: MESSAGE_FORWARDED_OK_EN }) });
    }
    await handleNotify(message, lang);
  } else {
    await sendMessage({ chat_id: chatId, text: getLocalizedPrompt(lang, { zh: MESSAGE_FORWARD_FAIL_PROMPT_ZH, en: MESSAGE_FORWARD_FAIL_PROMPT_EN }) });
  }
}

// ========================= 等待提醒节流 =========================

async function handleNotify(message, lang) {
  if (!ENABLE_NOTIFICATION) return;
  const chatId = message.chat.id;
  const now = Date.now();
  const interval = NOTIFY_INTERVAL;
  const keyUntil = `notify:until:${chatId}`;
  const legacyJsonKey = `notify:last:${chatId}`;
  const legacyTextKey = 'lastmsg-' + chatId;

  let until = 0;
  try {
    const obj = await nfd.get(keyUntil, { type: 'json' });
    if (obj && typeof obj.until === 'number' && isFinite(obj.until)) until = obj.until;
  } catch (_) { /* ignore */ }
  if (!until) {
    try {
      const j = await nfd.get(legacyJsonKey, { type: 'json' });
      if (j && typeof j.t === 'number' && isFinite(j.t)) until = j.t + interval;
    } catch (_) { /* ignore */ }
  }
  if (!until) {
    try {
      const s = await nfd.get(legacyTextKey, { type: 'text' });
      const t = s ? parseInt(s, 10) : 0;
      if (t && isFinite(t)) until = t + interval;
    } catch (_) { /* ignore */ }
  }
  if (until && now < until) return;

  try {
    await nfd.put(keyUntil, JSON.stringify({ until: now + interval }), { expirationTtl: Math.max(60, Math.ceil(interval / 1000)) });
  } catch (_) { /* ignore */ }
  const notificationText = getLocalizedPrompt(lang, { zh: MESSAGE_FORWARDED_NOTIF_ZH, en: MESSAGE_FORWARDED_NOTIF_EN });
  await sendMessage({ chat_id: chatId, text: notificationText });
}

// ========================= 用户管理命令 =========================

async function handleBlock(message, lang, explicitId) {
  const adminChat = message.chat.id;
  try {
    const guestId = await resolveTargetUserId(message, explicitId);
    if (!guestId) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH, en: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN }) });
      return;
    }
    if (isAdminId(guestId)) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_BLOCK_SELF_PROMPT_ZH, en: ADMIN_BLOCK_SELF_PROMPT_EN }) });
      return;
    }
    await kvPutJson('isblocked-' + guestId, true).catch(err => notifyAdminKvError(lang, 'block_user', err, adminChat));
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: `✅ 用户 \`${guestId}\` 已被成功屏蔽。`, en: `✅ User \`${guestId}\` has been successfully blocked.` }), parse_mode: 'Markdown' });
    await sendMessage({ chat_id: parseInt(guestId), text: `${USER_BLOCKED_PROMPT_ZH}\n${USER_BLOCKED_PROMPT_EN}` });
  } catch (err) {
    await notifyAdminKvError(lang, 'handleBlock', err, adminChat);
  }
}

async function handleUnblock(message, lang, explicitId) {
  const adminChat = message.chat.id;
  try {
    const guestId = await resolveTargetUserId(message, explicitId);
    if (!guestId) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH, en: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN }) });
      return;
    }
    await nfd.delete('isblocked-' + guestId).catch(err => notifyAdminKvError(lang, 'unblock_user', err, adminChat));
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: `✅ 用户 \`${guestId}\` 已被成功解除屏蔽。`, en: `✅ User \`${guestId}\` has been successfully unblocked.` }), parse_mode: 'Markdown' });
    await sendMessage({ chat_id: parseInt(guestId), text: `${USER_UNBLOCKED_PROMPT_ZH}\n${USER_UNBLOCKED_PROMPT_EN}` });
  } catch (err) {
    await notifyAdminKvError(lang, 'handleUnblock', err, adminChat);
  }
}

async function checkBlock(message, lang, explicitId) {
  const adminChat = message.chat.id;
  try {
    const guestId = await resolveTargetUserId(message, explicitId);
    if (!guestId) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH, en: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN }) });
      return;
    }
    const blocked = await nfd.get('isblocked-' + guestId, { type: 'json' }).catch(err => {
      notifyAdminKvError(lang, 'read_block_state_in_checkBlock', err, adminChat);
      return false;
    });
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: `用户信息:\`${guestId}\` ${blocked ? '已被屏蔽 🚫' : '未被屏蔽 ✅'}`, en: `User Info: \`${guestId}\` ${blocked ? 'is blocked 🚫' : 'is not blocked ✅'}` }), parse_mode: 'Markdown' });
  } catch (err) {
    await notifyAdminKvError(lang, 'checkBlock', err, adminChat);
  }
}

async function handleReset(message, lang, explicitId) {
  const adminChat = message.chat.id;
  try {
    const guestId = await resolveTargetUserId(message, explicitId);
    if (!guestId) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH, en: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN }) });
      return;
    }
    await clearUserVerification(guestId);
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: RESET_DONE_ZH(guestId), en: RESET_DONE_EN(guestId) }), parse_mode: 'Markdown' });
  } catch (err) {
    await notifyAdminKvError(lang, 'handleReset', err, adminChat);
  }
}

async function handleInfo(message, lang, explicitId) {
  const adminChat = message.chat.id;
  try {
    const guestId = await resolveTargetUserId(message, explicitId);
    if (!guestId) {
      await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_ZH, en: ADMIN_CANNOT_IDENTIFY_USER_PROMPT_EN }) });
      return;
    }
    const blocked = await nfd.get('isblocked-' + guestId, { type: 'json' }).catch(() => false);
    const vstate = await nfd.get(VERIFY_STORE_KEY(guestId), { type: 'json' }).catch(() => null);
    const verified = isVerificationFresh(vstate);
    const zh = `👤 用户信息\nID: \`${guestId}\`\n屏蔽: ${blocked ? '是 🚫' : '否 ✅'}\n验证: ${verified ? '已验证 ✅' : '未验证 ❌'}`;
    const en = `👤 User info\nID: \`${guestId}\`\nBlocked: ${blocked ? 'yes 🚫' : 'no ✅'}\nVerified: ${verified ? 'yes ✅' : 'no ❌'}`;
    await sendMessage({ chat_id: adminChat, text: getLocalizedPrompt(lang, { zh, en }), parse_mode: 'Markdown' });
  } catch (err) {
    await notifyAdminKvError(lang, 'handleInfo', err, adminChat);
  }
}

// ========================= Webhook / 菜单 / 调试 =========================

async function registerWebhook(url) {
  const webhookUrl = `${url.protocol}//${url.hostname}${WEBHOOK}`;
  const drop = url.searchParams.get('drop_pending_updates') === 'true';
  const res = await setWebhook(webhookUrl, SECRET, { allowed_updates: ['message', 'callback_query', 'chat_member'], drop_pending_updates: drop });
  return new Response(JSON.stringify(res, null, 2), { headers: { 'Content-Type': 'application/json' } });
}

async function unRegisterWebhook() {
  const res = await setWebhook('', undefined, { drop_pending_updates: false });
  return new Response(JSON.stringify(res, null, 2), { headers: { 'Content-Type': 'application/json' } });
}

async function setBotCommands() {
  const adminCommands = [
    { command: 'help', description: '显示管理员命令帮助' },
    { command: 'block', description: '屏蔽用户 (回复转发消息 或 /block <uid>)' },
    { command: 'unblock', description: '解除屏蔽 (回复 或 /unblock <uid>)' },
    { command: 'checkblock', description: '查询屏蔽状态 (回复 或 /checkblock <uid>)' },
    { command: 'reset', description: '重置用户验证 (回复 或 /reset <uid>)' },
    { command: 'info', description: '查看用户信息 (回复 或 /info <uid>)' },
    { command: 'addkw', description: '添加屏蔽关键词' },
    { command: 'rmkw', description: '移除屏蔽关键词' },
    { command: 'listkw', description: '查看本地关键词' },
    { command: 'reloadblock', description: '刷新远程拦截词' },
    { command: 'listkw_all', description: '查看合并关键词预览' },
    { command: 'resetnotify', description: '清理用户等待提醒节流' }
  ];
  const userCommands = [
    { command: 'start', description: '获取关于此机器人的信息' }
  ];

  const userRes = await setMyCommands(userCommands);
  if (!userRes.ok) console.error('设置用户命令失败:', userRes);

  let adminRes = { ok: true };
  for (const uid of ADMIN_UIDS) {
    const scope = { type: 'chat', chat_id: parseInt(uid) };
    const r = await setMyCommands(adminCommands, scope);
    if (!r.ok) {
      console.error(`设置管理员命令失败 (${uid}):`, r);
      adminRes = r;
    }
  }
  return { userCommandsSet: userRes.ok, adminCommandsSet: adminRes.ok, adminResponse: adminRes };
}

async function handleSetMenu() {
  const res = await setBotCommands();
  return new Response(JSON.stringify(res, null, 2), { headers: { 'Content-Type': 'application/json' } });
}

async function debugWebhook() {
  const r = await fetch(`https://api.telegram.org/bot${TOKEN}/getWebhookInfo`);
  const j = await r.json();
  return new Response(JSON.stringify(j, null, 2), { headers: { 'Content-Type': 'application/json' } });
}
