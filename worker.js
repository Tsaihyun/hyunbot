export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        
        const CONFIG = {
            TOKEN: env.BOT_TOKEN,
            WEBHOOK: '/endpoint',
            SECRET: env.BOT_SECRET,
            SUPERGROUP_ID: String(env.SUPERGROUP_ID),
            ADMIN_UID: String(env.ADMIN_UID),
            ADMIN_LIST_KEY: 'admin-list',
            USER_TAG_COUNTER_KEY: 'user-tag-counter',
            ADMIN_TOPIC_KEY: 'admin-topic-id',
            VERIFY_EXPIRE_SECONDS: 300,
            VERIFIED_EXPIRE_SECONDS: 2592000,
            MAX_TOPIC_TITLE_LENGTH: 128,
            MAX_NAME_LENGTH: 30,
            TOPIC_HEALTH_TTL: 60,
            MAX_RETRY_ATTEMPTS: 3,
            NOTIFY_INTERVAL: 3600000,
            RATE_LIMIT_MESSAGE: 45,
            RATE_LIMIT_WINDOW: 60,
            API_TIMEOUT_MS: 10000,
            START_MSG_ZH_URL: 'https://raw.githubusercontent.com/Tsaihyun/hyunbot/refs/heads/main/data/startMessage.zh.md',
            START_MSG_EN_URL: 'https://raw.githubusercontent.com/Tsaihyun/hyunbot/refs/heads/main/data/startMessage.en.md',
            DEFAULT_BLOCKLIST_URL: 'https://raw.githubusercontent.com/Tsaihyun/hyunbot/refs/heads/main/data/blocklist.txt',
            BLOCKLIST_REFRESH_MS: 900000,
            REMOTE_CACHE_KEY: 'blocked-words-cache',
            REMOTE_ETAG_KEY: 'blocked-words-etag',
            REMOTE_LASTFETCH_KEY: 'blocked-words-lastfetch',
        };
        
        if (url.pathname === CONFIG.WEBHOOK) {
            return handleWebhook(request, env, ctx, CONFIG);
        } else if (url.pathname === '/registerWebhook') {
            return registerWebhook(request, url, env, CONFIG);
        } else if (url.pathname === '/unRegisterWebhook') {
            return unRegisterWebhook(env, CONFIG);
        } else if (url.pathname === '/init') {
            return initAdminTopic(env, CONFIG);
        } else {
            return new Response('OK');
        }
    }
};

function randomId(length = 12) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    const bytes = new Uint8Array(length);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(b => chars[b % chars.length]).join('');
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[m] || m));
}

const topicCreateLocks = new Map();
const topicHealthCache = new Map();
let adminTopicIdCache = null;

async function kvGet(env, key, type = 'json') {
    try { return await env.nfd.get(key, { type }); } 
    catch (e) { console.error('KV Get Error:', e); return null; }
}

async function kvPut(env, key, value, ttl = null) {
    try {
        const options = {};
        if (ttl) options.expirationTtl = ttl;
        const data = typeof value === 'object' ? JSON.stringify(value) : value;
        await env.nfd.put(key, data, options);
        return true;
    } catch (e) { console.error('KV Put Error:', e); return false; }
}

async function kvDelete(env, key) {
    try { await env.nfd.delete(key); return true; } 
    catch (e) { console.error('KV Delete Error:', e); return false; }
}

async function tgCall(env, config, method, body) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), config.API_TIMEOUT_MS);
    try {
        const response = await fetch('https://api.telegram.org/bot' + config.TOKEN + '/' + method, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        return response.json();
    } catch (e) {
        clearTimeout(timeoutId);
        console.error('TG API Error (' + method + '):', e);
        return { ok: false, description: e.message };
    }
}

const sendMessage = (env, config, body) => tgCall(env, config, 'sendMessage', body);
const copyMessage = (env, config, body) => tgCall(env, config, 'copyMessage', body);
const deleteMessage = (env, config, body) => tgCall(env, config, 'deleteMessage', body);
const createForumTopic = (env, config, body) => tgCall(env, config, 'createForumTopic', body);
const closeForumTopic = (env, config, body) => tgCall(env, config, 'closeForumTopic', body);
const reopenForumTopic = (env, config, body) => tgCall(env, config, 'reopenForumTopic', body);
const answerCallbackQuery = (env, config, body) => tgCall(env, config, 'answerCallbackQuery', body);

async function getAdminTopicId(env, config) {
    if (adminTopicIdCache) return adminTopicIdCache;
    const saved = await kvGet(env, config.ADMIN_TOPIC_KEY, 'text');
    if (saved) {
        adminTopicIdCache = parseInt(saved);
        return adminTopicIdCache;
    }
    return null;
}

async function createAdminTopic(env, config) {
    const existing = await getAdminTopicId(env, config);
    if (existing) return existing;
    
    const result = await createForumTopic(env, config, {
        chat_id: config.SUPERGROUP_ID,
        name: '📋 使用说明'
    });
    
    if (!result.ok) throw new Error('创建话题失败: ' + result.description);
    
    const threadId = result.result.message_thread_id;
    await kvPut(env, config.ADMIN_TOPIC_KEY, String(threadId));
    adminTopicIdCache = threadId;
    
    const helpText = '📋 *使用说明*\n\n' +
        '在这里执行管理命令，请勿用于回复用户。\n\n' +
        '━━━━━━━━━━━━━━━\n' +
        '🛠 *管理命令*\n' +
        '━━━━━━━━━━━━━━━\n' +
        '`/reloadblock`  刷新远程敏感词库\n' +
        '`/help`  显示本说明\n\n' +
        '━━━━━━━━━━━━━━━\n' +
        '👤 *用户话题内命令*\n' +
        '━━━━━━━━━━━━━━━\n' +
        '`/close`  关闭对话，用户无法发消息\n' +
        '`/open`  重新打开对话\n' +
        '`/block`  屏蔽用户\n' +
        '`/unblock`  解除屏蔽\n' +
        '`/reset`  重置验证，用户需重新验证\n' +
        '`/retopic`  重建话题\n' +
        '`/info`  查看用户信息\n\n' +
        '━━━━━━━━━━━━━━━\n' +
        '💬 *回复用户方式*\n' +
        '━━━━━━━━━━━━━━━\n' +
        '进入用户话题直接发消息即可。';
    
    await sendMessage(env, config, {
        chat_id: config.SUPERGROUP_ID,
        message_thread_id: threadId,
        text: helpText,
        parse_mode: 'Markdown'
    });
    
    return threadId;
}

async function isAdmin(env, config, userId) {
    const admins = await getAdminList(env, config);
    return admins.some(a => String(a.id) === String(userId));
}

async function getAdminList(env, config) {
    let admins = await kvGet(env, config.ADMIN_LIST_KEY, 'json');
    if (!Array.isArray(admins)) {
        admins = [{
            id: String(config.ADMIN_UID),
            username: 'admin',
            addedAt: Date.now()
        }];
        await kvPut(env, config.ADMIN_LIST_KEY, admins);
    }
    return admins;
}

async function getOrCreateUserTag(env, config, userId) {
    const key = 'user-tag-' + userId;
    const existing = await kvGet(env, key, 'text');
    if (existing) return existing;
    let counter = parseInt(await kvGet(env, config.USER_TAG_COUNTER_KEY, 'text') || '0');
    counter++;
    const tag = 'U' + String(counter).padStart(4, '0');
    await kvPut(env, config.USER_TAG_COUNTER_KEY, String(counter));
    await kvPut(env, key, tag);
    return tag;
}

function formatUserForAdmin(u) {
    const id = u?.id;
    const uname = u?.username;
    const name = [u?.first_name, u?.last_name].filter(Boolean).join(' ') || 'User';
    if (uname) return '@' + uname;
    if (id) return '<a href="tg://user?id=' + id + '">' + escapeHtml(name) + '</a>';
    return escapeHtml(name);
}

function buildTopicTitle(from, tag) {
    const firstName = (from?.first_name || "").trim().substring(0, 30);
    const lastName = (from?.last_name || "").trim().substring(0, 30);
    let username = "";
    if (from?.username) {
        username = from.username.replace(/[^\w]/g, '').substring(0, 20);
    }
    const cleanName = (firstName + " " + lastName).replace(/[\u0000-\u001F\u007F-\u009F]/g, '').replace(/\s+/g, ' ').trim();
    const name = cleanName || "User";
    const usernameStr = username ? ' @' + username : "";
    return ('[' + tag + '] ' + name + usernameStr).substring(0, 128);
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
        const k = String(kw).trim().toLowerCase();
        if (!k) continue;
        if (low.includes(k)) return kw;
    }
    return null;
}

async function getBlockedWordsRemote(env, config) {
    try {
        const lastFetch = await kvGet(env, config.REMOTE_LASTFETCH_KEY, 'text');
        if (lastFetch && (Date.now() - parseInt(lastFetch)) < config.BLOCKLIST_REFRESH_MS) {
            const cached = await kvGet(env, config.REMOTE_CACHE_KEY, 'json');
            if (cached?.words) return cached.words;
        }
        const response = await fetch(config.DEFAULT_BLOCKLIST_URL);
        if (!response.ok) {
            const cached = await kvGet(env, config.REMOTE_CACHE_KEY, 'json');
            return cached?.words || [];
        }
        const text = await response.text();
        const words = text.split(/\r?\n/).filter(l => l.trim() && !l.startsWith('#'));
        await kvPut(env, config.REMOTE_CACHE_KEY, { words, updatedAt: Date.now() });
        await kvPut(env, config.REMOTE_LASTFETCH_KEY, String(Date.now()));
        return words;
    } catch (e) {
        const cached = await kvGet(env, config.REMOTE_CACHE_KEY, 'json');
        return cached?.words || [];
    }
}

async function getAllBlockedWords(env, config) {
    return await getBlockedWordsRemote(env, config);
}

async function checkTopicHealth(env, config, userId, threadId) {
    const cacheKey = String(threadId);
    const cached = topicHealthCache.get(cacheKey);
    if (cached && (Date.now() - cached.ts) < config.TOPIC_HEALTH_TTL * 1000) {
        return cached.ok;
    }
    const kvHealth = await kvGet(env, 'topic-health:' + threadId, 'text');
    if (kvHealth === 'ok') {
        topicHealthCache.set(cacheKey, { ts: Date.now(), ok: true });
        return true;
    }
    const res = await sendMessage(env, config, {
        chat_id: config.SUPERGROUP_ID,
        message_thread_id: threadId,
        text: '.'
    });
    if (res.ok && res.result?.message_id) {
        await deleteMessage(env, config, {
            chat_id: config.SUPERGROUP_ID,
            message_id: res.result.message_id
        }).catch(() => {});
    }
    if (!res.ok && (res.description || '').toLowerCase().includes('not found')) {
        await resetUserTopic(env, config, userId);
        return false;
    }
    topicHealthCache.set(cacheKey, { ts: Date.now(), ok: true });
    await kvPut(env, 'topic-health:' + threadId, 'ok', config.TOPIC_HEALTH_TTL);
    return true;
}

async function resetUserTopic(env, config, userId) {
    const topicData = await kvGet(env, 'user-topic:' + userId, 'json');
    if (topicData?.threadId) {
        await kvDelete(env, 'topic-user:' + topicData.threadId);
        await kvDelete(env, 'topic-health:' + topicData.threadId);
        topicHealthCache.delete(String(topicData.threadId));
    }
    await kvDelete(env, 'user-topic:' + userId);
    await kvDelete(env, 'verify:' + userId);
    await kvDelete(env, 'retry:' + userId);
}

async function createUserTopic(env, config, userId, from) {
    const lockKey = 'topic-create:' + userId;
    if (topicCreateLocks.has(lockKey)) {
        return topicCreateLocks.get(lockKey);
    }
    const createPromise = (async () => {
        const existing = await kvGet(env, 'user-topic:' + userId, 'json');
        if (existing?.threadId) return existing;
        const tag = await getOrCreateUserTag(env, config, userId);
        const title = buildTopicTitle(from, tag);
        const result = await createForumTopic(env, config, {
            chat_id: config.SUPERGROUP_ID,
            name: title
        });
        if (!result.ok) throw new Error('创建话题失败: ' + result.description);
        const threadId = result.result.message_thread_id;
        const topicData = { threadId, title, tag, createdAt: Date.now(), userId };
        await kvPut(env, 'user-topic:' + userId, topicData);
        await kvPut(env, 'topic-user:' + threadId, String(userId));
        const actor = formatUserForAdmin(from || {});
        const langCode = from?.language_code || 'n/a';
        const headerText = '📂 *' + tag + '*\n' +
            '👤 ' + actor + '\n' +
            '🆔 `' + userId + '`\n' +
            '🌐 `' + langCode + '`\n' +
            '🔗 [私聊用户](tg://user?id=' + userId + ')';
        await sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID,
            message_thread_id: threadId,
            text: headerText,
            parse_mode: 'Markdown'
        });
        return topicData;
    })();
    topicCreateLocks.set(lockKey, createPromise);
    try { return await createPromise; } 
    finally { setTimeout(() => topicCreateLocks.delete(lockKey), 5000); }
}

async function sendVerification(env, config, userId, lang = 'en') {
    const verifyId = randomId(12);
    const verifyToken = randomId(8);
    await kvPut(env, 'verify-challenge:' + verifyId, {
        userId, token: verifyToken,
        expire: Date.now() + config.VERIFY_EXPIRE_SECONDS * 1000
    }, config.VERIFY_EXPIRE_SECONDS);
    const text = lang?.startsWith('zh')
        ? '🛡 *身份验证*\n\n请点击下方按钮完成验证，证明您不是机器人。'
        : '🛡 *Verification*\n\nPlease click the button below to verify you are human.';
    const buttonText = lang?.startsWith('zh') ? '✅ 我是人类' : '✅ I am human';
    await sendMessage(env, config, {
        chat_id: userId,
        text: text,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: buttonText, callback_data: 'verify:' + verifyId + ':' + verifyToken }]] }
    });
    return false;
}

async function checkVerification(env, config, userId, lang) {
    const verifyData = await kvGet(env, 'verify:' + userId, 'json');
    if (verifyData?.verified && verifyData.verifiedAt) {
        const age = Date.now() - verifyData.verifiedAt;
        if (age < config.VERIFIED_EXPIRE_SECONDS * 1000) return true;
    }
    return await sendVerification(env, config, userId, lang);
}

async function checkRateLimit(env, userId, action) {
    const key = 'ratelimit:' + action + ':' + userId;
    const count = parseInt(await kvGet(env, key, 'text') || '0');
    if (count >= 45) return false;
    await kvPut(env, key, String(count + 1), 60);
    return true;
}

async function sendNotification(env, config, userId, lang) {
    const key = 'notify:until:' + userId;
    const data = await kvGet(env, key, 'json');
    if (data?.until && Date.now() < data.until) return;
    await kvPut(env, key, { until: Date.now() + config.NOTIFY_INTERVAL });
    const text = lang?.startsWith('zh')
        ? '💬 您的消息已转发给管理员，请耐心等待回复。'
        : '💬 Your message has been forwarded. Please wait for a reply.';
    await sendMessage(env, config, { chat_id: userId, text: text });
}

async function handleWebhook(request, env, ctx, config) {
    if (request.headers.get('X-Telegram-Bot-Api-Secret-Token') !== config.SECRET) {
        return new Response('Unauthorized', { status: 403 });
    }
    try {
        const update = await request.json();
        ctx.waitUntil(processUpdate(env, config, update));
        return new Response('OK');
    } catch (e) {
        return new Response('Bad Request', { status: 400 });
    }
}

async function processUpdate(env, config, update) {
    if (update.callback_query) {
        await handleCallback(env, config, update.callback_query);
    } else if (update.message) {
        await handleMessage(env, config, update.message);
    }
}

async function handleCallback(env, config, query) {
    const data = query.data || '';
    if (!data.startsWith('verify:')) {
        return answerCallbackQuery(env, config, { callback_query_id: query.id });
    }
    const parts = data.split(':');
    const verifyId = parts[1];
    const token = parts[2];
    const userId = query.from.id;
    const challenge = await kvGet(env, 'verify-challenge:' + verifyId, 'json');
    if (!challenge || challenge.expire < Date.now()) {
        await answerCallbackQuery(env, config, { callback_query_id: query.id, text: '❌ 验证已过期，请重新获取', show_alert: true });
        await kvDelete(env, 'verify-challenge:' + verifyId);
        return sendVerification(env, config, userId, query.from?.language_code);
    }
    if (String(challenge.userId) !== String(userId)) {
        return answerCallbackQuery(env, config, { callback_query_id: query.id, text: '❌ 验证无效', show_alert: true });
    }
    if (challenge.token !== token) {
        return answerCallbackQuery(env, config, { callback_query_id: query.id, text: '❌ 验证失败，请重试', show_alert: true });
    }
    await kvPut(env, 'verify:' + userId, { verified: true, verifiedAt: Date.now() }, config.VERIFIED_EXPIRE_SECONDS);
    await kvDelete(env, 'verify-challenge:' + verifyId);
    await answerCallbackQuery(env, config, { callback_query_id: query.id, text: '✅ 验证通过，欢迎！' });
    try {
        await tgCall(env, config, 'editMessageText', {
            chat_id: userId,
            message_id: query.message.message_id,
            text: '✅ *验证通过*\n\n您现在可以发送消息了，管理员会尽快回复。',
            parse_mode: 'Markdown'
        });
    } catch (e) {}
}

async function handleMessage(env, config, message) {
    const chatId = String(message.chat.id);
    if (chatId === String(config.SUPERGROUP_ID)) {
        await handleGroupMessage(env, config, message);
    } else {
        await handlePrivateMessage(env, config, message);
    }
}

async function handlePrivateMessage(env, config, message) {
    const userId = message.chat.id;
    const lang = message.from?.language_code;
    
    if (message.text === '/start') {
        try {
            const startMsgUrl = (lang?.startsWith('zh') ? config.START_MSG_ZH_URL : config.START_MSG_EN_URL);
            const response = await fetch(startMsgUrl);
            if (response.ok) {
                const text = await response.text();
                return sendMessage(env, config, { chat_id: userId, text: text, parse_mode: 'Markdown' });
            }
        } catch (e) {}
        return sendMessage(env, config, {
            chat_id: userId,
            text: lang?.startsWith('zh')
                ? '👋 *欢迎*\n\n请直接发送消息，管理员会尽快回复。'
                : '👋 *Welcome*\n\nSend your message and we will reply soon.',
            parse_mode: 'Markdown'
        });
    }
    
    if (!await checkRateLimit(env, userId, 'message')) {
        return sendMessage(env, config, {
            chat_id: userId,
            text: lang?.startsWith('zh') ? '⚠️ 操作太频繁，请稍后再试。' : '⚠️ Too many requests. Please try again later.'
        });
    }
    
    if (await kvGet(env, 'blocked:' + userId, 'json')) {
        return sendMessage(env, config, {
            chat_id: userId,
            text: lang?.startsWith('zh') ? '🚫 您已被管理员屏蔽。' : '🚫 You have been blocked.'
        });
    }
    
    const verified = await checkVerification(env, config, userId, lang);
    if (!verified) return;
    
    const searchText = extractSearchableText(message);
    if (searchText) {
        const keywords = await getAllBlockedWords(env, config);
        const hit = hitBlockedKeyword(searchText, keywords);
        if (hit) {
            return sendMessage(env, config, {
                chat_id: userId,
                text: '⚠️ 您的消息包含违规内容，未发送。'
            });
        }
    }
    
    await forwardToTopic(env, config, message);
}

async function forwardToTopic(env, config, message) {
    const userId = message.chat.id;
    const retryKey = 'retry:' + userId;
    let retryCount = parseInt(await kvGet(env, retryKey, 'text') || '0');
    
    if (retryCount >= config.MAX_RETRY_ATTEMPTS) {
        await sendMessage(env, config, {
            chat_id: userId,
            text: '❌ 系统繁忙，请稍后再试。'
        });
        await kvDelete(env, retryKey);
        return;
    }
    
    try {
        let topicData = await kvGet(env, 'user-topic:' + userId, 'json');
        if (!topicData?.threadId) {
            topicData = await createUserTopic(env, config, userId, message.from);
        }
        const healthy = await checkTopicHealth(env, config, userId, topicData.threadId);
        if (!healthy) {
            topicData = await createUserTopic(env, config, userId, message.from);
        }
        if (topicData.closed) {
            return sendMessage(env, config, {
                chat_id: userId,
                text: '🚫 当前对话已关闭。'
            });
        }
        const result = await copyMessage(env, config, {
            chat_id: config.SUPERGROUP_ID,
            from_chat_id: userId,
            message_id: message.message_id,
            message_thread_id: topicData.threadId
        });
        if (!result.ok) throw new Error(result.description);
        await kvPut(env, 'msg-map:' + result.result.message_id, String(userId), 86400);
        await kvDelete(env, retryKey);
        await sendNotification(env, config, userId, message.from?.language_code);
    } catch (e) {
        console.error('转发失败:', e);
        retryCount++;
        await kvPut(env, retryKey, String(retryCount), 3600);
        await sendMessage(env, config, {
            chat_id: userId,
            text: '⚠️ 发送失败，请稍后重试。'
        });
    }
}

async function handleGroupMessage(env, config, message) {
    const threadId = message.message_thread_id;
    const senderId = message.from?.id;
    
    if (!await isAdmin(env, config, senderId)) return;
    
    const adminTopicId = await getAdminTopicId(env, config);
    
    if (threadId === adminTopicId) {
        const text = message.text || '';
        if (text === '/reloadblock') {
            await kvDelete(env, config.REMOTE_LASTFETCH_KEY);
            const words = await getBlockedWordsRemote(env, config);
            return sendMessage(env, config, {
                chat_id: config.SUPERGROUP_ID,
                message_thread_id: threadId,
                text: '✅ 远程词表已刷新，共 `' + words.length + '` 条',
                parse_mode: 'Markdown'
            });
        }
        if (text === '/help') {
            const helpText = '📋 *使用说明*\n\n' +
                '在这里执行管理命令，请勿用于回复用户。\n\n' +
                '━━━━━━━━━━━━━━━\n' +
                '🛠 *管理命令*\n' +
                '━━━━━━━━━━━━━━━\n' +
                '`/reloadblock`  刷新远程敏感词库\n' +
                '`/help`  显示本说明\n\n' +
                '━━━━━━━━━━━━━━━\n' +
                '👤 *用户话题内命令*\n' +
                '━━━━━━━━━━━━━━━\n' +
                '`/close`  关闭对话，用户无法发消息\n' +
                '`/open`  重新打开对话\n' +
                '`/block`  屏蔽用户\n' +
                '`/unblock`  解除屏蔽\n' +
                '`/reset`  重置验证，用户需重新验证\n' +
                '`/retopic`  重建话题\n' +
                '`/info`  查看用户信息\n\n' +
                '━━━━━━━━━━━━━━━\n' +
                '💬 *回复用户方式*\n' +
                '━━━━━━━━━━━━━━━\n' +
                '进入用户话题直接发消息即可。';
            return sendMessage(env, config, {
                chat_id: config.SUPERGROUP_ID,
                message_thread_id: threadId,
                text: helpText,
                parse_mode: 'Markdown'
            });
        }
        return;
    }
    
    if (!threadId) return;
    if (message.text?.startsWith('/')) {
        await handleUserTopicCommand(env, config, message);
        return;
    }
    
    const userId = await kvGet(env, 'topic-user:' + threadId, 'text');
    if (!userId) return;
    try {
        await copyMessage(env, config, {
            chat_id: parseInt(userId),
            from_chat_id: config.SUPERGROUP_ID,
            message_id: message.message_id
        });
    } catch (e) {
        await sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID,
            message_thread_id: threadId,
            text: '⚠️ 发送失败，用户可能已屏蔽机器人。'
        });
    }
}

async function handleUserTopicCommand(env, config, message) {
    const text = message.text || '';
    const threadId = message.message_thread_id;
    const senderId = message.from?.id;
    const userId = await kvGet(env, 'topic-user:' + threadId, 'text');
    if (!userId) return;
    
    if (text === '/close') {
        const d = await kvGet(env, 'user-topic:' + userId, 'json') || {};
        d.closed = true;
        await kvPut(env, 'user-topic:' + userId, d);
        await closeForumTopic(env, config, { chat_id: config.SUPERGROUP_ID, message_thread_id: threadId });
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '🚫 *对话已关闭*\n用户无法继续发送消息。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/open') {
        const d = await kvGet(env, 'user-topic:' + userId, 'json') || {};
        d.closed = false;
        await kvPut(env, 'user-topic:' + userId, d);
        await reopenForumTopic(env, config, { chat_id: config.SUPERGROUP_ID, message_thread_id: threadId });
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '✅ *对话已恢复*\n用户可以继续发送消息。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/block') {
        await kvPut(env, 'blocked:' + userId, { by: String(senderId), at: Date.now() });
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '🚫 *用户已屏蔽*\n该用户消息将不再转发。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/unblock') {
        await kvDelete(env, 'blocked:' + userId);
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '✅ *已解除屏蔽*\n该用户可正常发送消息。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/reset') {
        await kvDelete(env, 'verify:' + userId);
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '🔄 *验证已重置*\n用户下次发消息需重新验证。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/retopic') {
        await resetUserTopic(env, config, userId);
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: '🔄 *话题已重置*\n用户下次发消息将创建新话题。',
            parse_mode: 'Markdown'
        });
    }
    if (text === '/info') {
        const d = await kvGet(env, 'user-topic:' + userId, 'json') || {};
        const blocked = await kvGet(env, 'blocked:' + userId, 'json');
        const verified = await kvGet(env, 'verify:' + userId, 'json');
        const infoText = '👤 *用户信息*\n\n' +
            '📛 编号: `' + (d.tag || 'N/A') + '`\n' +
            '🆔 ID: `' + userId + '`\n' +
            '✅ 验证: ' + (verified ? '已通过' : '未验证') + '\n' +
            '🚫 屏蔽: ' + (blocked ? '是' : '否') + '\n' +
            '🔗 [私聊用户](tg://user?id=' + userId + ')';
        return sendMessage(env, config, {
            chat_id: config.SUPERGROUP_ID, message_thread_id: threadId,
            text: infoText,
            parse_mode: 'Markdown'
        });
    }
}

async function initAdminTopic(env, config) {
    try {
        adminTopicIdCache = null;
        await kvDelete(env, config.ADMIN_TOPIC_KEY);
        const threadId = await createAdminTopic(env, config);
        return new Response(JSON.stringify({ ok: true, threadId }), {
            headers: { 'Content-Type': 'application/json' }
        });
    } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), {
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

async function registerWebhook(request, url, env, config) {
    const webhookUrl = url.protocol + '//' + url.hostname + config.WEBHOOK;
    const res = await tgCall(env, config, 'setWebhook', {
        url: webhookUrl,
        secret_token: config.SECRET,
        allowed_updates: ["message", "callback_query"],
        drop_pending_updates: true
    });
    
    adminTopicIdCache = null;
    await kvDelete(env, config.ADMIN_TOPIC_KEY);
    await createAdminTopic(env, config);
    
    return new Response(JSON.stringify(res, null, 2), {
        headers: { 'Content-Type': 'application/json' }
    });
}

async function unRegisterWebhook(env, config) {
    const res = await tgCall(env, config, 'setWebhook', { url: '' });
    adminTopicIdCache = null;
    return new Response(JSON.stringify(res, null, 2), {
        headers: { 'Content-Type': 'application/json' }
    });
}
