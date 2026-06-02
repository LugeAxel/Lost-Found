const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { body, param, validationResult } = require('express-validator');
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });
const cron = require('node-cron');
const cloudinary = require('./config/cloudinary');
const { cache, invalidateCache } = require('./middleware/cache');
const { webpush, sendPush, vapidPublicKey } = require('./config/webpush');

const http = require('http');
const { Server } = require('socket.io');
const { supabase } = require('./lib/supabase');
const crypto = require('crypto');

// ── STRUCTURED LOGGER ─────────────────────────
const LOG_LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const CURRENT_LOG_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL] || LOG_LEVELS.INFO;

function pad(n) { return String(n).padStart(2, '0'); }
function timestamp() {
    const d = new Date();
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.${String(d.getUTCMilliseconds()).padStart(3,'0')}Z`;
}

const logger = {
    _log(level, label, msg, meta) {
        if (LOG_LEVELS[level] < CURRENT_LOG_LEVEL) return;
        const metaStr = meta && Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
        const out = level === 'ERROR' || level === 'WARN' ? process.stderr : process.stdout;
        out.write(`[${timestamp()}] [${label}] ${msg}${metaStr}\n`);
    },
    info(msg, meta)  { this._log('INFO', 'INFO', msg, meta); },
    warn(msg, meta)  { this._log('WARN', 'WARN', msg, meta); },
    error(msg, meta) { this._log('ERROR', 'ERR', msg, meta); },
    debug(msg, meta) { this._log('DEBUG', 'DBG', msg, meta); },

    request(req, msg, data) {
        const meta = { ...data, requestId: req.requestId, userId: req.user?.id, method: req.method, path: req.originalUrl };
        this.info(msg, meta);
    },
    response(req, res, durationMs) {
        const meta = { requestId: req.requestId, userId: req.user?.id, statusCode: res.statusCode, durationMs };
        const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';
        this[level](`${req.method} ${req.originalUrl} → ${res.statusCode}`, meta);
    }
};

// Attach requestId to every request
function attachRequestId(req, res, next) {
    req.requestId = crypto.randomUUID().slice(0, 8);
    req._startTime = Date.now();
    res.on('finish', () => {
        const duration = Date.now() - req._startTime;
        if (req.originalUrl !== '/api/health') {
            logger.response(req, res, duration);
        }
    });
    next();
}

const ERROR_CODES = {
    VALIDATION: 'VALIDATION_ERROR',
    NOT_FOUND: 'NOT_FOUND',
    FORBIDDEN: 'FORBIDDEN',
    UNAUTHORIZED: 'UNAUTHORIZED',
    CONFLICT: 'CONFLICT',
    FILE_TOO_LARGE: 'FILE_TOO_LARGE',
    UPLOAD_FAILED: 'UPLOAD_FAILED',
    INTERNAL: 'INTERNAL_ERROR',
    BAD_REQUEST: 'BAD_REQUEST',
};

function sendError(res, status, message, code = ERROR_CODES.BAD_REQUEST, details = null) {
    const body = { success: false, message, code };
    if (details) body.details = details;
    return res.status(status).json(body);
}

function extractCloudinaryPublicId(url) {
    if (!url || !url.includes('cloudinary')) return null;
    const match = url.match(/\/upload\/(?:v\d+\/)?(.+?)\.\w+$/);
    return match ? match[1] : null;
}

const app = express();
app.set('trust proxy', 1);
const upload = require('./middleware/upload');
const server = http.createServer(app);
const PORT = process.env.PORT || 5000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    logger.error('FATAL: SUPABASE_URL or SUPABASE_ANON_KEY is not set in .env');
    process.exit(1);
}

// ─────────────────────────────────────────────
// 2. SECURITY MIDDLEWARE
// ─────────────────────────────────────────────

app.use(helmet({
    crossOriginResourcePolicy: false
}));

const isProduction = process.env.NODE_ENV === 'production';
const allowedOrigins = [
    ...(isProduction ? [] : [
        'http://localhost:5173',
        'http://localhost:5174',
        'http://localhost:4173',
        process.env.FRONTEND_URL ? process.env.FRONTEND_URL.replace(/\/$/, '') : null,
    ]),
    process.env.FRONTEND_URL
].filter(Boolean);

const io = new Server(server, {
    cors: {
        origin: (origin, callback) => {
            if (!origin) return callback(null, true);
            if (allowedOrigins.includes(origin)) {
                return callback(null, true);
            }
            logger.warn('Socket blocked by CORS', { origin });
            return callback(new Error('Socket CORS blocked'));
        },
        methods: ['GET', 'POST'],
        credentials: true
    }
});

io.use(async (socket, next) => {
    const token = socket.handshake.auth?.token;
    if (!token) {
        return next(new Error('Authentication required'));
    }
    try {
        const { data: { user }, error } = await supabase.auth.getUser(token);
        if (error || !user) {
            return next(new Error('Invalid or expired token'));
        }
        socket.userId = user.id;
        next();
    } catch (err) {
        next(new Error('Authentication error'));
    }
});

const socketRateLimit = new Map();
const SOCKET_RATE_LIMIT_WINDOW = 10000;
const SOCKET_RATE_LIMIT_MAX = 20;

const checkSocketRateLimit = (socketId) => {
    const now = Date.now();
    const entry = socketRateLimit.get(socketId);
    if (!entry || now - entry.windowStart > SOCKET_RATE_LIMIT_WINDOW) {
        socketRateLimit.set(socketId, { windowStart: now, count: 1 });
        return true;
    }
    if (entry.count >= SOCKET_RATE_LIMIT_MAX) {
        return false;
    }
    entry.count++;
    return true;
};

const onlineUsers = new Set();

io.on('connection', (socket) => {
    logger.info('New socket connection', { userId: socket.userId });

    onlineUsers.add(socket.userId);
    socket.broadcast.emit('user-online', socket.userId);

    socket.on('join-user', (userId) => {
        if (socket.userId === userId) {
            socket.join(`user-${userId}`);
        }
    });

    socket.on('join-item', async (itemId) => {
        try {
            const { data: item } = await supabase
                .from('items')
                .select('reporter, claimer')
                .eq('id', itemId)
                .single();

            if (!item) return;
            const uid = socket.userId;
            const isReporter = item.reporter === uid;
            const isClaimer = item.claimer && item.claimer === uid;
            if (isReporter || isClaimer) {
                socket.join(`item-${itemId}`);
            }
        } catch (e) {
            // silently ignore
        }
    });

    socket.on('send-message', async (data) => {
        if (!checkSocketRateLimit(socket.id)) {
            socket.emit('rate-limited', { message: 'Too many messages. Please slow down.' });
            return;
        }
        try {
            const { data: item } = await supabase
                .from('items')
                .select('reporter, claimer')
                .eq('id', data.itemId)
                .single();
            if (!item) return;
            const uid = socket.userId;
            if (item.reporter !== uid && item.claimer !== uid) return;
        } catch {
            return;
        }
        socket.broadcast.to(`item-${data.itemId}`).emit('new-message', data);
    });

    socket.on('disconnect', () => {
        onlineUsers.delete(socket.userId);
        socket.broadcast.emit('user-offline', socket.userId);
        socketRateLimit.delete(socket.id);
        logger.info('Socket disconnected', { userId: socket.userId });
    });
});

async function logActivity({ itemId, userId, actionType, description, metadata }) {
    try {
        await supabase
            .from('item_activities')
            .insert({
                item_id: itemId,
                user_id: userId,
                action_type: actionType,
                description,
                metadata: metadata || {},
            });
    } catch (e) {
        logger.error('logActivity error', { itemId, actionType, error: e.message });
    }
}

app.use(cors({
    origin: (origin, callback) => {
        if (!origin) {
            return callback(null, true);
        }
        if (allowedOrigins.includes(origin)) {
            return callback(null, true);
        }
        logger.warn(`Blocked by CORS: ${origin}`);
        return callback(new Error('Not allowed by CORS'));
    },
    credentials: true
}));

app.use(express.json({ limit: '5mb' }));
app.use(attachRequestId);

const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 2000,
    skip: (req) => req.originalUrl.startsWith('/socket.io'),
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, message: 'Too many requests, please try again later.' }
});
app.use(globalLimiter);

const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 20,
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, message: 'Too many login attempts, please try again later.' }
});

const heavyEndpointLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 30,
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, message: 'Too many requests to this endpoint.' }
});

// Per-user in-memory rate limiter (for actions that trigger notifications)
const userRateLimits = new Map();
const checkUserRateLimit = (userId, action, maxPerMinute = 50) => {
    const now = Date.now();
    const key = `${userId}:${action}`;
    const entry = userRateLimits.get(key);
    if (!entry || now - entry.windowStart > 60000) {
        userRateLimits.set(key, { windowStart: now, count: 1 });
        return true;
    }
    if (entry.count >= maxPerMinute) {
        return false;
    }
    entry.count++;
    return true;
};
// Cleanup stale entries and log memory usage every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of userRateLimits.entries()) {
        if (now - entry.windowStart > 120000) {
            userRateLimits.delete(key);
        }
    }
    const mcache = require('memory-cache');
    const usage = process.memoryUsage();
    logger.info(`[HEALTH] rateLimitEntries: ${userRateLimits.size}, cacheSize: ${mcache.size()}, rss: ${Math.round(usage.rss / 1024 / 1024)}MB, heap: ${Math.round(usage.heapUsed / 1024 / 1024)}/${Math.round(usage.heapTotal / 1024 / 1024)}MB`);
}, 300000);

// ─────────────────────────────────────────────
// 3. AUTH MIDDLEWARE (Supabase Auth)
// ─────────────────────────────────────────────
const authMiddleware = async (req, res, next) => {
    try {
        const authHeader = req.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            logger.warn('Auth: no token provided', { requestId: req.requestId });
            return sendError(res, 401, 'Access denied. No token provided.', ERROR_CODES.UNAUTHORIZED);
        }

        const token = authHeader.split(' ')[1];
        const { data: { user }, error } = await supabase.auth.getUser(token);

        if (error || !user) {
            logger.warn('Auth: invalid or expired token', { requestId: req.requestId });
            return sendError(res, 401, 'Invalid or expired token.', ERROR_CODES.UNAUTHORIZED);
        }

        if (!user.email_confirmed_at) {
            return res.status(403).json({
                success: false,
                message: 'Email not verified. Please check your inbox.',
                email_verified: false
            });
        }

        const { data: profile, error: profileError } = await supabase
            .from('profiles')
            .select('*')
            .eq('id', user.id)
            .single();

        if (profileError || !profile) {
            logger.warn('Auth: profile not found', { requestId: req.requestId, userId: user.id });
            return sendError(res, 401, 'User profile not found.', ERROR_CODES.UNAUTHORIZED);
        }

        req.user = profile;
        req.supabaseUser = user;
        next();
    } catch (error) {
        logger.error('Auth middleware error', { requestId: req.requestId, error: error.message });
        return sendError(res, 500, 'Authentication error.', ERROR_CODES.INTERNAL);
    }
};

const handleValidationErrors = (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
        return res.status(400).json({
            success: false,
            message: 'Validation failed',
            errors: errors.array().map(e => e.msg)
        });
    }
    next();
};

function sanitizeHtml(str) {
    if (typeof str !== 'string') return str;
    return str.replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));
}

// Send Web Push notification to all devices for a user
async function sendPushNotification(userId, payload) {
    try {
        const { data: subs, error } = await supabase
            .from('push_subscriptions')
            .select('*')
            .eq('user_id', userId);

        if (error) throw error;
        if (!subs || subs.length === 0) return;

        for (const sub of subs) {
            const result = await sendPush({
                endpoint: sub.endpoint,
                keys: { p256dh: sub.p256dh, auth: sub.auth },
            }, payload);

            if (result && result.expired) {
                await supabase
                    .from('push_subscriptions')
                    .delete()
                    .eq('id', sub.id);
                logger.info('Removed expired push subscription', { userId, subId: sub.id });
            }
        }
    } catch (e) {
        logger.error('sendPushNotification error', { userId, error: e.message });
    }
}

// Haversine distance in meters
function haversineDistance(lat1, lng1, lat2, lng2) {
    const R = 6371000;
    const toRad = (deg) => (deg * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLng = toRad(lng2 - lng1);
    const a = Math.sin(dLat / 2) ** 2 +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLng / 2) ** 2;
    return R * 2 * Math.asin(Math.sqrt(a));
}

// Basic criteria words — generic/common terms that get lower match weight
const BASIC_CRITERIA = new Set([
    'hitam', 'putih', 'merah', 'biru', 'hijau', 'kuning', 'coklat', 'abu', 'ungu',
    'pink', 'orange', 'silver', 'emas', 'krem', 'tosca', 'marun',
    'kain', 'plastik', 'logam', 'karet', 'kulit', 'kaca', 'kayu', 'besi', 'kertas',
    'kecil', 'besar', 'baru', 'bekas', 'tipis', 'tebal', 'panjang', 'pendek',
    'tas', 'botol', 'buku', 'pulpen', 'pensil', 'hp', 'ponsel', 'laptop',
    'charger', 'kabel', 'mouse', 'sepatu', 'sandal', 'topi', 'jam', 'kaos',
    'baju', 'jaket', 'celana', 'dompet', 'kacamata', 'handuk', 'payung',
    'makanan', 'minuman', 'tumbler', 'earphone', 'headset', 'flashdisk',
    'kalkulator', 'penggaris', 'penghapus', 'mistar',
    'rusak', 'robek', 'sobek', 'hilang', 'ditemukan',
]);

// Extract meaningful keywords from item name and description
function extractKeywords(name, description) {
    const text = `${name || ''} ${description || ''}`.toLowerCase();
    const stopWords = new Set([
        'dan', 'di', 'ke', 'dari', 'yang', 'ini', 'itu', 'saya', 'aku',
        'the', 'a', 'an', 'in', 'of', 'to', 'is', 'for', 'on', 'and',
        'or', 'with', 'yang', 'ada', 'tidak', 'bisa', 'barang', 'warna',
    ]);
    const words = text.split(/\s+/).filter(w => w.length >= 3 && !stopWords.has(w));
    return [...new Set(words)];
}

// Compute text relevance score between lost item and found item
function computeTextRelevance(lostItem, foundItem) {
    const lostName = (lostItem.name || '').toLowerCase().trim();
    const lostDesc = (lostItem.description || '').toLowerCase();
    const foundName = (foundItem.name || '').toLowerCase().trim();
    const foundDesc = (foundItem.description || '').toLowerCase();
    const foundText = `${foundName} ${foundDesc}`;

    let score = 0;

    // Exact full name match (highest priority)
    if (lostName === foundName) {
        score += 80;
    } else {
        // Check if all meaningful name words from lost item appear in found item name
        const lostNameWords = lostName.split(/\s+/).filter(w => w.length >= 2 && w !== 'yang' && w !== 'dan');
        const allWordsMatch = lostNameWords.length > 0 && lostNameWords.every(w => foundName.includes(w));
        if (allWordsMatch) {
            score += 50;
        }

        // Full phrase match: lost item name appears anywhere in found text
        if (foundText.includes(lostName)) score += 30;
    }

    // Keyword-level matching with basic criteria awareness
    const keywords = extractKeywords(lostItem.name, lostItem.description);
    for (const kw of keywords) {
        if (BASIC_CRITERIA.has(kw)) {
            if (foundName.includes(kw)) score += 3;
            else if (foundDesc.includes(kw)) score += 1;
        } else {
            if (foundName.includes(kw)) score += 15;
            else if (foundDesc.includes(kw)) score += 5;
        }
    }

    return score;
}

// Score how well a candidate matches a source item (category + area + text + GPS)
function computeScore(sourceItem, candidateItem) {
    let score = 0;

    // Text relevance is the primary factor
    const textScore = computeTextRelevance(sourceItem, candidateItem);
    score += textScore;

    // Category/area bonuses only apply if there's keyword text match
    if (textScore > 0) {
        if (candidateItem.category === sourceItem.category) score += 14;
        if (sourceItem.area_category && candidateItem.area_category === sourceItem.area_category) score += 14;
    }

    // GPS proximity bonus (reduced weight)
    const lat = sourceItem.coordinates_lat;
    const lng = sourceItem.coordinates_lng;
    if (lat != null && lng != null && candidateItem.coordinates_lat != null && candidateItem.coordinates_lng != null) {
        const dist = haversineDistance(lat, lng, candidateItem.coordinates_lat, candidateItem.coordinates_lng);
        if (dist < 100) score += 10;
        else if (dist < 500) score += 5;
        else if (dist < 1000) score += 1;
    }

    return score;
}

// 3-pass matching: find top-N items of targetType that match sourceItem
async function findMatchingItems(sourceItem, targetType, limit = 5) {
    const keywords = extractKeywords(sourceItem.name, sourceItem.description);
    const candidates = [];
    const seen = new Set();

    // Pass 1: same category + same area_category (highest confidence)
    let q1 = supabase
        .from('items')
        .select('*, reporter:profiles!items_reporter_fkey(id, nama, nisn)')
        .eq('type', targetType)
        .in('status', ['Available', 'On Progress', 'Returned'])
        .eq('category', sourceItem.category)
        .neq('id', sourceItem.id)
        .neq('reporter', sourceItem.reporter);
    if (sourceItem.area_category) q1 = q1.eq('area_category', sourceItem.area_category);
    const { data: p1 } = await q1.limit(30);
    if (p1) for (const fi of p1) { seen.add(fi.id); candidates.push(fi); }

    // Pass 2: same category only (broader)
    if (sourceItem.area_category) {
        let q2 = supabase
            .from('items')
            .select('*, reporter:profiles!items_reporter_fkey(id, nama, nisn)')
            .eq('type', targetType)
            .in('status', ['Available', 'On Progress', 'Returned'])
            .eq('category', sourceItem.category)
            .neq('id', sourceItem.id)
            .neq('reporter', sourceItem.reporter);
        const { data: p2 } = await q2.limit(20);
        if (p2) for (const fi of p2) { if (!seen.has(fi.id)) { seen.add(fi.id); candidates.push(fi); } }
    }

    // Pass 3: text keyword match across all categories
    if (keywords.length > 0) {
        const orClauses = keywords.flatMap(kw => [
            `name.ilike.%25${kw}%25`,
            `description.ilike.%25${kw}%25`
        ]).join(',');
        let q3 = supabase
            .from('items')
            .select('*, reporter:profiles!items_reporter_fkey(id, nama, nisn)')
            .eq('type', targetType)
            .in('status', ['Available', 'On Progress', 'Returned'])
            .neq('category', sourceItem.category)
            .neq('id', sourceItem.id)
            .neq('reporter', sourceItem.reporter)
            .or(orClauses);
        const { data: p3 } = await q3.limit(10);
        if (p3) for (const fi of p3) { if (!seen.has(fi.id)) { seen.add(fi.id); candidates.push(fi); } }
    }

    // Score & sort
    const scored = candidates.map(fi => {
        const score = computeScore(sourceItem, fi);
        let distance = null;
        if (sourceItem.coordinates_lat != null && sourceItem.coordinates_lng != null && fi.coordinates_lat != null && fi.coordinates_lng != null) {
            distance = Math.round(haversineDistance(sourceItem.coordinates_lat, sourceItem.coordinates_lng, fi.coordinates_lat, fi.coordinates_lng));
        }
        return { ...fi, score, distance_meters: distance };
    });

    scored.sort((a, b) => b.score - a.score || (a.distance_meters || Infinity) - (b.distance_meters || Infinity));

    const filtered = scored.filter(s => s.score >= 40);
    return filtered.slice(0, limit);
}

// ─────────────────────────────────────────────
// 4. ROUTES
// ─────────────────────────────────────────────

// Health check (public)
app.get('/api/health', async (req, res) => {
    try {
        const { error } = await supabase.from('profiles').select('id').limit(1);
        res.json({
            status: 'online',
            db: error ? 'disconnected' : 'connected'
        });
    } catch {
        res.json({ status: 'online', db: 'disconnected' });
    }
});

// ── AUTH ROUTES ──────────────────────────────

// QR Code login (NISN-based auto-registration via Supabase Auth)
app.post('/api/login',
    authLimiter,
    [
        body('nisn').trim().notEmpty().withMessage('NISN is required')
            .isLength({ max: 20 }).withMessage('NISN too long')
            .matches(/^[0-9]+$/).withMessage('NISN must be numeric'),
        body('nama').trim().notEmpty().withMessage('Name is required')
            .isLength({ max: 100 }).withMessage('Name too long'),
        body('nis').optional().trim().isLength({ max: 20 }),
        body('jurusan').optional().trim().isLength({ max: 100 }),
        body('ttl').optional().trim().isLength({ max: 100 }),
        body('agama').optional().trim().isLength({ max: 50 }),
        body('gender').optional().trim().isLength({ max: 20 }),
    ],
    handleValidationErrors,
    async (req, res) => {
        const maskedNisn = req.body.nisn ? req.body.nisn.slice(0, -4) + '****' : '(none)';
        logger.request(req, 'QR Login request', { nisn: maskedNisn });

        try {
            const { nisn, nama, nis, jurusan, ttl, agama, gender } = req.body;

            let { data: profile } = await supabase
                .from('profiles')
                .select('*')
                .eq('nisn', nisn)
                .single();

            if (!profile) {
                logger.request(req, 'New student detected, creating profile', { nisn: maskedNisn });
                const email = `nisn_${nisn}@qreturn.local`;
                const password = crypto.randomBytes(32).toString('hex');

                const { data: authData, error: authError } = await supabase.auth.admin.createUser({
                    email,
                    password,
                    email_confirm: true,
                    user_metadata: { nama, nisn, nis, jurusan, ttl, agama, gender, qr_password: password }
                });

                if (authError) {
                    logger.error('Auth creation error', { requestId: req.requestId, error: authError.message, nisn: maskedNisn });
                    return sendError(res, 500, 'Failed to create user.', ERROR_CODES.INTERNAL);
                }

                profile = authData.user;
                profile.qr_password = password;
            } else {
                logger.request(req, 'Welcome back', { nama: profile.nama, nisn: maskedNisn });
                const { data: authUser } = await supabase.auth.admin.getUserById(profile.id);
                profile.qr_password = authUser?.user?.user_metadata?.qr_password || crypto.randomBytes(32).toString('hex');
            }

            const email = `nisn_${nisn}@qreturn.local`;
            const password = profile.qr_password;

            const { data: sessionData, error: sessionError } = await supabase.auth.signInWithPassword({
                email,
                password
            });

            if (sessionError) {
                logger.error('Session error, resetting password', { requestId: req.requestId, error: sessionError.message, nisn: maskedNisn });
                const newPassword = crypto.randomBytes(32).toString('hex');
                await supabase.auth.admin.updateUserById(profile.id, {
                    password: newPassword,
                    user_metadata: { qr_password: newPassword }
                });

                const { data: newSession, error: newSessionError } = await supabase.auth.signInWithPassword({
                    email,
                    password: newPassword
                });

                if (newSessionError) {
                    logger.error('Failed to login after password reset', { requestId: req.requestId, error: newSessionError.message, nisn: maskedNisn });
                    return sendError(res, 500, 'Failed to authenticate user.', ERROR_CODES.INTERNAL);
                }

                res.json({
                    success: true,
                    token: newSession.session.access_token,
                    refresh_token: newSession.session.refresh_token,
                    user: {
                        id: profile.id,
                        nama: profile.nama,
                        nisn: profile.nisn,
                        nis: profile.nis,
                        jurusan: profile.jurusan,
                        role: profile.role,
                        email: profile.email || null
                    }
                });
                return;
            }

            res.json({
                success: true,
                token: sessionData.session.access_token,
                refresh_token: sessionData.session.refresh_token,
                user: {
                    id: profile.id,
                    nama: profile.nama,
                    nisn: profile.nisn,
                    nis: profile.nis,
                    jurusan: profile.jurusan,
                    role: profile.role,
                    email: profile.email || null
                }
            });
        } catch (error) {
            logger.error('QR Login error', { requestId: req.requestId, error: error.message, stack: error.stack });
            sendError(res, 500, 'Server error during login.', ERROR_CODES.INTERNAL);
        }
    }
);

// Email registration
app.post('/api/auth/register-email',
    authLimiter,
    [
        body('email').trim().isEmail().withMessage('Valid email is required')
            .isLength({ max: 100 }).withMessage('Email too long'),
        body('password').isLength({ min: 6, max: 100 }).withMessage('Password must be at least 6 characters'),
        body('nama').trim().notEmpty().withMessage('Name is required')
            .isLength({ max: 100 }).withMessage('Name too long'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { email, password, nama } = req.body;

            const { data: existingUsers } = await supabase
                .from('profiles')
                .select('id')
                .eq('email', email)
                .limit(1);

            if (existingUsers && existingUsers.length > 0) {
                return res.status(409).json({
                    success: false,
                    message: 'An account with this email already exists. Please login instead.'
                });
            }

            const { data: authData, error: authError } = await supabase.auth.signUp({
                email,
                password,
                options: {
                    data: { nama },
                    emailRedirectTo: `${process.env.FRONTEND_URL}/verify-email`
                }
            });

            if (authError) {
                logger.error('Email register error', { requestId: req.requestId, error: authError.message });
                return sendError(res, 500, 'Server error during registration.', ERROR_CODES.INTERNAL);
            }

            logger.info('New email user registered', { email: email?.split('@')[0] + '@***' });

            res.status(201).json({
                success: true,
                message: 'Account created! Please check your email to verify.',
                requiresVerification: true,
                user: {
                    id: authData.user?.id,
                    email: authData.user?.email,
                    email_confirmed_at: authData.user?.email_confirmed_at || null
                }
            });
        } catch (error) {
            logger.error('Email register error', { requestId: req.requestId, error: error.message, stack: error.stack });
            sendError(res, 500, 'Server error during registration.', ERROR_CODES.INTERNAL);
        }
    }
);

// Email login
app.post('/api/auth/login-email',
    authLimiter,
    [
        body('email').trim().isEmail().withMessage('Valid email is required'),
        body('password').notEmpty().withMessage('Password is required'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { email, password } = req.body;

            const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
                email,
                password
            });

            if (authError) {
                return res.status(401).json({
                    success: false,
                    message: 'Invalid email or password.'
                });
            }

            const emailConfirmed = !!authData.user?.email_confirmed_at;

            if (!emailConfirmed) {
                return res.status(403).json({
                    success: false,
                    message: 'Email not verified. Please check your inbox.',
                    email_verified: false,
                    email: email
                });
            }

            const { data: profile } = await supabase
                .from('profiles')
                .select('*')
                .eq('id', authData.user.id)
                .single();

            logger.info('Email login', { email: email?.split('@')[0] + '@***' });

            res.json({
                success: true,
                token: authData.session.access_token,
                user: {
                    id: profile?.id,
                    nama: profile?.nama,
                    nisn: profile?.nisn || null,
                    nis: profile?.nis || null,
                    jurusan: profile?.jurusan || null,
                    role: profile?.role,
                    email: profile?.email,
                    email_verified: emailConfirmed
                }
            });
        } catch (error) {
            logger.error('Email login error', { requestId: req.requestId, error: error.message, stack: error.stack });
            sendError(res, 500, 'Server error during login.', ERROR_CODES.INTERNAL);
        }
    }
);

// Verify token + get current user
app.get('/api/auth/me', authMiddleware, async (req, res) => {
    res.json({
        success: true,
        user: {
            id: req.user.id,
            nama: req.user.nama,
            nisn: req.user.nisn || null,
            nis: req.user.nis || null,
            jurusan: req.user.jurusan || null,
            role: req.user.role,
            email: req.user.email || null,
            email_verified: !!req.supabaseUser?.email_confirmed_at
        }
    });
});

// Resend email verification
app.post('/api/auth/resend-verification', async (req, res) => {
    try {
        const { email } = req.body;
        if (!email) {
            return res.status(400).json({ success: false, message: 'Email is required' });
        }

        const { error } = await supabase.auth.resend({
            type: 'signup',
            email
        });

        if (error) {
            return res.status(400).json({ success: false, message: error.message });
        }

        res.json({ success: true, message: 'Verification email sent' });
    } catch (error) {
        logger.error('Resend verification error', { requestId: req.requestId, error: error.message });
        sendError(res, 500, 'Failed to resend verification email.', ERROR_CODES.INTERNAL);
    }
});

// ── PROFILE ROUTES ──────────────────────────

// Get own profile (full, with new fields)
app.get('/api/profile/me', authMiddleware, async (req, res) => {
    try {
        const { data: profile, error } = await supabase
            .from('profiles')
            .select('*')
            .eq('id', req.user.id)
            .single();

        if (error || !profile) {
            return res.status(404).json({ success: false, message: 'Profile not found.' });
        }

        res.json({ success: true, profile });
    } catch (error) {
        logger.error('Fetch profile error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch profile.', ERROR_CODES.INTERNAL);
    }
});

// Update own profile
app.put('/api/profile/me',
    authMiddleware,
    [
        body('username').optional({ values: 'null' }).trim().isLength({ min: 3, max: 50 }).withMessage('Username must be 3-50 characters')
            .matches(/^[a-zA-Z0-9_-]+$/).withMessage('Username can only contain letters, numbers, hyphens, and underscores'),
        body('bio').optional({ values: 'falsy' }).trim().isLength({ max: 200 }).withMessage('Bio too long'),
        body('wa').optional({ values: 'falsy' }).trim().isLength({ max: 20 }).withMessage('WhatsApp number too long'),
        body('ig').optional({ values: 'falsy' }).trim().isLength({ max: 50 }).withMessage('Instagram handle too long'),
        body('fb').optional({ values: 'falsy' }).trim().isLength({ max: 50 }).withMessage('Facebook handle too long'),
        body('tg').optional({ values: 'falsy' }).trim().isLength({ max: 50 }).withMessage('Telegram handle too long'),
        body('show_tips').optional().isBoolean().withMessage('show_tips must be a boolean'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const updateData = {};
            const contactFields = ['wa', 'ig', 'fb', 'tg'];
            const profileFields = ['username', 'bio', ...contactFields];

            for (const field of profileFields) {
                if (req.body[field] !== undefined) {
                    updateData[field] = req.body[field] || null;
                }
            }

            if (req.body.show_tips !== undefined) {
                updateData.show_tips = req.body.show_tips;
            }

            // Enforce max 3 contacts
            const filledContacts = contactFields.filter(f => updateData[f] !== undefined && updateData[f] !== null);
            const { data: existingProfile, error: fetchProfileError } = await supabase.from('profiles').select('*').eq('id', req.user.id).maybeSingle();

            if (fetchProfileError) throw fetchProfileError;
            const existingFilled = contactFields.filter(f => existingProfile?.[f]);
            const finalFilled = [...new Set([...filledContacts.filter(f => updateData[f] !== null), ...existingFilled.filter(f => !(f in updateData))])];

            if (finalFilled.length > 3) {
                return res.status(400).json({ success: false, message: 'Maximum 3 contact links allowed.' });
            }

            // Username cannot be changed once set
            if (existingProfile?.username && req.body.username !== undefined && req.body.username !== existingProfile.username) {
                return res.status(400).json({ success: false, message: 'Username cannot be changed after it has been set.' });
            }

            // Auto-generate username from nama if not provided and user has none yet
            if (req.body.username === undefined && (!existingProfile?.username)) {
                let slug = req.user.nama
                    .trim()
                    .replace(/\s+/g, '')
                    .replace(/[^a-zA-Z0-9]/g, '');
                if (slug.length < 3) slug = 'user' + slug;
                if (slug.length > 50) slug = slug.slice(0, 50);

                // Ensure uniqueness with -1, -2 fallback
                let candidate = slug;
                let attempt = 0;
                while (true) {
                    const { data: conflict } = await supabase
                        .from('profiles')
                        .select('id')
                        .eq('username', candidate)
                        .neq('id', req.user.id)
                        .maybeSingle();
                    if (!conflict) break;
                    attempt++;
                    candidate = attempt === 1 ? `${slug}-1` : `${slug.slice(0, 47)}-${attempt}`;
                    if (attempt > 100) {
                        candidate = slug + '-' + Date.now().toString(36);
                        break;
                    }
                }
                updateData.username = candidate;
            }

            if (Object.keys(updateData).length === 0) {
                return res.status(400).json({ success: false, message: 'No fields to update.' });
            }

            // Check username uniqueness when user explicitly provides it (not auto-gen)
            if (req.body.username !== undefined && updateData.username) {
                const { data: existing } = await supabase
                    .from('profiles')
                    .select('id')
                    .eq('username', updateData.username)
                    .neq('id', req.user.id)
                    .maybeSingle();

                if (existing) {
                    return res.status(400).json({ success: false, message: 'Username already taken.' });
                }
            }

            const { data: updated, error: updateError } = await supabase
                .from('profiles')
                .update(updateData)
                .eq('id', req.user.id)
                .select()
                .maybeSingle();

            if (updateError) throw updateError;

            if (!updated) {
                return res.status(404).json({ success: false, message: 'Profile not found.' });
            }

            res.json({ success: true, profile: updated });
        } catch (error) {
            logger.error('Update profile error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to update profile.', ERROR_CODES.INTERNAL);
        }
    }
);

// Public profile (no auth) — for QR scanning
app.get('/api/profile/:username', async (req, res) => {
    try {
        const { data: profile, error } = await supabase
            .from('profiles')
            .select('id, nama, nisn, jurusan, bio, wa, ig, fb, tg')
            .eq('username', req.params.username)
            .maybeSingle();

        if (error || !profile) {
            return res.status(404).json({ success: false, message: 'Profile not found.' });
        }

        // Mask WA: show only last 4 digits
        const maskedWa = profile.wa
            ? '*'.repeat(Math.max(0, profile.wa.length - 4)) + profile.wa.slice(-4)
            : null;

        res.json({
            success: true,
            profile: {
                ...profile,
                wa: maskedWa,
                wa_raw: profile.wa || null,
            }
        });
    } catch (error) {
        logger.error('Fetch public profile error', { requestId: req.requestId, username: req.params.username, error: error.message });
        sendError(res, 500, 'Failed to fetch profile.', ERROR_CODES.INTERNAL);
    }
});

// ── ITEM ROUTES (all protected) ──────────────

// Get all items
app.get('/api/items', authMiddleware, (req, res, next) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    if (page === 1 && limit <= 20) {
        return cache(30)(req, res, next);
    }
    next();
}, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const from = (page - 1) * limit;
        const to = from + limit - 1;

        let query = supabase
            .from('items')
            .select(`
                *,
                reporter:profiles!items_reporter_fkey(id, nama, nisn, jurusan),
                claimer:profiles!items_claimer_fkey(id, nama),
                item_images(id, image_url)
            `)
            .order('reported_at', { ascending: false })
            .range(from, to);

        if (req.query.category && req.query.category !== 'all') {
            query = query.eq('category', req.query.category);
        }

        if (req.query.area_category && req.query.area_category !== 'all') {
            query = query.eq('area_category', req.query.area_category);
        }

        if (req.query.type && req.query.type !== 'all') {
            query = query.eq('type', req.query.type);
        }

        if (req.query.q) {
            const q = req.query.q.trim();
            query = query.or(`name.ilike.%${q}%,description.ilike.%${q}%,location.ilike.%${q}%`);
        }

        const { data: items, error, count } = await query;

        if (error) throw error;

        res.json({
            items,
            currentPage: page,
            totalPages: Math.ceil((count || 0) / limit),
            totalItems: count
        });
    } catch (error) {
        logger.error('Error fetching items', { requestId: req.requestId, error: error.message });
        sendError(res, 500, 'Failed to fetch items.', ERROR_CODES.INTERNAL);
    }
});

// Get MY reports
app.get('/api/items/mine', authMiddleware, async (req, res) => {
    try {
        const { data: items, error } = await supabase
            .from('items')
            .select(`
                *,
                reporter:profiles!items_reporter_fkey(id, nama, nisn, jurusan),
                claimer:profiles!items_claimer_fkey(id, nama),
                complaints(*),
                item_images(id, image_url)
            `)
            .eq('reporter', req.user.id)
            .order('reported_at', { ascending: false });

        if (error) throw error;

        res.json(items);
    } catch (error) {
        logger.error('Error fetching my items', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch your reports.', ERROR_CODES.INTERNAL);
    }
});

// Get item by claim code (used by Claim.vue after QR scan)
app.get('/api/items/claim-code/:code', authMiddleware, async (req, res) => {
    try {
        const { data: item, error } = await supabase
            .from('items')
            .select(`
                *,
                reporter:profiles!items_reporter_fkey(id, nama, nisn, jurusan),
                claimer:profiles!items_claimer_fkey(id, nama),
                complaints(*, user:profiles!complaints_user_id_fkey(nama, id)),
                item_images(id, image_url)
            `)
            .eq('claim_code', req.params.code)
            .single();

        if (error || !item) {
            return res.status(404).json({ success: false, message: 'Item not found' });
        }

        if (item.status === 'Returned') {
            return res.status(400).json({ success: false, message: 'This item has already been returned.' });
        }

        // If item is On Progress and has an assigned claimer, only that claimer can access
        if (item.status === 'On Progress' && item.claimer && item.claimer.id !== req.user.id) {
            return res.status(403).json({ success: false, message: 'This claim link belongs to another user.' });
        }

        res.json(item);
    } catch (error) {
        logger.error('Error fetching item by claim code', { requestId: req.requestId, code: req.params.code, error: error.message });
        sendError(res, 500, 'Failed to fetch item.', ERROR_CODES.INTERNAL);
    }
});

// Get matched found items for a lost item (3-pass text + category + GPS scoring)
app.get('/api/items/matches/:id', authMiddleware, async (req, res) => {
    try {
        const { data: lostItem, error: lostError } = await supabase
            .from('items')
            .select('*')
            .eq('id', req.params.id)
            .single();

        if (lostError || !lostItem) {
            return res.status(404).json({ success: false, message: 'Item not found' });
        }

        if (lostItem.type !== 'lost') {
            return res.json([]);
        }

        const matches = await findMatchingItems(lostItem, 'found', 5);
        res.json(matches);
    } catch (error) {
        logger.error('Error fetching matches', { requestId: req.requestId, itemId: req.params.id, error: error.message });
        sendError(res, 500, 'Failed to fetch matches.', ERROR_CODES.INTERNAL);
    }
});

// Dashboard recommendations: find matches for ALL of the current user's items
app.get('/api/items/recommendations', authMiddleware, async (req, res) => {
    try {
        const { data: myItems, error: myError } = await supabase
            .from('items')
            .select('*')
            .eq('reporter', req.user.id);

        if (myError) throw myError;
        if (!myItems || myItems.length === 0) return res.json([]);

        const seen = new Set();
        const allMatches = [];

        for (const item of myItems) {
            const targetType = item.type === 'lost' ? 'found' : 'lost';
            const matches = await findMatchingItems(item, targetType, 24);
            for (const m of matches) {
                if (!seen.has(m.id)) {
                    seen.add(m.id);
                    allMatches.push(m);
                }
            }
        }

        allMatches.sort((a, b) => b.score - a.score || (a.distance_meters || Infinity) - (b.distance_meters || Infinity));

        res.json(allMatches.slice(0, 24));
    } catch (error) {
        logger.error('Error fetching recommendations', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch recommendations.', ERROR_CODES.INTERNAL);
    }
});

// Get single item
app.get('/api/items/:id', authMiddleware, async (req, res) => {
    try {
        const { data: item, error } = await supabase
            .from('items')
            .select(`
                *,
                reporter:profiles!items_reporter_fkey(id, nama, nisn, jurusan),
                claimer:profiles!items_claimer_fkey(id, nama),
                complaints(*, user:profiles!complaints_user_id_fkey(nama, id)),
                messages(*),
                item_images(id, image_url)
            `)
            .eq('id', req.params.id)
            .single();

        if (error || !item) {
            return res.status(404).json({ success: false, message: 'Item not found' });
        }

        res.json(item);
    } catch (error) {
        logger.error('Error fetching item', { requestId: req.requestId, itemId: req.params.id, error: error.message });
        sendError(res, 500, 'Failed to fetch item.', ERROR_CODES.INTERNAL);
    }
});

// Create new item
app.post('/api/items',
    authMiddleware,
    (req, res, next) => {
        const ct = req.headers['content-type'] || '';
        if (ct.includes('multipart/form-data')) {
            upload.array('images', 3)(req, res, next);
        } else {
            next();
        }
    },
    [
        body('name').trim().notEmpty().withMessage('Item name is required')
            .isLength({ max: 200 }).withMessage('Item name too long'),
        body('location').trim().notEmpty().withMessage('Location is required')
            .isLength({ max: 200 }).withMessage('Location too long'),
        body('category').optional().trim().isLength({ max: 50 }),
        body('area_category').optional().trim().isLength({ max: 50 }),
        body('type').trim().notEmpty().isIn(['found', 'lost']).withMessage('Type must be found or lost'),
        body('description').optional().trim().isLength({ max: 500 }).withMessage('Description too long'),
        body('imageUrl').optional().isLength({ max: 3000000 }).withMessage('Image too large'),
        body('coordinates').optional({ nullable: true }),
        body('coordinates').custom((value) => {
            if (!value) return true;
            try {
                const parsed = typeof value === 'string' ? JSON.parse(value) : value;
                if (typeof parsed !== 'object') throw new Error('Must be an object');
                if (parsed.latitude && (parsed.latitude < -90 || parsed.latitude > 90)) throw new Error('Invalid latitude');
                if (parsed.longitude && (parsed.longitude < -180 || parsed.longitude > 180)) throw new Error('Invalid longitude');
            } catch (e) {
                throw new Error('Invalid coordinates format');
            }
            return true;
        }),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { name, location, category, area_category, type, description } = req.body;
            let coordinates;
            if (req.body.coordinates) {
                try {
                    coordinates = typeof req.body.coordinates === 'string' ? JSON.parse(req.body.coordinates) : req.body.coordinates;
                } catch (e) {
                    logger.warn('Failed to parse coordinates', { requestId: req.requestId });
                }
            }

            let mainImageUrl = null;
            const extraImageUrls = [];

            if (req.files && req.files.length > 0) {
                mainImageUrl = req.files[0].path;
                for (let i = 1; i < req.files.length; i++) {
                    extraImageUrls.push(req.files[i].path);
                }
            } else if (req.body.imageUrl) {
                mainImageUrl = req.body.imageUrl;
            }

            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            const bytes = crypto.randomBytes(8);
            let claimCode = '';
            for (let i = 0; i < 8; i++) {
                claimCode += chars[bytes[i] % chars.length];
            }

            const newItemData = {
                name,
                location,
                category: category || 'Others',
                area_category: area_category || null,
                type: type.toLowerCase(),
                description,
                image_url: mainImageUrl,
                reporter: req.user.id,
                claim_code: claimCode,
            };

            if (coordinates && typeof coordinates.latitude === 'number' && typeof coordinates.longitude === 'number') {
                newItemData.coordinates_lat = coordinates.latitude;
                newItemData.coordinates_lng = coordinates.longitude;
            }

            const { data: newItem, error } = await supabase
                .from('items')
                .insert(newItemData)
                .select()
                .single();

            if (error) throw error;

            // Save extra images to item_images table
            if (extraImageUrls.length > 0) {
                const imageRows = extraImageUrls.map(url => ({
                    item_id: newItem.id,
                    image_url: url,
                }));
                await supabase.from('item_images').insert(imageRows);
            }

            invalidateCache('/api/items');
            invalidateCache('/api/stats');

            logActivity({
                itemId: newItem.id,
                userId: req.user.id,
                actionType: 'item_created',
                description: `Melaporkan barang: ${newItem.name}`,
            });

            // Find matching items and create suggestion notifications
            try {
                const keywords = extractKeywords(newItem.name, newItem.description);
                const targetType = newItem.type === 'lost' ? 'found' : 'lost';
                const candidates = [];
                const seen = new Set();

                // Pass 1: same category + same area_category
                let q1 = supabase.from('items').select('*')
                    .eq('type', targetType).in('status', ['Available', 'On Progress', 'Returned'])
                    .eq('category', newItem.category).neq('id', newItem.id).neq('reporter', newItem.reporter);
                if (newItem.area_category) q1 = q1.eq('area_category', newItem.area_category);
                const { data: p1 } = await q1.limit(30);
                if (p1) for (const fi of p1) { seen.add(fi.id); candidates.push(fi); }

                // Pass 2: same category only
                if (newItem.area_category) {
                    let q2 = supabase.from('items').select('*')
                        .eq('type', targetType).in('status', ['Available', 'On Progress', 'Returned'])
                        .eq('category', newItem.category).neq('id', newItem.id).neq('reporter', newItem.reporter);
                    const { data: p2 } = await q2.limit(20);
                    if (p2) for (const fi of p2) { if (!seen.has(fi.id)) { seen.add(fi.id); candidates.push(fi); } }
                }

                // Pass 3: text keyword match across all categories
                if (keywords.length > 0) {
                    const orClauses = keywords.flatMap(kw => [
                        `name.ilike.%25${kw}%25`,
                        `description.ilike.%25${kw}%25`
                    ]).join(',');
                    let q3 = supabase.from('items').select('*')
                        .eq('type', targetType).in('status', ['Available', 'On Progress', 'Returned'])
                        .neq('category', newItem.category).neq('id', newItem.id).neq('reporter', newItem.reporter)
                        .or(orClauses);
                    const { data: p3 } = await q3.limit(10);
                    if (p3) for (const fi of p3) { if (!seen.has(fi.id)) { seen.add(fi.id); candidates.push(fi); } }
                }

                if (candidates.length > 0) {
                    // Notify the creator of the new item about the best match
                    let bestMatch = null;
                    let bestScore = -Infinity;

                    for (const fi of candidates) {
                        const score = computeScore(newItem, fi);
                        if (score > bestScore) {
                            bestScore = score;
                            bestMatch = fi;
                        }
                    }

                    if (bestMatch && bestScore >= 40) {
                        await supabase
                            .from('notifications')
                            .insert({
                                user_id: newItem.reporter,
                                type: 'suggestion',
                                item_id: bestMatch.id,
                                text: `This might be your item: ${sanitizeHtml(bestMatch.name)}`,
                                params: { itemName: sanitizeHtml(bestMatch.name) }
                            });
                        sendPushNotification(newItem.reporter, {
                            type: 'suggestion',
                            itemId: bestMatch.id,
                            title: 'Possible Match Found',
                            body: `This might be your item: ${sanitizeHtml(bestMatch.name)}`
                        });
                    }

                    // If found item created, also notify matching lost item reporters
                    if (newItem.type === 'found') {
                        for (const fi of candidates) {
                            const score = computeScore(newItem, fi);
                            if (score >= 40) {
                                await supabase
                                    .from('notifications')
                                    .insert({
                                        user_id: fi.reporter,
                                        type: 'suggestion',
                                        item_id: newItem.id,
                                        text: `We found a possible match: ${sanitizeHtml(newItem.name)}`,
                                        params: { itemName: sanitizeHtml(newItem.name) }
                                    });
                                sendPushNotification(fi.reporter, {
                                    type: 'suggestion',
                                    itemId: newItem.id,
                                    title: 'Possible Match Found',
                                    body: `We found a possible match: ${sanitizeHtml(newItem.name)}`
                                });
                            }
                        }
                    }
                }
            } catch (matchError) {
                logger.error('Error finding matches for new item', { requestId: req.requestId, itemId: newItem.id, error: matchError.message });
            }

            res.status(201).json({ success: true, item: newItem });
        } catch (error) {
            logger.error('Error creating item', { requestId: req.requestId, error: error.message, stack: error.stack });
            sendError(res, 500, 'Failed to create report.', ERROR_CODES.INTERNAL);
        }
    }
);

// Edit item (reporter only)
app.put('/api/items/:id',
    authMiddleware,
    upload.single('image'),
    [
        param('id').isUUID().withMessage('Invalid item ID'),
        body('name').optional().trim().notEmpty().withMessage('Item name is required')
            .isLength({ max: 200 }).withMessage('Item name too long'),
        body('location').optional().trim().notEmpty().withMessage('Location is required')
            .isLength({ max: 200 }).withMessage('Location too long'),
        body('category').optional().trim().isLength({ max: 50 }),
        body('area_category').optional({ values: 'falsy' }).trim().isLength({ max: 50 }),
        body('type').optional().trim().isIn(['found', 'lost']).withMessage('Type must be found or lost'),
        body('description').optional().trim().isLength({ max: 500 }).withMessage('Description too long'),
        body('imageUrl').optional().isLength({ max: 3000000 }).withMessage('Image too large'),
        body('coordinates').optional({ nullable: true }),
        body('coordinates').custom((value) => {
            if (!value) return true;
            try {
                const parsed = typeof value === 'string' ? JSON.parse(value) : value;
                if (typeof parsed !== 'object') throw new Error('Must be an object');
                if (parsed.latitude && (parsed.latitude < -90 || parsed.latitude > 90)) throw new Error('Invalid latitude');
                if (parsed.longitude && (parsed.longitude < -180 || parsed.longitude > 180)) throw new Error('Invalid longitude');
            } catch (e) {
                throw new Error('Invalid coordinates format');
            }
            return true;
        }),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { data: item, error: itemError } = await supabase
                .from('items')
                .select('*')
                .eq('id', req.params.id)
                .single();

            if (itemError || !item) {
                return res.status(404).json({ success: false, message: 'Item not found' });
            }

            if (item.reporter !== req.user.id) {
                return res.status(403).json({ success: false, message: 'Only the reporter can edit this item' });
            }

            if (item.status === 'Returned') {
                return res.status(400).json({ success: false, message: 'Cannot edit a returned item' });
            }

            const updateData = {};
            const editableFields = ['name', 'location', 'category', 'area_category', 'type', 'description'];

            for (const field of editableFields) {
                if (req.body[field] !== undefined) {
                    updateData[field] = req.body[field];
                }
            }

            // Detect when a file was expected but multer didn't parse one
            const isMultipart = req.headers['content-type']?.includes('multipart/form-data');
            if (isMultipart && !req.file && !req.body.imageUrl) {
                logger.warn('Edit item: multipart request but no file received', { requestId: req.requestId, itemId: req.params.id });
            }

            if (req.file) {
                updateData.image_url = req.file.path;
                logger.info('Edit item: new image uploaded', { requestId: req.requestId, itemId: req.params.id, path: req.file.path });
            } else if (req.body.imageUrl) {
                updateData.image_url = req.body.imageUrl;
            }

            // Handle explicit image removal
            if (req.body.remove_image === true || req.body.remove_image === 'true') {
                updateData.image_url = null;
            }

            // Delete old Cloudinary image if a new image is being set
            const oldImageUrl = updateData.image_url ? item.image_url : null;
            if (updateData.image_url === null) {
                // Image being removed — delete old if it was Cloudinary
                if (item.image_url?.includes('cloudinary')) {
                    try {
                        const publicId = extractCloudinaryPublicId(item.image_url);
                        if (publicId) await cloudinary.uploader.destroy(publicId);
                        logger.info('Deleted Cloudinary image (removal)', { requestId: req.requestId, itemId: req.params.id, publicId });
                    } catch (e) {
                        logger.error('Failed to delete Cloudinary image on removal', { requestId: req.requestId, itemId: req.params.id, error: e.message });
                    }
                }
            } else if (updateData.image_url && item.image_url?.includes('cloudinary')) {
                // Image being replaced — delete old
                try {
                    const publicId = extractCloudinaryPublicId(item.image_url);
                    if (publicId) {
                        await cloudinary.uploader.destroy(publicId);
                        logger.info('Deleted old Cloudinary image (replacement)', { requestId: req.requestId, itemId: req.params.id, publicId });
                    }
                } catch (e) {
                    logger.error('Failed to delete old Cloudinary image', { requestId: req.requestId, itemId: req.params.id, error: e.message });
                }
            }

            if (req.body.coordinates) {
                try {
                    const coords = typeof req.body.coordinates === 'string' ? JSON.parse(req.body.coordinates) : req.body.coordinates;
                    if (coords && typeof coords.latitude === 'number' && typeof coords.longitude === 'number') {
                        updateData.coordinates_lat = coords.latitude;
                        updateData.coordinates_lng = coords.longitude;
                    }
                } catch (e) {
                    logger.warn('Edit item: invalid coordinates', { requestId: req.requestId });
                }
            }

            if (req.body.remove_claimer === true || req.body.remove_claimer === 'true') {
                if (item.status !== 'On Progress') {
                    return res.status(400).json({ success: false, message: 'Can only remove claimer from items with On Progress status' });
                }
                updateData.claimer = null;
                updateData.status = 'Available';
                updateData.claim_photo = null;
                updateData.claim_notes = null;
                updateData.claimed_at = null;
            }

            if (Object.keys(updateData).length === 0) {
                return res.status(400).json({ success: false, message: 'No fields to update' });
            }

            // Log what's being updated
            logger.request(req, 'Edit item applying update', { itemId: req.params.id, fields: Object.keys(updateData) });

            const { data: updated, error: updateError } = await supabase
                .from('items')
                .update(updateData)
                .eq('id', req.params.id)
                .select();

            if (updateError) throw updateError;

            invalidateCache('/api/items');
            invalidateCache('/api/stats');

            if (req.body.remove_claimer === true || req.body.remove_claimer === 'true') {
                logActivity({
                    itemId: req.params.id,
                    userId: req.user.id,
                    actionType: 'claimer_removed',
                    description: `Menghapus klaimer dari barang: ${sanitizeHtml(item.name)}`,
                });
            } else {
                logActivity({
                    itemId: req.params.id,
                    userId: req.user.id,
                    actionType: 'item_edited',
                    description: `Mengedit barang: ${sanitizeHtml(item.name)}`,
                    metadata: { fields: Object.keys(updateData) },
                });
            }

            res.json({ success: true, item: updated[0] });
        } catch (error) {
            logger.error('Edit item error', { requestId: req.requestId, itemId: req.params.id, userId: req.user?.id, error: error.message, stack: error.stack });
            sendError(res, 500, 'Failed to edit item.', ERROR_CODES.INTERNAL);
        }
    }
);

// Start claim
app.post('/api/items/:id/start-claim',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            if (!checkUserRateLimit(req.user.id, 'start-claim')) {
                return sendError(res, 429, 'Too many claim attempts. Please slow down.', ERROR_CODES.BAD_REQUEST);
            }
            logger.request(req, 'Start claim', { itemId: req.params.id });

            const { data: updated, error } = await supabase
                .from('items')
                .update({
                    status: 'On Progress',
                    claimer: req.user.id
                })
                .eq('id', req.params.id)
                .eq('status', 'Available')
                .neq('reporter', req.user.id)
                .select();

            if (error) throw error;

            if (!updated || updated.length === 0) {
                const { data: existing } = await supabase
                    .from('items')
                    .select('reporter, status, claimer')
                    .eq('id', req.params.id)
                    .single();

                if (!existing) return res.status(404).json({ success: false, message: 'Item not found' });

                logger.error('Claim UPDATE returned 0 rows', {
                    itemId: req.params.id,
                    userId: req.user.id,
                    existingStatus: JSON.stringify(existing.status),
                    existingReporter: existing.reporter,
                    existingClaimer: existing.claimer,
                    matchReporter: existing.reporter === req.user.id,
                    matchClaimer: existing.claimer === req.user.id,
                });

                if (existing.reporter === req.user.id) {
                    return res.status(400).json({ success: false, message: 'You cannot claim your own item' });
                }
                if (existing.claimer === req.user.id) {
                    return res.status(400).json({ success: false, message: 'You have already claimed this item' });
                }
                if (existing.status === 'On Progress') {
                    return res.status(400).json({ success: false, message: 'This item is already being claimed by someone else.' });
                }
                if (existing.status === 'Returned') {
                    return res.status(400).json({ success: false, message: 'This item has already been returned.' });
                }
                if (existing.status === 'Available') {
                    logger.error('Claim failed — status IS Available but UPDATE affected 0 rows. Possible RLS/permission issue.', {
                        itemId: req.params.id, userId: req.user.id, reporterId: existing.reporter
                    });
                    return res.status(400).json({ success: false, message: 'Cannot claim item. Server configuration issue.' });
                }
                return res.status(400).json({ success: false, message: `Item status "${existing.status}" cannot be claimed.` });
            }

            const item = updated[0];

            invalidateCache('/api/items');
            invalidateCache('/api/stats');

            const { error: notifError } = await supabase
                .from('notifications')
                .insert({
                    user_id: item.reporter,
                    type: 'claim',
                    item_id: item.id,
                    text: `Someone wants to claim your item: ${sanitizeHtml(item.name)}`,
                    params: { itemName: sanitizeHtml(item.name) }
                });

            if (notifError) logger.error('Notification error', { requestId: req.requestId, itemId: req.params.id, error: notifError.message });

            sendPushNotification(item.reporter, {
                type: 'claim',
                itemId: item.id,
                title: 'New Claim Request',
                body: `Someone wants to claim your item: ${sanitizeHtml(item.name)}`
            });

            logActivity({
                itemId: item.id,
                userId: req.user.id,
                actionType: 'claim_started',
                description: `Mulai klaim barang: ${sanitizeHtml(item.name)}`,
            });

            res.json({ success: true, item });
        } catch (error) {
            logger.error('Start claim error', { requestId: req.requestId, itemId: req.params.id, userId: req.user?.id, error: error.message, stack: error.stack });
            sendError(res, 500, 'Failed to start claim.', ERROR_CODES.INTERNAL);
        }
    }
);

// Chat in item
app.post('/api/items/:id/chat',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            if (!checkUserRateLimit(req.user.id, 'chat', 20)) {
                return sendError(res, 429, 'Too many messages. Please slow down.', ERROR_CODES.BAD_REQUEST);
            }
            const text = req.body.text?.trim();
            if (!text || text.length > 1000) {
                return res.status(400).json({
                    success: false,
                    message: 'Invalid message'
                });
            }

            const { data: item, error: itemError } = await supabase
                .from('items')
                .select('*')
                .eq('id', req.params.id)
                .single();

            if (itemError || !item) return res.status(404).json({ success: false, message: 'Item not found' });

            const isReporter = item.reporter === req.user.id;
            const isClaimer = item.claimer === req.user.id;

            if (!isReporter && !isClaimer) {
                return res.status(403).json({ success: false, message: 'Only founder and claimer can chat' });
            }

            const { data: message, error: msgError } = await supabase
                .from('messages')
                .insert({
                    item_id: req.params.id,
                    sender: req.user.id,
                    text
                })
                .select()
                .single();

            if (msgError) throw msgError;

            const recipientId = isReporter ? item.claimer : item.reporter;
            if (recipientId) {
                const senderRole = isReporter ? 'founder' : 'claimer';
                await supabase
                    .from('notifications')
                    .insert({
                        user_id: recipientId,
                        type: 'message',
                        item_id: item.id,
                        text: `New message from ${isReporter ? 'Founder' : 'Claimer'} for item: ${sanitizeHtml(item.name)}`,
                        params: { itemName: sanitizeHtml(item.name), sender: senderRole }
                    });
                sendPushNotification(recipientId, {
                    type: 'message',
                    itemId: item.id,
                    title: 'New Message',
                    body: `New message from ${isReporter ? 'Founder' : 'Claimer'} for item: ${sanitizeHtml(item.name)}`
                });
            }

            io.to(`item-${item.id}`).emit('new-message', {
                itemId: item.id,
                message
            });

            logActivity({
                itemId: item.id,
                userId: req.user.id,
                actionType: 'message_sent',
                description: `Mengirim pesan tentang: ${sanitizeHtml(item.name)}`,
                metadata: { preview: text.slice(0, 100) },
            });

            logger.info('Chat message sent', { requestId: req.requestId, itemId: req.params.id, senderId: req.user.id });
            res.json({ success: true, messages: [message] });
        } catch (error) {
            logger.error('Chat error', { requestId: req.requestId, itemId: req.params.id, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to send message.', ERROR_CODES.INTERNAL);
        }
    }
);

// File a complaint
app.post('/api/items/:id/complaint',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
        body('reason').trim().notEmpty().withMessage('Reason is required')
            .isLength({ min: 3, max: 500 }).withMessage('Reason must be 3-500 characters'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            if (!checkUserRateLimit(req.user.id, 'complaint')) {
                return sendError(res, 429, 'Too many complaints. Please slow down.', ERROR_CODES.BAD_REQUEST);
            }
            const { reason } = req.body;

            const { data: inserted, error: complaintError } = await supabase
                .from('complaints')
                .insert({
                    item_id: req.params.id,
                    user_id: req.user.id,
                    reason
                })
                .select('id');

            if (complaintError) {
                if (complaintError.code === '23505' || !inserted?.length) {
                    return res.status(400).json({ success: false, message: 'You have already filed a complaint for this item' });
                }
                throw complaintError;
            }

            invalidateCache('/api/items');

            const { data: item } = await supabase
                .from('items')
                .select('name, reporter')
                .eq('id', req.params.id)
                .single();

            if (item) {
                await supabase
                    .from('notifications')
                    .insert({
                        user_id: item.reporter,
                        type: 'complaint',
                        item_id: item.id,
                        text: `Seseorang telah mengajukan komplain kepemilikan untuk barang "${sanitizeHtml(item.name)}".`,
                        params: { itemName: sanitizeHtml(item.name) }
                    });
                sendPushNotification(item.reporter, {
                    type: 'complaint',
                    itemId: item.id,
                    title: 'Complaint Filed',
                    body: `Seseorang telah mengajukan komplain kepemilikan untuk barang "${sanitizeHtml(item.name)}".`
                });
            }

            logActivity({
                itemId: req.params.id,
                userId: req.user.id,
                actionType: 'complaint_filed',
                description: `Mengajukan komplain untuk barang`,
                metadata: { reason: reason.slice(0, 200) },
            });

            res.json({ success: true, message: 'Complaint filed' });
        } catch (error) {
            logger.error('Complaint error', { requestId: req.requestId, itemId: req.params.id, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to file complaint.', ERROR_CODES.INTERNAL);
        }
    }
);

// Claim item (final)
app.post('/api/items/:id/claim',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { claim_photo, claim_notes } = req.body;
            const itemId = req.params.id;

            if (claim_photo && typeof claim_photo === 'string' && claim_photo.length > 5 * 1024 * 1024) {
                return res.status(400).json({ success: false, message: 'Photo too large (max 5MB base64)' });
            }

            const { data: item, error: itemError } = await supabase
                .from('items')
                .select('*')
                .eq('id', itemId)
                .single();

            if (itemError || !item) {
                return res.status(404).json({ success: false, message: 'Item not found' });
            }

            if (item.status !== 'Available' && item.status !== 'On Progress') {
                return res.status(400).json({ success: false, message: 'Item is not available for claim' });
            }

            if (item.reporter === req.user.id) {
                return res.status(400).json({ success: false, message: 'You cannot claim your own item' });
            }

            // If item is On Progress and has an assigned claimer, only that claimer can claim
            if (item.status === 'On Progress' && item.claimer && item.claimer !== req.user.id) {
                return res.status(403).json({ success: false, message: 'This item is being claimed by another user.' });
            }

            const { data: updated, error: updateError } = await supabase
                .from('items')
                .update({
                    status: 'Returned',
                    claimer: req.user.id,
                    claim_photo,
                    claim_notes,
                    claimed_at: new Date().toISOString()
                })
                .eq('id', itemId)
                .eq('status', item.status)
                .eq('claimer', item.claimer)
                .neq('reporter', req.user.id)
                .select();

            if (updateError) throw updateError;

            if (!updated || updated.length === 0) {
                return res.status(400).json({ success: false, message: 'Item could not be claimed. It may have been updated by someone else.' });
            }

            const wibNow = new Date(Date.now() + 7 * 60 * 60 * 1000);
            const y = wibNow.getUTCFullYear();
            const m = String(wibNow.getUTCMonth() + 1).padStart(2, '0');
            const d = String(wibNow.getUTCDate()).padStart(2, '0');
            const today = `${y}-${m}-${d}`;
            await supabase
                .from('counters')
                .upsert({ name: 'returnedAllTime', value: 0 }, { onConflict: 'name' })
                .then(async () => {
                    await supabase.rpc('increment_counter', { counter_name: 'returnedAllTime' });
                });

            await supabase
                .from('daily_counters')
                .upsert({ date: today, returned: 0 }, { onConflict: 'date' })
                .then(async () => {
                    await supabase.rpc('increment_daily_counter', { counter_date: today });
                });

            await supabase
                .from('notifications')
                .insert({
                    user_id: item.reporter,
                    type: 'resolved',
                    item_id: itemId,
                    text: `Barang "${sanitizeHtml(item.name)}" telah diklaim dan dikembalikan.`,
                    params: { itemName: sanitizeHtml(item.name) }
                });
            sendPushNotification(item.reporter, {
                type: 'resolved',
                itemId: itemId,
                title: 'Item Returned',
                body: `Barang "${sanitizeHtml(item.name)}" telah diklaim dan dikembalikan.`
            });

            invalidateCache('/api/items');
            invalidateCache('/api/stats');

            logActivity({
                itemId,
                userId: req.user.id,
                actionType: 'item_returned',
                description: `Mengembalikan barang: ${sanitizeHtml(item.name)}`,
            });

            return res.json({
                success: true,
                message: 'Item claimed successfully! Proof submitted.'
            });
        } catch (error) {
            logger.error('Error claiming item', { requestId: req.requestId, itemId: req.params.id, userId: req.user?.id, error: error.message, stack: error.stack });
            return sendError(res, 500, 'Failed to claim item.', ERROR_CODES.INTERNAL);
        }
    }
);

// Stats
app.get('/api/stats', authMiddleware, cache(60), async (req, res) => {
    try {
        const todayStart = new Date();
        todayStart.setHours(0, 0, 0, 0);

        const { count: currentlyLost } = await supabase
            .from('items')
            .select('*', { count: 'exact', head: true })
            .eq('type', 'lost')
            .neq('status', 'Returned');

        const { count: foundToday } = await supabase
            .from('items')
            .select('*', { count: 'exact', head: true })
            .eq('type', 'found')
            .gte('reported_at', todayStart.toISOString());

        const { data: counter } = await supabase
            .from('counters')
            .select('value')
            .eq('name', 'returnedAllTime')
            .single();

        res.json({
            currentlyLost: currentlyLost || 0,
            foundToday: foundToday || 0,
            returnedAllTime: counter?.value || 0
        });
    } catch (error) {
        logger.error('Error fetching stats', { requestId: req.requestId, error: error.message });
        sendError(res, 500, 'Failed to fetch stats.', ERROR_CODES.INTERNAL);
    }
});

// ── RATING ROUTES ──────────────────────────

app.post('/api/ratings',
    authMiddleware,
    [
        body('rating').isInt({ min: 1, max: 5 }).withMessage('Rating must be between 1 and 5'),
        body('comment').optional().trim().isLength({ max: 500 }).withMessage('Comment too long'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { rating, comment } = req.body;

            const { data: existing } = await supabase
                .from('ratings')
                .select('*')
                .eq('user_id', req.user.id)
                .single();

            if (existing) {
                const { data: updated, error } = await supabase
                    .from('ratings')
                    .update({ rating, comment: comment !== undefined ? comment : existing.comment })
                    .eq('user_id', req.user.id)
                    .select()
                    .single();

                if (error) throw error;

                invalidateCache('/api/ratings');
                return res.json({ success: true, message: 'Rating updated', rating: updated });
            }

            const { data: newRating, error } = await supabase
                .from('ratings')
                .insert({ user_id: req.user.id, rating, comment })
                .select()
                .single();

            if (error) throw error;

            invalidateCache('/api/ratings');
            res.json({ success: true, message: 'Rating submitted', rating: newRating });
        } catch (error) {
            logger.error('Submit rating error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to submit rating.', ERROR_CODES.INTERNAL);
        }
    }
);

app.get('/api/ratings', authMiddleware, cache(300), async (req, res) => {
    try {
        const { data: ratings, error } = await supabase
            .from('ratings')
            .select('*, user:profiles!ratings_user_id_fkey(nama)')
            .order('created_at', { ascending: false });

        if (error) throw error;

        const totalRatings = ratings.length;
        const averageRating = totalRatings > 0
            ? (ratings.reduce((sum, r) => sum + r.rating, 0) / totalRatings)
            : 0;

        const ratingsWithUsers = (ratings || []).map(r => r);

        res.json({
            averageRating: Math.round(averageRating * 10) / 10,
            totalRatings,
            ratings: ratingsWithUsers
        });
    } catch (error) {
        logger.error('Fetch ratings error', { requestId: req.requestId, error: error.message });
        sendError(res, 500, 'Failed to fetch ratings.', ERROR_CODES.INTERNAL);
    }
});

app.get('/api/ratings/mine', authMiddleware, async (req, res) => {
    try {
        const { data: rating, error } = await supabase
            .from('ratings')
            .select('*')
            .eq('user_id', req.user.id)
            .single();

        res.json({ rating: rating || null });
    } catch (error) {
        logger.error('Fetch my rating error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch your rating.', ERROR_CODES.INTERNAL);
    }
});

// ── DETAILED STATS ROUTE ─────────────────────

app.get('/api/stats/detailed', authMiddleware, heavyEndpointLimiter, cache(300), async (req, res) => {
    try {
        const wibOffset = 7 * 60 * 60 * 1000;
        const now = new Date();

        const wibNow = new Date(now.getTime() + wibOffset);
        const todayStartWIB = new Date(Date.UTC(wibNow.getUTCFullYear(), wibNow.getUTCMonth(), wibNow.getUTCDate()) - wibOffset);

        const sevenDaysAgoStart = new Date(todayStartWIB.getTime() - 6 * 24 * 60 * 60 * 1000);

        const { data: itemsSince, error } = await supabase
            .from('items')
            .select('*')
            .gte('reported_at', sevenDaysAgoStart.toISOString())
            .order('reported_at', { ascending: true });

        if (error) throw error;

        const itemsPerDay = [];
        for (let i = 0; i < 7; i++) {
            const dayStartUTC = new Date(sevenDaysAgoStart.getTime() + i * 24 * 60 * 60 * 1000);
            const dayEndUTC = new Date(dayStartUTC.getTime() + 24 * 60 * 60 * 1000 - 1);

            const dayStartWIB = new Date(dayStartUTC.getTime() + wibOffset);
            const y = dayStartWIB.getUTCFullYear();
            const m = String(dayStartWIB.getUTCMonth() + 1).padStart(2, '0');
            const d = String(dayStartWIB.getUTCDate()).padStart(2, '0');
            const dateStr = `${y}-${m}-${d}`;

            const dayItems = itemsSince.filter(item =>
                new Date(item.reported_at) >= dayStartUTC && new Date(item.reported_at) <= dayEndUTC
            );

            const { data: dc } = await supabase
                .from('daily_counters')
                .select('returned')
                .eq('date', dateStr)
                .single();

            itemsPerDay.push({
                date: dateStr,
                lost: dayItems.filter(i => i.type === 'lost').length,
                found: dayItems.filter(i => i.type === 'found').length,
                returned: dc?.returned || 0
            });
        }

        const { data: allItems } = await supabase.from('items').select('*');

        const locationCount = {};
        allItems.forEach(item => {
            const loc = item.location?.trim();
            if (loc) {
                locationCount[loc] = (locationCount[loc] || 0) + 1;
            }
        });
        const topLocations = Object.entries(locationCount)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5)
            .map(([location, count]) => ({ location, count }));

        const categoryCount = {};
        allItems.forEach(item => {
            const cat = item.category?.trim() || 'Others';
            categoryCount[cat] = (categoryCount[cat] || 0) + 1;
        });
        const topCategories = Object.entries(categoryCount)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5)
            .map(([category, count]) => ({ category, count }));

        const totalItems = allItems.length;
        const { data: counterDoc } = await supabase
            .from('counters')
            .select('value')
            .eq('name', 'returnedAllTime')
            .single();
        const totalReturned = counterDoc?.value || 0;
        const returnRate = totalItems > 0 ? Math.round((totalReturned / totalItems) * 100) : 0;

        const mostCommonLocation = topLocations[0]?.location || 'N/A';
        const mostLostCategory = topCategories[0]?.category || 'N/A';

        const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const dayCount = [0, 0, 0, 0, 0, 0, 0];
        allItems.forEach(item => {
            const day = new Date(item.reported_at).getDay();
            dayCount[day]++;
        });
        const busiestDayIndex = dayCount.indexOf(Math.max(...dayCount));
        const busiestDay = dayNames[busiestDayIndex];

        const returnedItems = allItems.filter(i => i.status === 'Returned' && i.claimed_at);
        let totalHours = 0;
        let avgHours = 0;
        if (returnedItems.length > 0) {
            totalHours = returnedItems.reduce((sum, item) => {
                const reported = new Date(item.reported_at).getTime();
                const claimed = new Date(item.claimed_at).getTime();
                return sum + (claimed - reported);
            }, 0);
            avgHours = totalHours / returnedItems.length / (1000 * 60 * 60);
        }
        const avgDaysToReturn = avgHours > 0 ? Math.round((avgHours / 24) * 10) / 10 : 0;

        res.json({
            itemsPerDay,
            topLocations,
            topCategories,
            funFacts: {
                mostCommonLocation,
                mostLostCategory,
                busiestDay,
                totalItems,
                totalReturned,
                returnRate,
                avgDaysToReturn
            }
        });
    } catch (error) {
        logger.error('Error fetching detailed stats', { requestId: req.requestId, error: error.message });
        sendError(res, 500, 'Failed to fetch detailed stats.', ERROR_CODES.INTERNAL);
    }
});

// Reassign claimer
app.post('/api/items/:id/resolve',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
        body('userId').isUUID().withMessage('Invalid user ID'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            logger.request(req, 'Resolve claim', { itemId: req.params.id, newClaimerId: req.body.userId });

            const { data: item, error } = await supabase
                .from('items')
                .select('*')
                .eq('id', req.params.id)
                .single();

            if (error || !item) return res.status(404).json({ success: false, message: 'Item not found' });

            if (item.reporter !== req.user.id) {
                return res.status(403).json({ success: false, message: 'Only founder can assign claimer' });
            }

            const { error: updateError } = await supabase
                .from('items')
                .update({ claimer: req.body.userId })
                .eq('id', req.params.id);

            if (updateError) {
                logger.error('Resolve update error', { requestId: req.requestId, itemId: req.params.id, error: updateError.message });
                throw updateError;
            }
            logger.request(req, 'Resolve update succeeded', { itemId: req.params.id });

            await supabase
                .from('complaints')
                .delete()
                .eq('item_id', req.params.id)
                .eq('user_id', req.body.userId);

            await supabase
                .from('notifications')
                .insert({
                    user_id: req.body.userId,
                    type: 'resolved',
                    item_id: req.params.id,
                    text: `You have been assigned as the claimer for: ${sanitizeHtml(item.name)}`,
                    params: { itemName: sanitizeHtml(item.name) }
                });
            sendPushNotification(req.body.userId, {
                type: 'resolved',
                itemId: req.params.id,
                title: 'Claimer Assigned',
                body: `You have been assigned as the claimer for: ${sanitizeHtml(item.name)}`
            });

            invalidateCache('/api/items');

            logActivity({
                itemId: req.params.id,
                userId: req.user.id,
                actionType: 'claimer_assigned',
                description: `Menunjuk pengklaim baru untuk barang: ${sanitizeHtml(item.name)}`,
            });

            logger.info('Claimer reassigned successfully', { requestId: req.requestId, itemId: req.params.id });
            res.json({ success: true, message: 'Claimer reassigned successfully' });
        } catch (error) {
            logger.error('Resolve error', { requestId: req.requestId, itemId: req.params.id, error: error.message, stack: error.stack });
            sendError(res, 500, 'Failed to reassign claimer.', ERROR_CODES.INTERNAL);
        }
    }
);

// ── PUSH SUBSCRIPTION ROUTES ──────────────────

// Get VAPID public key (so frontend can subscribe)
app.get('/api/push/vapid-key', (req, res) => {
    res.json({ publicKey: vapidPublicKey });
});

// Subscribe to push notifications
app.post('/api/push/subscribe',
    authMiddleware,
    [
        body('endpoint').trim().notEmpty().withMessage('Endpoint is required'),
        body('p256dh').trim().notEmpty().withMessage('p256dh is required'),
        body('auth').trim().notEmpty().withMessage('Auth is required'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { endpoint, p256dh, auth } = req.body;

            // Limit subscriptions per user (max 5)
            const { count, error: countError } = await supabase
                .from('push_subscriptions')
                .select('*', { count: 'exact', head: true })
                .eq('user_id', req.user.id);

            if (countError) throw countError;

            if (count >= 5) {
                return sendError(res, 400, 'Maximum 5 push subscriptions per account.', ERROR_CODES.BAD_REQUEST);
            }

            // Upsert: same endpoint = update, otherwise insert
            const { data: existing } = await supabase
                .from('push_subscriptions')
                .select('id')
                .eq('user_id', req.user.id)
                .eq('endpoint', endpoint)
                .maybeSingle();

            if (existing) {
                const { error: updateError } = await supabase
                    .from('push_subscriptions')
                    .update({ p256dh, auth, user_agent: req.headers['user-agent'] || null, updated_at: new Date().toISOString() })
                    .eq('id', existing.id);

                if (updateError) throw updateError;
            } else {
                const { error: insertError } = await supabase
                    .from('push_subscriptions')
                    .insert({
                        user_id: req.user.id,
                        endpoint,
                        p256dh,
                        auth,
                        user_agent: req.headers['user-agent'] || null,
                    });

                if (insertError) throw insertError;
            }

            res.json({ success: true });
        } catch (error) {
            logger.error('Push subscribe error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to subscribe.', ERROR_CODES.INTERNAL);
        }
    }
);

// Unsubscribe (remove a specific subscription)
app.post('/api/push/unsubscribe',
    authMiddleware,
    [
        body('endpoint').trim().notEmpty().withMessage('Endpoint is required'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { error } = await supabase
                .from('push_subscriptions')
                .delete()
                .eq('user_id', req.user.id)
                .eq('endpoint', req.body.endpoint);

            if (error) throw error;

            res.json({ success: true });
        } catch (error) {
            logger.error('Push unsubscribe error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
            sendError(res, 500, 'Failed to unsubscribe.', ERROR_CODES.INTERNAL);
        }
    }
);

// Test push notification (with 10-second delay for development)
app.post('/api/push/test', authMiddleware, async (req, res) => {
    res.json({ success: true, message: 'Push notification queued. Will arrive in 10 seconds.' });

    setTimeout(async () => {
        await sendPushNotification(req.user.id, {
            type: 'test',
            itemId: null,
            title: 'QReturn Test Notification',
            body: 'This is a test push notification sent 10 seconds after you requested it. Push is working!',
        });
        logger.info('Test push notification sent', { userId: req.user.id });
    }, 10000);
});

// ── NOTIFICATION ROUTES ─────────────────────

app.get('/api/notifications/unread-count', authMiddleware, async (req, res) => {
    try {
        const { count, error } = await supabase
            .from('notifications')
            .select('*', { count: 'exact', head: true })
            .eq('user_id', req.user.id)
            .eq('is_read', false);

        if (error) throw error;

        res.json({ count: count || 0 });
    } catch (error) {
        logger.error('Fetch unread count error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch unread count.', ERROR_CODES.INTERNAL);
    }
});

app.get('/api/notifications', authMiddleware, async (req, res) => {
    try {
        const { data: notifs, error } = await supabase
            .from('notifications')
            .select('*')
            .eq('user_id', req.user.id)
            .order('created_at', { ascending: false })
            .limit(20);

        if (error) throw error;

        res.json(notifs);
    } catch (error) {
        logger.error('Fetch notifications error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to fetch notifications.', ERROR_CODES.INTERNAL);
    }
});

app.delete('/api/notifications', authMiddleware, async (req, res) => {
    try {
        const { error } = await supabase
            .from('notifications')
            .delete()
            .eq('user_id', req.user.id);

        if (error) throw error;

        res.json({ success: true });
    } catch (error) {
        logger.error('Delete all notifications error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to delete notifications.', ERROR_CODES.INTERNAL);
    }
});

app.delete('/api/notifications/:id', authMiddleware, async (req, res) => {
    try {
        const { error } = await supabase
            .from('notifications')
            .delete()
            .eq('id', req.params.id)
            .eq('user_id', req.user.id);

        if (error) throw error;

        res.json({ success: true });
    } catch (error) {
        logger.error('Delete notification error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to delete notification.', ERROR_CODES.INTERNAL);
    }
});

app.post('/api/notifications/:id/read', authMiddleware, async (req, res) => {
    try {
        const { error } = await supabase
            .from('notifications')
            .update({ is_read: true })
            .eq('id', req.params.id)
            .eq('user_id', req.user.id);

        if (error) throw error;

        res.json({ success: true });
    } catch (error) {
        logger.error('Mark as read error', { requestId: req.requestId, userId: req.user?.id, error: error.message });
        sendError(res, 500, 'Failed to mark notification as read.', ERROR_CODES.INTERNAL);
    }
});

// Activity log for an item
app.get('/api/items/:id/activities',
    authMiddleware,
    [
        param('id').isUUID().withMessage('Invalid item ID'),
    ],
    handleValidationErrors,
    async (req, res) => {
        try {
            const { data: activities, error } = await supabase
                .from('item_activities')
                .select('*')
                .eq('item_id', req.params.id)
                .order('created_at', { ascending: false })
                .limit(50);

            if (error) throw error;

            res.json(activities || []);
        } catch (error) {
            logger.error('Fetch activities error', { requestId: req.requestId, itemId: req.params.id, error: error.message });
            sendError(res, 500, 'Failed to fetch activities.', ERROR_CODES.INTERNAL);
        }
    }
);

// Online users (presence)
app.get('/api/online-users',
    authMiddleware,
    async (req, res) => {
        res.json(Array.from(onlineUsers));
    }
);

// ── CATCH-ALL ────────────────────────────────
app.use('/api', (req, res) => {
    res.status(404).json({ success: false, message: 'API route not found.' });
});

// Multer/upload error handler (must come before the generic error handler)
app.use((err, req, res, next) => {
    if (err.code === 'LIMIT_FILE_SIZE') {
        logger.warn('File too large', { requestId: req.requestId, error: err.message });
        return sendError(res, 413, 'File too large. Maximum size is 10MB.', ERROR_CODES.FILE_TOO_LARGE);
    }
    if (err.code === 'LIMIT_UNEXPECTED_FILE') {
        logger.warn('Unexpected file field', { requestId: req.requestId, error: err.message });
        return sendError(res, 400, 'Unexpected file field.', ERROR_CODES.UPLOAD_FAILED);
    }
    if (err.name === 'MulterError' || err.message?.includes('multipart')) {
        logger.error('Upload error', { requestId: req.requestId, error: err.message, code: err.code });
        return sendError(res, 400, 'File upload failed: ' + err.message, ERROR_CODES.UPLOAD_FAILED);
    }
    // Generic error
    logger.error('Unhandled error', { requestId: req.requestId, error: err.message, stack: err.stack, method: req.method, path: req.originalUrl });
    sendError(res, 500, 'Internal server error.', ERROR_CODES.INTERNAL);
});

// ─────────────────────────────────────────────
// 5. SOCKET.IO & SERVER START
// ─────────────────────────────────────────────
// 6. CRON JOBS (Auto Cleanup)
// ─────────────────────────────────────────────
const runCleanupJob = async () => {
    logger.info('Running Auto-Cleanup Job...');

    try {
        const tenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
        const twoDaysAgo = new Date(Date.now() - 4 * 24 * 60 * 60 * 1000);

        const { data: expiredUnclaimed } = await supabase
            .from('items')
            .select('*')
            .neq('status', 'Returned')
            .lte('reported_at', tenDaysAgo.toISOString());

        const { data: expiredReturned } = await supabase
            .from('items')
            .select('*')
            .eq('status', 'Returned')
            .lte('claimed_at', twoDaysAgo.toISOString());

        const itemsToDelete = [
            ...(expiredUnclaimed || []),
            ...(expiredReturned || [])
        ];

        logger.info('Found items to delete', { count: itemsToDelete.length });

        for (const item of itemsToDelete) {
            if (item.image_url?.includes('cloudinary')) {
                try {
                    const publicId = extractCloudinaryPublicId(item.image_url);
                    if (publicId) {
                        await cloudinary.uploader.destroy(publicId);
                        logger.info('Deleted Cloudinary image', { publicId, itemId: item.id });
                    }
                } catch (cloudinaryErr) {
                    logger.error('Cloudinary delete failed', { itemId: item.id, error: cloudinaryErr.message });
                }
            }

            await supabase
                .from('notifications')
                .insert({
                    user_id: item.reporter,
                    type: 'system',
                    item_id: null,
                    text: `Laporan "${sanitizeHtml(item.name)}" telah dihapus otomatis.`,
                    params: { itemName: sanitizeHtml(item.name) }
                });
            sendPushNotification(item.reporter, {
                type: 'system',
                itemId: null,
                title: 'Item Auto-Deleted',
                body: `Laporan "${sanitizeHtml(item.name)}" telah dihapus otomatis.`
            });

            await supabase
                .from('notifications')
                .delete()
                .eq('item_id', item.id);

            await supabase
                .from('items')
                .delete()
                .eq('id', item.id);

            logger.info('Deleted item', { itemId: item.id, name: item.name });
        }

        logger.info('Auto-Cleanup complete');
    } catch (error) {
        logger.error('Auto-Cleanup failed', { error: error.message, stack: error.stack });
    }
};

cron.schedule('0 * * * *', async () => {
    await runCleanupJob();
});

server.listen(PORT, '0.0.0.0', () => {
    logger.info(`Server running`, { url: `http://localhost:${PORT}` });
    logger.info(`Health check`, { url: `http://localhost:${PORT}/api/health` });
});
