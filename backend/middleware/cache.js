const mcache = require('memory-cache');

const MAX_CACHE_SIZE = 100;

const cache = (durationSeconds) => {
    return (req, res, next) => {
        const key = '__express__' + (req.originalUrl || req.url);
        const cachedBody = mcache.get(key);
        if (cachedBody) {
            return res.json(cachedBody);
        }
        const originalJson = res.json.bind(res);
        res.json = (body) => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
                if (mcache.size() < MAX_CACHE_SIZE) {
                    mcache.put(key, body, durationSeconds * 1000);
                }
            }
            originalJson(body);
        };
        next();
    };
};

const invalidateCache = (pattern) => {
    const keys = mcache.keys();
    for (const key of keys) {
        if (key.includes(pattern)) {
            mcache.del(key);
        }
    }
};

module.exports = { cache, invalidateCache };
