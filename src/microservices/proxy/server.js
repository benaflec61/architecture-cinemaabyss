const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://localhost:8080';
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || 'http://localhost:8081';
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || 'http://localhost:8082';
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '0', 10);

function getMoviesTarget() {
  if (!GRADUAL_MIGRATION) {
    return MOVIES_SERVICE_URL;
  }
  return Math.random() * 100 < MOVIES_MIGRATION_PERCENT
    ? MOVIES_SERVICE_URL
    : MONOLITH_URL;
}

function getTargetBase(pathname) {
  if (pathname.startsWith('/api/movies')) {
    return getMoviesTarget();
  }
  if (pathname.startsWith('/api/events')) {
    return EVENTS_SERVICE_URL;
  }
  return MONOLITH_URL;
}

const app = express();

app.get('/health', (req, res) => {
  res.status(200).json({ status: true });
});

app.use((req, res, next) => {
  const target = getTargetBase(req.path);

  createProxyMiddleware({
    target,
    changeOrigin: true,
    on: {
      error: (err, _req, res) => {
        console.error('Proxy error:', err.message);
        if (!res.headersSent) {
          res.status(502).json({ error: 'Bad Gateway' });
        }
      },
    },
  })(req, res, next);
});

app.listen(PORT, () => {
  console.log(`Proxy service listening on port ${PORT}`);
  console.log(`GRADUAL_MIGRATION=${GRADUAL_MIGRATION}, MOVIES_MIGRATION_PERCENT=${MOVIES_MIGRATION_PERCENT}`);
});
