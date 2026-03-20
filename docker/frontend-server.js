const http = require('http');
const fs = require('fs');
const path = require('path');
const { createProxyMiddleware } = require('http-proxy-middleware');

const PORT = process.env.FRONTEND_PORT || 13000;
let API_TARGET = process.env.API_TARGET;

if (!API_TARGET) {
    console.error('ERROR: API_TARGET environment variable is not set!');
    console.error('Please set the BACKEND_URL in .env.docker file');
    console.error('Example: BACKEND_URL=http://host.docker.internal:8006');
    process.exit(1);
}

API_TARGET = API_TARGET.replace(/\/$/, '');

console.log('========================================');
console.log('Frontend Server Configuration:');
console.log(`  Port: ${PORT}`);
console.log(`  API Target: ${API_TARGET}`);
console.log('========================================');

const proxy = createProxyMiddleware({
  target: API_TARGET,
  changeOrigin: true,
  pathRewrite: { '^/api': '' },
  onError: (err, req, res) => {
    console.error(`[Proxy Error] ${err.message}`);
    console.error(`[Proxy Error] Failed to connect to: ${API_TARGET}`);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: 'Proxy Error',
      message: `Failed to connect to backend: ${err.message}`,
      backend: API_TARGET
    }));
  },
  onProxyReq: (proxyReq, req) => {
    console.log(`[Proxy] ${req.method} ${req.url} -> ${API_TARGET}${req.url.replace('/api', '')}`);
  },
});

const STATIC_DIR = path.join(__dirname, 'dist');

if (!fs.existsSync(STATIC_DIR)) {
    console.error(`ERROR: Static directory not found: ${STATIC_DIR}`);
    process.exit(1);
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/api')) {
    proxy(req, res);
    return;
  }

  let filePath = path.join(STATIC_DIR, req.url === '/' ? 'index.html' : req.url);
  
  if (!fs.existsSync(filePath)) {
    filePath = path.join(STATIC_DIR, 'index.html');
  }
  
  const ext = path.extname(filePath);
  const contentTypes = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
  };
  
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    
    res.writeHead(200, { 
      'Content-Type': contentTypes[ext] || 'text/plain',
      'Cache-Control': 'public, max-age=31536000'
    });
    res.end(data);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Frontend server running on http://0.0.0.0:${PORT}`);
  console.log(`API proxy: /api/* -> ${API_TARGET}`);
});
