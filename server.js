// App con login + roles + guardas y home protegida
import 'dotenv/config';
import express from 'express';
import path from 'node:path';
import { google } from 'googleapis';
import axios from 'axios';
import https from 'node:https';
import session from 'express-session';
import compression from 'compression';


// ===== Config de comportamiento (flags de debug)
const IN_PROD = process.env.NODE_ENV === 'production';
const DEBUG_ORDERS = process.env.DEBUG_ORDERS === '1';
const DEBUG_PAYMENTS = process.env.DEBUG_PAYMENTS === '1';
// Silenciar logs con prefijo "[tax]" si no está activado DEBUG_PAYMENTS
if (!DEBUG_PAYMENTS) {
const _origLog = console.log;
console.log = (...args) => {
try {
if (typeof args[0] === 'string' && args[0].startsWith('[tax]')) return;
} catch {}
_origLog(...args);
};
}


// Mostrar configuración sensible SOLO en modo debug
if (DEBUG_ORDERS) {
console.log('[cfg] ENABLE_MP_TAXES =', process.env.ENABLE_MP_TAXES);
console.log('[cfg] FETCH_PAYMENTS =', process.env.FETCH_PAYMENTS);
console.log('[cfg] MP_TOKEN_ENDING =', process.env.MP_ACCESS_TOKEN?.slice(-6));
}


// Store simple de usuarios
import { verifyPassword, createUser, getUserByEmail } from './data/authStore.js';


// Utilidades/servicios existentes del proyecto
import { limit } from './src/utils/limiter.js';
import {
setMeliToken,
getUserId,
searchOrdersByDate, // se usa en /debug/orders-raw
getOrderPayments,
getShipment,
} from './src/services/meli.js';
import { summarizePayments } from './src/services/payments.js';
import { HEADERS } from './src/utils/columns.js';
import { mapOrderToGridRow } from './src/utils/mapOrder.js';


const app = express();


// ===== Proxy / compresión / JSON
app.set('trust proxy', 1);
app.use(express.json({ limit: '1mb' }));
app.use(compression()); // acelera JSON/CSV
});

// (después de proteger "/" y "/index.html") servimos estáticos
app.use(express.static('public'));

// ====== Arranque
const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`ML backend listo en http://localhost:${PORT}`);
});

