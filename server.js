// App con login + roles + guardas y home protegida
import 'dotenv/config';
import express from 'express';
import path from 'node:path';
import { google } from 'googleapis';
import axios from 'axios';
import https from 'node:https';
import session from 'express-session';
import compression from 'compression';

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
app.use(express.json());
app.use(compression()); // ↓ comprime JSON/CSV y acelera red

// ===== Config de sesión (requerido para login)
app.use(session({
  secret: process.env.SESSION_SECRET || 'dev-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { httpOnly: true, sameSite: 'lax', secure: false, maxAge: 1000 * 60 * 60 * 8 } // 8h
}));

// ===== Clientes HTTP con keep-alive
const httpsAgentKeepAlive = new https.Agent({ keepAlive: true, maxSockets: 50 });
const meliAxios = axios.create({
  baseURL: 'https://api.mercadolibre.com',
  timeout: 15000,
  httpsAgent: httpsAgentKeepAlive,
});
const mpAxios = axios.create({
  baseURL: 'https://api.mercadopago.com',
  timeout: 10000,
  httpsAgent: httpsAgentKeepAlive,
});

// ===== Envíos: reglas y reparto
const SHIP_FREE_THRESHOLD = Number(process.env.SHIP_FREE_THRESHOLD || 32999);
const SHIP_RULE_USE_BASE = process.env.SHIP_RULE_USE_BASE !== 'false';
const SHIP_THRESHOLD_INCLUSIVE = (process.env.SHIP_THRESHOLD_INCLUSIVE ?? 'true') !== 'false';
const SHIP_SPLIT_MODE = (process.env.SHIP_SPLIT_MODE || 'by_price').toLowerCase();

// ===== Config de comportamiento
const TZ_OFFSET = process.env.TZ_OFFSET || '-03:00';
const DEFAULT_DATE_BY = (process.env.DEFAULT_DATE_BY || 'both').toLowerCase();
const PAID_LOOKBACK_DAYS = Number(process.env.PAID_LOOKBACK_DAYS || 5);
const FETCH_PAYMENTS_DEFAULT = process.env.FETCH_PAYMENTS === 'true';
const ENABLE_MP_TAXES = process.env.ENABLE_MP_TAXES !== 'false';
const INCLUDE_SHIPMENT_DEFAULT = process.env.INCLUDE_SHIPMENT_DEFAULT !== 'false';
const CSV_FMT_DEFAULT = (process.env.CSV_FMT_DEFAULT || 'ars').toLowerCase();
const DEBUG_ORDERS = process.env.DEBUG_ORDERS === '1';

// ===== Utilidades de fecha (AR -03:00)
function dateWithOffset(raw, endOfDay) {
  if (!raw) return null;
  if (/[T]\d{2}:\d{2}/.test(raw) && /Z|[+\-]\d{2}:?\d{2}$/.test(raw)) return new Date(raw);
  const m = /^(\d{2})[\/\-](\d{2})[\/\-](\d{4})$/.exec(raw);
  let iso = raw;
  if (m) iso = `${m[3]}-${m[2]}-${m[1]}`;
  const time = endOfDay ? '23:59:59.999' : '00:00:00.000';
  return new Date(`${iso}T${time}${TZ_OFFSET}`);
}
function parseRangeAR(q) {
  const fromD = dateWithOffset(q.from, false);
  const toD   = dateWithOffset(q.to ?? q.until, true);
  return {
    fromMs: fromD ? +fromD : Number.NEGATIVE_INFINITY,
    toMs:   toD   ? +toD   : Number.POSITIVE_INFINITY,
    fromISO: fromD ? fromD.toISOString() : undefined,
    toISO:   toD   ? toD.toISOString()   : undefined,
    fromYMD: fromD ? fromD.toISOString().slice(0,10) : '',
    toYMD:   toD   ? toD.toISOString().slice(0,10)   : '',
  };
}

// ===== Índices de columnas
const IDX = {
  ID_VENTA: HEADERS.indexOf('ID DE VENTA'),
  FECHA: HEADERS.indexOf('FECHA'),
  TITULO: HEADERS.indexOf('TITULO'),
  PRECIO_FINAL_COMPRADOR: HEADERS.indexOf('Precio Final'),
  NETO: HEADERS.indexOf('NETO'),
  COSTO: HEADERS.indexOf('COSTO'),
  GANANCIA: HEADERS.indexOf('GANANCIA'),
  PRECIO_BASE: HEADERS.indexOf('PRECIO BASE'),
  DESCUENTO_PCT: HEADERS.indexOf('% DESCUENTO'),
  PRECIO_FINAL_SIN_INTERES: HEADERS.indexOf('PRECIO FINAL'),
  ENVIO: HEADERS.indexOf('ENVIO'),
  IMPUESTO: HEADERS.indexOf('IMPUESTO'),
  CARGO: HEADERS.indexOf('CARGO X VENTA'),
  CUOTAS: HEADERS.indexOf('CUOTAS'),
};

// ===== Helpers números
function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const round1 = (n) => Math.round((Number(n) + Number.EPSILON) * 10) / 10;

// Fix de NETO si hay "PRECIO FINAL"
function fixRowNETO(row){
  const pfIdxUpper = HEADERS.indexOf('PRECIO FINAL');
  const pfIdxLower = HEADERS.indexOf('Precio Final');
  const pfIdx = (pfIdxUpper >= 0) ? pfIdxUpper : pfIdxLower;
  if (pfIdx < 0) return row;
  const pf    = toNum(row[pfIdx]);
  const cargo = toNum(row[IDX.CARGO]);
  const envio = toNum(row[IDX.ENVIO]);
  const imp   = toNum(row[IDX.IMPUESTO]);
  if (!pf) return row;
  const neto = pf - cargo - envio - imp;
  if (IDX.NETO >= 0) row[IDX.NETO] = round2(neto);
  return row;
}

// ===== MP taxes (opcional)
async function computeTaxesFromMercadoPago(payments = []) {
  const mpToken = process.env.MP_ACCESS_TOKEN;
  if (!mpToken) return null;
  let totalTaxes = 0;
  const seen = new Set();
  for (const p of payments) {
    const status = String(p?.status || '').toLowerCase();
    const statusDetail = String(p?.status_detail || '').toLowerCase();
    const approved = status === 'approved' || statusDetail === 'accredited';
    const paymentId = p?.id;
    if (!approved || !paymentId || seen.has(paymentId)) continue;
    seen.add(paymentId);
    try {
      const r = await mpAxios.get(`/v1/payments/${paymentId}`, {
        headers: { Authorization: `Bearer ${mpToken}` },
      });
      const charges = Array.isArray(r.data?.charges_details) ? r.data.charges_details : [];
      for (const ch of charges) {
        if (String(ch?.type || '').toLowerCase() === 'tax') {
          const amt = ch?.amounts?.original ?? ch?.amounts?.payer ?? ch?.amounts?.collector ?? ch?.amount ?? 0;
          totalTaxes += Number(amt) || 0;
        }
      }
    } catch {}
  }
  return Math.round(totalTaxes * 100) / 100;
}

// Fecha de pago a partir de payments y/o date_closed
function getPaidMs(order, payments = []) {
  const cands = [];
  for (const p of payments) {
    const status = String(p?.status || '').toLowerCase();
    const cand = p?.date_approved || p?.date_accredited || p?.date_created;
    if ((status === 'approved' || status === 'accredited') && cand) {
      const ms = Date.parse(cand);
      if (!Number.isNaN(ms)) cands.push(ms);
    }
  }
  if (order?.date_closed) {
    const ms = Date.parse(order.date_closed);
    if (!Number.isNaN(ms)) cands.push(ms);
  }
  return cands.length ? Math.min(...cands) : NaN;
}

// ===== Admin-key sólo para /debug/*
const ADMIN_KEY = process.env.ADMIN_KEY || '';
function requireAdminKey(req, res, next) {
  const key = req.query.key || req.get('x-admin-key');
  if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(401).json({ error: 'unauthorized' });
  next();
}
app.use('/debug', requireAdminKey);

// ===== Token/Sheets cacheados
let _mlTokenCache = { value: null, exp: 0 };
let _costsCache   = { map: {}, exp: 0 };
let _sellerCache  = { token: '', id: null, exp: 0 };

async function getSheetsClient() {
  const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS || './credentials/service-account.json';
  const auth = new google.auth.GoogleAuth({
    keyFile,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
  });
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}
async function getTokenFromSheetsCached() {
  const now = Date.now();
  if (_mlTokenCache.value && now < _mlTokenCache.exp) return _mlTokenCache.value;
  const sheets = await getSheetsClient();
  const spreadsheetId = process.env.GS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';
  const sheetName = process.env.GS_TOKENS_SHEET || 'Tokens';
  const cell = process.env.GS_TOKENS_CELL || 'A2';
  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetName}!${cell}` });
  const token = resp.data.values?.[0]?.[0] || '';
  _mlTokenCache = { value: token, exp: now + 5 * 60 * 1000 };
  return token;
}
async function getCostsMapFromSheetsCached() {
  const now = Date.now();
  if (_costsCache.map && now < _costsCache.exp) return _costsCache.map;
  const sheets = await getSheetsClient();
  const spreadsheetId = process.env.GS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';
  const sheetName = process.env.GS_COSTS_SHEET || 'Comparador';
  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetName}!A2:M` });
  const rows = resp.data.values || [];
  const map = {};
  for (const row of rows) {
    const id = (row[0] || '').trim();
    const costRaw = row[12];
    if (!id) continue;
    const cleaned = String(costRaw ?? '')
      .replace(/\s+/g,'')
      .replace(/[^\d,.\-]/g,'')
      .replace(/\./g,'')
      .replace(',', '.');
    const cost = Number(cleaned);
    if (Number.isFinite(cost)) map[id] = cost;
  }
  _costsCache = { map, exp: now + 5 * 60 * 1000 };
  return map;
}
async function getCachedSellerId(token) {
  const now = Date.now();
  if (_sellerCache.token === token && now < _sellerCache.exp && _sellerCache.id) return _sellerCache.id;
  setMeliToken(token);
  const id = await getUserId();
  _sellerCache = { token, id, exp: now + 5 * 60 * 1000 };
  return id;
}

// ===== Búsqueda por ID o Pack (paralelizada)
async function fetchOrdersByIdsOrPacks(ids = [], sellerId, token) {
  const out = new Map();
  await Promise.all(ids.map(async raw => {
    const id = String(raw || '').trim();
    if (!id) return;
    const tryOrder = meliAxios.get(`/orders/${id}`, {
      headers: { Authorization: `Bearer ${token}` }
    }).catch(() => null);
    const tryPack = meliAxios.get(
      `/orders/search?seller=${sellerId}&pack_id=${encodeURIComponent(id)}&limit=50&offset=0`,
      { headers: { Authorization: `Bearer ${token}` } }
    ).catch(() => null);
    const [ro, rp] = await Promise.all([tryOrder, tryPack]);
    if (ro?.data?.id) out.set(ro.data.id, ro.data);
    const results = rp?.data?.results || [];
    for (const o of results) if (o?.id) out.set(o.id, o);
  }));
  return Array.from(out.values());
}

// ====== Reglas de envío por grupo (shipment/pack)
function pfIndex() {
  const up = HEADERS.indexOf('PRECIO FINAL');
  const lo = HEADERS.indexOf('Precio Final');
  return { pfUp: up, pfLo: lo, pfAny: (up >= 0 ? up : lo) };
}
function priceForThreshold(row) {
  const { pfAny } = pfIndex();
  const base = toNum(row[IDX.PRECIO_BASE]);
  const pf   = toNum(row[pfAny]);
  return SHIP_RULE_USE_BASE ? (base || pf) : (pf || base);
}
function isFreeByThreshold(row) {
  const basis = priceForThreshold(row);
  return SHIP_THRESHOLD_INCLUSIVE ? (basis <= SHIP_FREE_THRESHOLD) : (basis < SHIP_FREE_THRESHOLD);
}
function recomputeNetoYGanancia(row) {
  const { pfAny } = pfIndex();
  const pf    = toNum(row[pfAny]);
  const cargo = toNum(row[IDX.CARGO]);
  const envio = toNum(row[IDX.ENVIO]);
  const imp   = toNum(row[IDX.IMPUESTO]);
  if (pf && IDX.NETO >= 0) row[IDX.NETO] = round2(pf - cargo - envio - imp);
  const cost = toNum(row[IDX.COSTO]);
  const neto = toNum(row[IDX.NETO]);
  if (cost > 0 && neto && IDX.GANANCIA >= 0) {
    row[IDX.GANANCIA] = round1(((neto - cost) / cost) * 100);
  }
}
function groupKey(order, shipmentData) {
  return shipmentData?.id || order?.pack_id || order?.shipping?.id || null;
}
function applyShippingSplit(items) {
  // Sin grupo → ver si está libre por umbral
  for (const it of items) {
    const key = groupKey(it.order, it.shipment);
    if (!key) {
      if (isFreeByThreshold(it.row)) {
        it.row[IDX.ENVIO] = 0;
        recomputeNetoYGanancia(it.row);
      }
    }
  }
  const groups = new Map();
  for (const it of items) {
    const key = groupKey(it.order, it.shipment);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(it);
  }
  for (const arr of groups.values()) {
    if (arr.length === 1) {
      const it = arr[0];
      if (isFreeByThreshold(it.row)) {
        it.row[IDX.ENVIO] = 0;
        recomputeNetoYGanancia(it.row);
      }
      continue;
    }
    const rawVals = arr.map(it => toNum(it.row[IDX.ENVIO]));
    const totalShip = Math.max(...rawVals, 0);
    if (!totalShip) {
      arr.forEach(it => { it.row[IDX.ENVIO] = 0; recomputeNetoYGanancia(it.row); });
      continue;
    }
    const eligible = arr.filter(it => !isFreeByThreshold(it.row));
    if (eligible.length === 0) {
      arr.forEach(it => { it.row[IDX.ENVIO] = 0; recomputeNetoYGanancia(it.row); });
      continue;
    }
    arr.forEach(it => { it.row[IDX.ENVIO] = 0; });
    if (SHIP_SPLIT_MODE === 'first') {
      eligible[0].row[IDX.ENVIO] = round2(totalShip);
    } else if (SHIP_SPLIT_MODE === 'even') {
      const baseCents = Math.floor((totalShip * 100) / eligible.length);
      let remainder = Math.round(totalShip * 100) - baseCents * eligible.length;
      for (const it of eligible) {
        let cents = baseCents;
        if (remainder > 0) { cents += 1; remainder -= 1; }
        it.row[IDX.ENVIO] = round2(cents / 100);
      }
    } else {
      const priceBasis = (it) => {
        const up = IDX.PRECIO_FINAL_SIN_INTERES;
        const { pfAny } = pfIndex();
        const v = toNum(up >= 0 ? it.row[up] : it.row[pfAny]);
        return v > 0 ? v : 0;
      };
      const weights = eligible.map(priceBasis);
      const sumW = weights.reduce((a,b) => a + b, 0);
      if (sumW <= 0) {
        const baseCents = Math.floor((totalShip * 100) / eligible.length);
        let remainder = Math.round(totalShip * 100) - baseCents * eligible.length;
        for (const it of eligible) {
          let cents = baseCents;
          if (remainder > 0) { cents += 1; remainder -= 1; }
          it.row[IDX.ENVIO] = round2(cents / 100);
        }
      } else {
        let assignedCents = 0;
        const targetCents = Math.round(totalShip * 100);
        for (let i = 0; i < eligible.length; i++) {
          let cents = (i < eligible.length - 1)
            ? Math.floor(targetCents * (weights[i] / sumW))
            : (targetCents - assignedCents);
          assignedCents += cents;
          eligible[i].row[IDX.ENVIO] = round2(cents / 100);
        }
      }
    }
    arr.forEach(it => recomputeNetoYGanancia(it.row));
  }
}

// ====== BOOTSTRAP ADMIN (si no existe y hay envs)
(async () => {
  const email = process.env.ADMIN_EMAIL;
  const pass  = process.env.ADMIN_PASSWORD;
  if (email && pass && !getUserByEmail(email)) {
    await createUser({ email, password: pass, roles: ['ADMIN'] });
    console.log(`[auth] Admin inicial creado: ${email}`);
  }
})();

// ====== HELPERS DE AUTORIZACIÓN (session)
function requireAuth(req, res, next) {
  if (req.session?.user) return next();
  return res.status(401).json({ error: 'auth_required' });
}
function requireRole(roles) {
  const needed = Array.isArray(roles) ? roles : [roles];
  return (req, res, next) => {
    const u = req.session?.user;
    if (!u) return res.status(401).json({ error: 'auth_required' });
    const ok = u.roles?.some(r => needed.includes(r)) || u.roles?.includes('ADMIN');
    if (!ok) return res.status(403).json({ error: 'forbidden' });
    next();
  };
}

// ====== ENDPOINTS DE AUTH
app.post('/auth/login', async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: 'missing_fields' });
  const user = await verifyPassword(email, password);
  if (!user) return res.status(401).json({ error: 'invalid_credentials' });
  req.session.user = user;
  res.json({ ok: true, user });
});
app.post('/auth/logout', (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});
app.get('/auth/me', (req, res) => {
  const u = req.session?.user;
  if (!u) return res.status(401).json({ error: 'auth_required' });
  res.json({ user: u });
});

// ====== GUARDAS POR ROL
app.use(['/orders', '/orders.csv'], requireRole(['VENTAS', 'ADMIN']));
app.use(['/ml/visits', '/ml/visits-user'], requireRole(['VENTAS', 'ADMIN']));
app.use('/ml/labels', requireRole(['ENVIOS', 'ADMIN']));

// ====== ML: VISITAS POR ITEM (agrupadas por día)
function diffDaysInclusive(fromYMD, toYMD){
  const a = new Date(fromYMD+'T00:00:00.000Z');
  const b = new Date(toYMD  +'T00:00:00.000Z');
  return Math.max(1, Math.round((b - a) / 86400000) + 1);
}
function buildDenseDailySeries(fromYMD, toYMD, sparse = []) {
  const map = new Map(sparse.map(x => [x.label, Number(x.count) || 0]));
  const start = new Date(fromYMD + 'T00:00:00.000Z');
  const end   = new Date(toYMD   + 'T00:00:00.000Z');
  const out = [];
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd= String(d.getUTCDate()).padStart(2, '0');
    const label = `${y}-${m}-${dd}`;
    out.push({ label, count: map.get(label) ?? 0 });
  }
  return out;
}

app.get('/ml/visits', async (req, res) => {
  try{
    const item_id = String(req.query.item_id || '').trim();
    const from    = String(req.query.from || '').trim();
    const to      = String(req.query.to   || '').trim();
    if (!item_id || !from || !to){
      return res.status(400).json({ error: 'Falta item_id, from o to (YYYY-MM-DD)' });
    }
    const last   = diffDaysInclusive(from, to);
    const ending = `${to}T23:59:59.999Z`;
    const token  = await getTokenFromSheetsCached();

    const url = `/items/${encodeURIComponent(item_id)}/visits/time_window?last=${last}&unit=day&ending=${encodeURIComponent(ending)}`;
    const r = await meliAxios.get(url, { headers: { Authorization: `Bearer ${token}` } });

    const api = r.data || {};
    let seriesSparse = Array.isArray(api.results) ? api.results.map(pt => {
      const d  = new Date(pt.date);
      const y  = d.getUTCFullYear();
      const m  = String(d.getUTCMonth()+1).padStart(2,'0');
      const dd = String(d.getUTCDate()).padStart(2,'0');
      return { label: `${y}-${m}-${dd}`, count: Number(pt.total)||0 };
    }) : [];

    seriesSparse = seriesSparse.filter(x => x.label >= from && x.label <= to)
                               .sort((a,b) => a.label.localeCompare(b.label));
    const series = buildDenseDailySeries(from, to, seriesSparse);
    const total  = series.reduce((s,x)=> s + x.count, 0);

    res.json({ item_id, from, to, unit:'day', last, ending, total, series,
      source: 'meli/items/visits/time_window' });
  } catch(err){
    if (err.response) {
      return res.status(err.response.status).json({
        error: 'ML visits error',
        detail: err.response.data
      });
    }
    console.error('ml/visits error', err);
    res.status(500).json({ error: 'internal', detail: String(err?.message || err) });
  }
});

// ====== ML: VISITAS DE LA CUENTA (agrupadas por día)
app.get('/ml/visits-user', async (req, res) => {
  try{
    const from = String(req.query.from || '').trim();
    const to   = String(req.query.to   || '').trim();
    const unit = 'day';
    if (!from || !to) return res.status(400).json({ error: 'Falta from o to (YYYY-MM-DD)' });

    const last   = diffDaysInclusive(from, to);
    const ending = `${to}T23:59:59.999Z`;

    const token    = await getTokenFromSheetsCached();
    const sellerId = await getCachedSellerId(token);

    const url = `/users/${encodeURIComponent(sellerId)}/items_visits/time_window?last=${last}&unit=${unit}&ending=${encodeURIComponent(ending)}`;
    const r = await meliAxios.get(url, { headers: { Authorization: `Bearer ${token}` } });

    const api = r.data || {};
    let seriesSparse = Array.isArray(api.results) ? api.results.map(p => {
      const d = new Date(p.date);
      const y = d.getUTCFullYear();
      const m = String(d.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(d.getUTCDate()).padStart(2, '0');
      return { label: `${y}-${m}-${dd}`, count: Number(p.total) || 0 };
    }).sort((a, b) => a.label.localeCompare(b.label)) : [];

    seriesSparse = seriesSparse.filter(p => p.label >= from && p.label <= to);
    const series = buildDenseDailySeries(from, to, seriesSparse);
    const total  = series.reduce((s, x) => s + x.count, 0);

    res.json({ user_id: String(sellerId), from, to, unit, last, ending, total, series,
      source: 'meli/users/items_visits/time_window' });
  }catch(err){
    console.error('ml/visits-user error', err);
    res.status(500).json({ error: 'internal', detail: String(err && err.message || err) });
  }
});

// ====== Gestión de envíos (stubs por ahora)
app.get('/ml/labels/pending', (req, res) => {
  res.json({ rows: [] });
});
app.post('/ml/labels/print', (req, res) => {
  res.status(501).json({ error: 'not_implemented' });
});

// ====== /orders (JSON para la grilla)
app.get('/orders', async (req, res) => {
  try {
    // Fallback de fechas mínimo: si faltan, usar HOY
    const pad = n => String(n).padStart(2, '0');
    const now = new Date();
    const ymdToday = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`;
    if (!req.query.from) req.query.from = ymdToday;
    if (!req.query.to)   req.query.to   = ymdToday;
    if (!req.query.dateBy) req.query.dateBy = DEFAULT_DATE_BY;
    if (DEBUG_ORDERS) console.log('[orders] rango efectivo:', req.query.from, '→', req.query.to, 'dateBy=', req.query.dateBy);

    const token    = await getTokenFromSheetsCached();
    const costsMap = await getCostsMapFromSheetsCached();
    const sellerId = await getCachedSellerId(token);

    const rawMode = String(req.query.dateBy || req.query.mode || DEFAULT_DATE_BY).toLowerCase();
    const dateBy = (rawMode === 'created' || rawMode === 'paid' || rawMode === 'both') ? rawMode : DEFAULT_DATE_BY;

    // Parse del rango (puede no devolver ISO en algunos casos → reconstruimos)
    const { fromISO, toISO, fromMs, toMs, fromYMD, toYMD } = parseRangeAR(req.query);
    const toIsoFromYMD = (ymd, endOfDay=false) => {
      const time = endOfDay ? '23:59:59.999' : '00:00:00.000';
      return new Date(`${ymd}T${time}${TZ_OFFSET}`).toISOString();
    };
    const _searchFromISO = fromISO || toIsoFromYMD(req.query.from, false);
    const _searchToISO   = toISO   || toIsoFromYMD(req.query.to,   true);

    // Base ms por si fromMs/toMs no son válidos
    const baseFromMs = Number.isFinite(fromMs) ? fromMs : new Date(_searchFromISO).getTime();
    const baseToMs   = Number.isFinite(toMs)   ? toMs   : new Date(_searchToISO).getTime();

    // lookback para pagos
    const paidLookbackDays = Number(req.query.paidLookbackDays || PAID_LOOKBACK_DAYS);
    let searchFromISO = _searchFromISO;
    let searchToISO   = _searchToISO;
    if (dateBy !== 'created') {
      const lookbackMs = paidLookbackDays * 24 * 60 * 60 * 1000;
      searchFromISO = new Date(baseFromMs - lookbackMs).toISOString();
      searchToISO   = new Date(baseToMs).toISOString();
    }
    if (DEBUG_ORDERS) console.log('[orders] ISO efectivos →', { searchFromISO, searchToISO, dateBy });

    const includeShipment = (req.query.includeShipment ?? '').length ? (req.query.includeShipment === 'true') : INCLUDE_SHIPMENT_DEFAULT;
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    // Búsqueda por IDs directos (order o pack)
    const idsList = String(req.query.id || req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);

    let baseOrders = [];

    if (idsList.length) {
      baseOrders = await fetchOrdersByIdsOrPacks(idsList, sellerId, token);
    } else {
      // paginado por fecha
      const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
      const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);
      let offset = 0;

      if (!searchFromISO || !searchToISO) {
        console.error('[orders] ISO faltantes', { searchFromISO, searchToISO, query: req.query });
        return res.status(400).json({ error: 'range_build_failed', detail: { searchFromISO, searchToISO, query: req.query } });
      }

      for (let i = 0; i < pages; i++) {
        const url = new URL('/orders/search', 'https://api.mercadolibre.com');
        url.searchParams.set('seller', String(sellerId));
        url.searchParams.set('order.date_created.from', searchFromISO);
        url.searchParams.set('order.date_created.to',   searchToISO);
        url.searchParams.set('sort',   'date_desc');
        url.searchParams.set('limit',  String(limitPage));
        url.searchParams.set('offset', String(offset));

        if (DEBUG_ORDERS) console.log('[orders][debug] GET →', url.toString());
        const { data } = await meliAxios.get(url.toString(), {
          headers: { Authorization: `Bearer ${token}` },
        });

        const results = Array.isArray(data?.results) ? data.results : [];
        baseOrders.push(...results);
        if (DEBUG_ORDERS) console.log(`[orders][debug] page ${i} offset=${offset} got=${results.length}`);
        if (results.length < limitPage) break;
        offset += limitPage;
      }
    }

    // Filtrar canceladas
    const orders = baseOrders.filter(o => o?.status !== 'cancelled');

    // Armar items → fila
    const items = await Promise.all(orders.map(order => limit(async () => {
      const createdMs = Date.parse(order?.date_created);
      let payments = order?.payments || [];
      let paidMs = getPaidMs(order, payments);

      if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && FETCH_PAYMENTS_DEFAULT) {
        if (fetchPayments) {
          try { payments = await getOrderPayments(order?.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }
      }

      let shipmentData = null;
      if (includeShipment) {
        const shipId = order?.shipping?.id || order?.packages?.[0]?.shipping?.id;
        if (shipId) { try { shipmentData = await getShipment(shipId); } catch {} }
      }

      let taxesFromMp = null;
      if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
        try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
      }

      const row = mapOrderToGridRow(order, payments, shipmentData, costsMap, taxesFromMp);
      fixRowNETO(row);

      const pivot = (dateBy === 'created') ? 'created' : (dateBy === 'paid') ? 'paid'
        : (Number.isFinite(paidMs) ? 'paid' : 'created');
      const fechaPivot = (Number.isFinite(paidMs) ? new Date(paidMs) : new Date(createdMs))
        .toISOString().replace('T',' ').slice(0,19);

      return { order, row, payments, shipment: shipmentData, createdMs, paidMs, pivot, fechaPivot };
    })));

    // Reparto del costo de envío dentro del pack/grupo
    applyShippingSplit(items);

    // Filtrar por rango real (created/paid según dateBy)
    const inRange = items.filter(it => {
      const ms = (it.pivot === 'paid')
        ? (Number.isFinite(it.paidMs) ? it.paidMs : it.createdMs)
        : it.createdMs;
      return (ms >= baseFromMs && ms <= baseToMs);
    });

    const rows = inRange.map(it => it.row);

    res.json({
      headers: HEADERS,
      from: fromYMD, to: toYMD, dateBy,
      count: rows.length,
      rows
    });
  } catch (err) {
    console.error('[orders] error:', err);
    res.status(500).json({ error: err?.message || 'Internal error' });
  }
});

// ====== /orders.csv (CSV descargable)
function toCsvLine(arr) {
  return arr.map(v => {
    if (v === null || v === undefined) return '';
    const s = String(v).replace(/"/g, '""');
    return /[",;\n]/.test(s) ? `"${s}"` : s;
  }).join(';');
}
app.get('/orders.csv', async (req, res) => {
  try{
    // Fallback de fechas como en /orders
    if (!req.query.from || !req.query.to) {
      const pad = n => String(n).padStart(2,'0');
      const now = new Date();
      const ymd = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`;
      if (!req.query.from) req.query.from = ymd;
      if (!req.query.to)   req.query.to   = ymd;
      if (!req.query.dateBy) req.query.dateBy = DEFAULT_DATE_BY;
    }
    // Consumimos el JSON de /orders manteniendo cookies de sesión
    const url = req.protocol + '://' + req.get('host') + '/orders?' + (new URLSearchParams(req.query)).toString();
    const r = await (await fetch(url, { headers: { cookie: req.headers.cookie || '' } })).json();

    const headers = Array.isArray(r.headers) ? r.headers : HEADERS;
    const rows = Array.isArray(r.rows) ? r.rows : [];

    const lines = [ toCsvLine(headers) ];
    for (const row of rows) lines.push(toCsvLine(row));

    const csv = lines.join('\r\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment: filename="orders_${Date.now()}.csv"`);
    res.send(csv);
  } catch(err){
    console.error('orders.csv error', err);
    res.status(500).send('CSV error');
  }
});

// ====== Debug crudo (opcional)
app.get('/debug/orders-raw', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);

    const rawMode = String(req.query.dateBy || req.query.mode || DEFAULT_DATE_BY).toLowerCase();
    const dateBy = (rawMode === 'created' || rawMode === 'paid' || rawMode === 'both') ? rawMode : DEFAULT_DATE_BY;
    const { fromISO, toISO, fromMs, toMs, fromYMD, toYMD } = parseRangeAR(req.query);

    const toIsoFromYMD = (ymd, endOfDay=false) => {
      const time = endOfDay ? '23:59:59.999' : '00:00:00.000';
      return new Date(`${ymd}T${time}${TZ_OFFSET}`).toISOString();
    };
    const _fromISO = fromISO || toIsoFromYMD(req.query.from, false);
    const _toISO   = toISO   || toIsoFromYMD(req.query.to,   true);

    const paidLookbackDays = Number(req.query.paidLookbackDays || PAID_LOOKBACK_DAYS);
    let searchFromISO = _fromISO, searchToISO = _toISO;
    if (dateBy !== 'created') {
      const lookbackMs = paidLookbackDays * 24 * 60 * 60 * 1000;
      searchFromISO = new Date((fromMs || new Date(_fromISO).getTime()) - lookbackMs).toISOString();
      searchToISO   = new Date(toMs || new Date(_toISO).getTime()).toISOString();
    }

    const sellerId  = await getUserId();
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);

    let offset = 0;
    const items = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        from: searchFromISO,
        to:   searchToISO,
        limit: limitPage,
        offset,
      });
      const results = Array.isArray(data?.results) ? data.results : [];
      items.push(...results);
      if (results.length < limitPage) break;
      offset += limitPage;
    }

    res.json({ from: fromYMD, to: toYMD, dateBy, count: items.length, items });
  } catch (err) {
    console.error('[debug/orders-raw] error:', err);
    res.status(500).json({ error: err?.message || 'Internal error' });
  }
});

// ====== Home protegida: si no hay sesión → login.html
function serve(file){ return path.resolve('public', file); }
app.get('/', (req, res) => {
  if (req.session?.user) return res.sendFile(serve('index.html'));
  return res.sendFile(serve('login.html'));
});
app.get('/index.html', (req, res) => {
  if (req.session?.user) return res.sendFile(serve('index.html'));
  return res.sendFile(serve('login.html'));
});

// (después de proteger "/" y "/index.html") servimos estáticos
app.use(express.static('public'));

// ====== Arranque
const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`ML backend listo en http://localhost:${PORT}`);
});
