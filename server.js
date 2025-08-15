// server.js (FAST)
import 'dotenv/config';
import express from 'express';
import fs from 'node:fs';
import { google } from 'googleapis';
import axios from 'axios';

import { limit } from './src/utils/limiter.js';
import {
  setMeliToken,
  getUserId,
  searchOrdersByDate,
  getOrderPayments,
  getShipment,
} from './src/services/meli.js';
import { summarizePayments } from './src/services/payments.js';
import { HEADERS } from './src/utils/columns.js';
import { mapOrderToGridRow } from './src/utils/mapOrder.js';

const app = express();
app.use(express.json());
app.use(express.static('public'));

// ===== Config de performance / comportamiento =====
const TZ_OFFSET = process.env.TZ_OFFSET || '-03:00';
const DEFAULT_DATE_BY = (process.env.DEFAULT_DATE_BY || 'created').toLowerCase(); // created|paid|both
const PAID_LOOKBACK_DAYS = Number(process.env.PAID_LOOKBACK_DAYS || 7);          // ventana hacia atrás p/paid
const FETCH_PAYMENTS_DEFAULT = process.env.FETCH_PAYMENTS === 'true';            // off por defecto
const ENABLE_MP_TAXES = process.env.ENABLE_MP_TAXES === 'true';                  // off por defecto

// ===== Utilidades de fecha (AR -03:00) =====
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

// Índice FECHA (para filas-array)
const FECHA_IDX = Math.max(HEADERS.indexOf('FECHA'), 1);

// Parser flexible (array/obj)
function rowTimeMs(r) {
  const TZ = TZ_OFFSET;
  const P = (s) => {
    if (!s) return NaN;
    let str = String(s);
    if (/T\d{2}:\d{2}/.test(str) && /Z|[+\-]\d{2}:\d{2}$/.test(str)) { const ms = Date.parse(str); return Number.isNaN(ms)?NaN:ms; }
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(str)) { const ms = Date.parse(str.replace(' ','T')+TZ); return Number.isNaN(ms)?NaN:ms; }
    if (/^\d{4}-\d{2}-\d{2}$/.test(str)) { const ms = Date.parse(`${str}T00:00:00.000${TZ}`); return Number.isNaN(ms)?NaN:ms; }
    const ms = Date.parse(str); return Number.isNaN(ms)?NaN:ms;
  };
  const c = Array.isArray(r) ? r[FECHA_IDX] : (r?.FECHA ?? r?.date ?? r?.date_created);
  return P(c);
}

// ===== Impuestos (opcional) =====
async function computeTaxesFromMercadoPago(payments = []) {
  const mpToken = process.env.MP_ACCESS_TOKEN;
  if (!mpToken) return null;
  let totalTaxes = 0;
  for (const p of payments) {
    const paymentId = p?.id;
    if (!paymentId) continue;
    try {
      const r = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
        headers: { Authorization: `Bearer ${mpToken}` }, timeout: 8000,
      });
      const charges = r.data?.charges_details || [];
      for (const ch of charges) {
        if (ch?.type === 'tax') {
          const amt = ch?.amounts?.original ?? ch?.amounts?.payer ?? ch?.amounts?.collector ?? ch?.amount ?? 0;
          totalTaxes += Number(amt) || 0;
        }
      }
    } catch {}
  }
  return totalTaxes;
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

// ===== Auth debug y Sheets =====
const ADMIN_KEY = process.env.ADMIN_KEY || '';
function requireAdmin(req, res, next) {
  const key = req.query.key || req.get('x-admin-key');
  if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(401).json({ error: 'unauthorized' });
  next();
}
app.use('/debug', requireAdmin);

let _mlTokenCache = { value: null, exp: 0 };
let _costsCache   = { map: {}, exp: 0 };

async function getSheetsClient() {
  const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS || './credentials/service-account.json';
  const auth = new google.auth.GoogleAuth({ keyFile, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
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
    const cleaned = String(costRaw ?? '').replace(/\s+/g,'').replace(/[^\d,.\-]/g,'').replace(/\./g,'').replace(',', '.');
    const cost = Number(cleaned);
    if (Number.isFinite(cost)) map[id] = cost;
  }
  _costsCache = { map, exp: now + 5 * 60 * 1000 };
  return map;
}

// ===== Búsqueda por ID o Pack (paralelizada) =====
async function fetchOrdersByIdsOrPacks(ids = [], sellerId, token) {
  const out = new Map();
  await Promise.all(ids.map(async raw => {
    const id = String(raw || '').trim();
    if (!id) return;

    const tryOrder = axios.get(`https://api.mercadolibre.com/orders/${id}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 7000,
    }).catch(() => null);

    const tryPack = axios.get(
      `https://api.mercadolibre.com/orders/search?seller=${sellerId}&pack_id=${encodeURIComponent(id)}&limit=50&offset=0`,
      { headers: { Authorization: `Bearer ${token}` }, timeout: 7000 }
    ).catch(() => null);

    const [ro, rp] = await Promise.all([tryOrder, tryPack]);

    if (ro?.data?.id) out.set(ro.data.id, ro.data);
    const results = rp?.data?.results || [];
    for (const o of results) if (o?.id) out.set(o.id, o);
  }));
  return Array.from(out.values());
}

// ===== Rutas =====
app.get('/', (_req, res) => res.type('text').send('API ML OK. Endpoints: /orders, /orders.csv, /debug/token'));
app.get('/debug/token', async (_req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    res.json({ ok: true, tokenLength: token.length, preview: token ? token.slice(0,12)+'...' : '' });
  } catch (e) { res.status(500).json({ ok:false, error: e.message }); }
});

// ====== /orders ======
app.get('/orders', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);
    const costsMap = await getCostsMapFromSheetsCached();

    // Modo (rápido por defecto = created)
    const rawMode = String(req.query.dateBy || req.query.mode || DEFAULT_DATE_BY).toLowerCase();
    const dateBy = (rawMode === 'created' || rawMode === 'paid' || rawMode === 'both') ? rawMode : DEFAULT_DATE_BY;

    const { fromISO, toISO, fromMs, toMs, fromYMD, toYMD } = parseRangeAR(req.query);

    // Si el modo usa pago (paid/both) ampliamos un poquito la ventana (lookback)
    const paidLookbackDays = Number(req.query.paidLookbackDays || PAID_LOOKBACK_DAYS);
    let searchFromISO = fromISO, searchToISO = toISO;
    if (dateBy !== 'created') {
      const lookbackMs = paidLookbackDays * 24 * 60 * 60 * 1000;
      searchFromISO = new Date(fromMs - lookbackMs).toISOString();
      searchToISO   = new Date(toMs).toISOString();
    }

    const includeShipment = req.query.includeShipment === 'true';
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    // --- Búsqueda por IDs (order/pack) ---
    const idsList = String(req.query.id || req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);
    if (idsList.length) {
      const sellerId = await getUserId();
      const baseOrders = await fetchOrdersByIdsOrPacks(idsList, sellerId, token);
      const orders = baseOrders.filter(o => o?.status !== 'cancelled');

      const batch = await Promise.all(orders.map(order => limit(async () => {
        // Filtro “barato” primero: creation y paid usando date_closed + order.payments
        const createdMs = Date.parse(order?.date_created);
        let payments = order?.payments || [];
        let paidMs = getPaidMs(order, payments);

        // Si estoy filtrando por pago y aún no tengo fecha, opcionalmente pego a la API
        if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && fetchPayments) {
          try { payments = await getOrderPayments(order.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }

        let inRange;
        if (dateBy === 'created') {
          inRange = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
        } else if (dateBy === 'paid') {
          inRange = Number.isFinite(paidMs) && paidMs >= fromMs && paidMs <= toMs;
        } else {
          const createdIn = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
          const paidIn    = Number.isFinite(paidMs)    && paidMs    >= fromMs && paidMs    <= toMs;
          inRange = createdIn || paidIn;
        }
        if (!inRange) return null;

        // Impuestos MP (opcional)
        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        // Envío (opcional)
        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        return mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
      })));

      const rows = batch.filter(Boolean);
      return res.json({ from: fromYMD, to: toYMD, dateBy, count: rows.length, headers: HEADERS, rows, note: 'by id/pack' });
    }

    // --- Búsqueda por fecha (paginada) ---
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);
    const sellerId  = await getUserId();

    let offset = 0;
    const rows = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO: searchFromISO,
        toISO: searchToISO,
        limit: limitPage,
        offset,
      });

      const raw = data?.results || [];
      const orders = raw.filter(o => o?.status !== 'cancelled');
      if (!raw.length) break;

      const batch = await Promise.all(orders.map(order => limit(async () => {
        const createdMs = Date.parse(order?.date_created);

        // Primero intentamos con info local (rápido)
        let payments = order?.payments || [];
        let paidMs = (dateBy !== 'created') ? getPaidMs(order, payments) : NaN;

        // Si necesito pago y no lo tengo, sólo entonces puedo ir a la API (si lo pedís)
        if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && fetchPayments) {
          try { payments = await getOrderPayments(order.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }

        // Decidir inclusión
        let inRange;
        if (dateBy === 'created') {
          inRange = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
        } else if (dateBy === 'paid') {
          inRange = Number.isFinite(paidMs) && paidMs >= fromMs && paidMs <= toMs;
        } else {
          const createdIn = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
          const paidIn    = Number.isFinite(paidMs)    && paidMs    >= fromMs && paidMs    <= toMs;
          inRange = createdIn || paidIn;
        }
        if (!inRange) return null;

        // Impuestos MP (opcional)
        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        // Envío (opcional)
        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        return mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
      })));

      rows.push(...batch.filter(Boolean));
      if (raw.length < limitPage) break;
      offset += limitPage;
    }

    res.json({ from: fromYMD, to: toYMD, dateBy, count: rows.length, headers: HEADERS, rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err?.message || 'Internal error' });
  }
});

// ====== /orders.csv ======
app.get('/orders.csv', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);
    const costsMap = await getCostsMapFromSheetsCached();

    const rawMode = String(req.query.dateBy || req.query.mode || DEFAULT_DATE_BY).toLowerCase();
    const dateBy = (rawMode === 'created' || rawMode === 'paid' || rawMode === 'both') ? rawMode : DEFAULT_DATE_BY;
    const { fromISO, toISO, fromMs, toMs, fromYMD, toYMD } = parseRangeAR(req.query);

    const paidLookbackDays = Number(req.query.paidLookbackDays || PAID_LOOKBACK_DAYS);
    let searchFromISO = fromISO, searchToISO = toISO;
    if (dateBy !== 'created') {
      const lookbackMs = paidLookbackDays * 24 * 60 * 60 * 1000;
      searchFromISO = new Date(fromMs - lookbackMs).toISOString();
      searchToISO   = new Date(toMs).toISOString();
    }

    const includeShipment = req.query.includeShipment === 'true';
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    const sellerId  = await getUserId();
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);

    const esc = (v) => { if (v == null) v = ''; const s = String(v).replace(/"/g,'""'); return `"${s}"`; };

    // Rama por IDs
    const idsList = String(req.query.id || req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);
    if (idsList.length) {
      const baseOrders = await fetchOrdersByIdsOrPacks(idsList, sellerId, token);
      const orders = baseOrders.filter(o => o?.status !== 'cancelled');

      const batch = await Promise.all(orders.map(order => limit(async () => {
        const createdMs = Date.parse(order?.date_created);
        let payments = order?.payments || [];
        let paidMs = (dateBy !== 'created') ? getPaidMs(order, payments) : NaN;
        if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && fetchPayments) {
          try { payments = await getOrderPayments(order.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }
        let inRange;
        if (dateBy === 'created') inRange = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
        else if (dateBy === 'paid') inRange = Number.isFinite(paidMs) && paidMs >= fromMs && paidMs <= toMs;
        else {
          const createdIn = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
          const paidIn    = Number.isFinite(paidMs)    && paidMs    >= fromMs && paidMs    <= toMs;
          inRange = createdIn || paidIn;
        }
        if (!inRange) return null;

        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        return mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
      })));

      const rows = batch.filter(Boolean);
      const lines = [HEADERS.map(esc).join(',')];
      for (const row of rows) {
        const cells = row.map((v, idx) => (idx === 0 && v != null && v !== '') ? `="${String(v)}"` : esc(v));
        lines.push(cells.join(','));
      }
      const csv = '\uFEFF' + lines.join('\r\n');
      const fname = `orders_by_id_${idsList.join('-')}.csv`;
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
      return res.send(csv);
    }

    // Rama por fechas
    let offset = 0;
    const allRows = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO: searchFromISO,
        toISO: searchToISO,
        limit: limitPage,
        offset,
      });

      const raw = data?.results || [];
      const orders = raw.filter(o => o?.status !== 'cancelled');
      if (!raw.length) break;

      const batch = await Promise.all(orders.map(order => limit(async () => {
        const createdMs = Date.parse(order?.date_created);
        let payments = order?.payments || [];
        let paidMs = (dateBy !== 'created') ? getPaidMs(order, payments) : NaN;
        if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && fetchPayments) {
          try { payments = await getOrderPayments(order.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }
        let inRange;
        if (dateBy === 'created') inRange = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
        else if (dateBy === 'paid') inRange = Number.isFinite(paidMs) && paidMs >= fromMs && paidMs <= toMs;
        else {
          const createdIn = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
          const paidIn    = Number.isFinite(paidMs)    && paidMs    >= fromMs && paidMs    <= toMs;
          inRange = createdIn || paidIn;
        }
        if (!inRange) return null;

        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        return mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
      })));

      allRows.push(...batch.filter(Boolean));
      if (raw.length < limitPage) break;
      offset += limitPage;
    }

    const esc2 = (v) => { if (v == null) v = ''; const s = String(v).replace(/"/g,'""'); return `"${s}"`; };
    const lines = [HEADERS.map(esc2).join(',')];
    for (const row of allRows) {
      const cells = row.map((v, idx) => (idx === 0 && v != null && v !== '') ? `="${String(v)}"` : esc2(v));
      lines.push(cells.join(','));
    }
    const csv = '\uFEFF' + lines.join('\r\n');
    const fname = `orders_${fromYMD}_${toYMD}_${dateBy}.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(csv);
  } catch (err) {
    console.error('[orders.csv] error:', err);
    res.status(500).send('Error generando CSV');
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`ML backend listo (rápido) en http://localhost:${port}`));
