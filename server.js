// server.js (FAST - fixes NETO, CSV AR$, excel-friendly, lookback=5d, dateBy=both por defecto)
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

// ===== Envíos: reglas y reparto =====
// Umbral debajo del cual el vendedor NO paga envío
const SHIP_FREE_THRESHOLD = Number(process.env.SHIP_FREE_THRESHOLD || 32999);
// Con qué precio evaluar el umbral: true = PRECIO BASE, false = PRECIO FINAL
const SHIP_RULE_USE_BASE = process.env.SHIP_RULE_USE_BASE !== 'false'; // default: base
// Umbral incluyente: true => gratis si precio <= THRESHOLD (default true)
const SHIP_THRESHOLD_INCLUSIVE = (process.env.SHIP_THRESHOLD_INCLUSIVE ?? 'true') !== 'false';
// Modo de reparto para envíos compartidos: 'by_price' (default) | 'even' | 'first'
const SHIP_SPLIT_MODE = (process.env.SHIP_SPLIT_MODE || 'by_price').toLowerCase();

// ===== Config de performance / comportamiento =====
const TZ_OFFSET = process.env.TZ_OFFSET || '-03:00';
const DEFAULT_DATE_BY = (process.env.DEFAULT_DATE_BY || 'both').toLowerCase();
const PAID_LOOKBACK_DAYS = Number(process.env.PAID_LOOKBACK_DAYS || 5);
const FETCH_PAYMENTS_DEFAULT = process.env.FETCH_PAYMENTS === 'true';
const ENABLE_MP_TAXES = process.env.ENABLE_MP_TAXES !== 'false';
const INCLUDE_SHIPMENT_DEFAULT = process.env.INCLUDE_SHIPMENT_DEFAULT !== 'false';
const CSV_FMT_DEFAULT = (process.env.CSV_FMT_DEFAULT || 'ars').toLowerCase();

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

// Índices de columnas útiles
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
  PRECIO_FINAL_SIN_INTERES: HEADERS.indexOf('PRECIO FINAL'), // transaction_amount
  ENVIO: HEADERS.indexOf('ENVIO'),
  IMPUESTO: HEADERS.indexOf('IMPUESTO'),
  CARGO: HEADERS.indexOf('CARGO X VENTA'),
  CUOTAS: HEADERS.indexOf('CUOTAS'),
};

// Parser flexible (array/obj) para FECHA
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
  const c = Array.isArray(r) ? r[Math.max(IDX.FECHA, 1)] : (r?.FECHA ?? r?.date ?? r?.date_created);
  return P(c);
}

// ===== Impuestos MP — sólo pagos aprobados + deduplicación =====
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
      const r = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
        headers: { Authorization: `Bearer ${mpToken}` }, timeout: 8000,
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

// ===== Helpers comunes (números/round) =====
function toNum(v) { const n = Number(v); return Number.isFinite(n) ? n : 0; }
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const round1 = (n) => Math.round((Number(n) + Number.EPSILON) * 10) / 10;

// Fix de NETO (usar PRECIO FINAL si viene) — evita pisar si no hay precio
function fixRowNETO(row) {
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

// ====== Reglas de envío por grupo (shipment/pack) ======
function pfIndex() {
  const up = HEADERS.indexOf('PRECIO FINAL');   // transaction_amount (sin interés)
  const lo = HEADERS.indexOf('Precio Final');   // precio visible comprador (con interés)
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
  // Pre-paso: ítems sin clave de grupo → aplicar solo umbral
  for (const it of items) {
    const key = groupKey(it.order, it.shipment);
    if (!key) {
      if (isFreeByThreshold(it.row)) {
        it.row[IDX.ENVIO] = 0;
        recomputeNetoYGanancia(it.row);
      }
    }
  }

  // Agrupar por shipment/pack
  const groups = new Map();
  for (const it of items) {
    const key = groupKey(it.order, it.shipment);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(it);
  }

  for (const arr of groups.values()) {
    // Venta simple dentro de grupo conocido
    if (arr.length === 1) {
      const it = arr[0];
      if (isFreeByThreshold(it.row)) {
        it.row[IDX.ENVIO] = 0;
        recomputeNetoYGanancia(it.row);
      }
      continue;
    }

    // Total real del envío del grupo (suele venir duplicado en cada línea)
    const rawVals = arr.map(it => toNum(it.row[IDX.ENVIO]));
    const totalShip = Math.max(...rawVals, 0);

    if (!totalShip) {
      arr.forEach(it => { it.row[IDX.ENVIO] = 0; recomputeNetoYGanancia(it.row); });
      continue;
    }

    // Solo pagan los ítems por encima del umbral
    const eligible = arr.filter(it => !isFreeByThreshold(it.row));

    if (eligible.length === 0) {
      arr.forEach(it => { it.row[IDX.ENVIO] = 0; recomputeNetoYGanancia(it.row); });
      continue;
    }

    // Inicializar en 0 para todos, luego asignar a elegibles
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
      // by_price (default): prorrata por PRECIO FINAL (txn_amount si está)
      const priceBasis = (it) => {
        const up = IDX.PRECIO_FINAL_SIN_INTERES; // 'PRECIO FINAL' (txn)
        const { pfAny } = pfIndex();
        const v = toNum(up >= 0 ? it.row[up] : it.row[pfAny]);
        return v > 0 ? v : 0;
      };
      const weights = eligible.map(priceBasis);
      const sumW = weights.reduce((a,b) => a + b, 0);

      if (sumW <= 0) {
        // fallback a even
        const baseCents = Math.floor((totalShip * 100) / eligible.length);
        let remainder = Math.round(totalShip * 100) - baseCents * eligible.length;
        for (const it of eligible) {
          let cents = baseCents;
          if (remainder > 0) { cents += 1; remainder -= 1; }
          it.row[IDX.ENVIO] = round2(cents / 100);
        }
      } else {
        // prorrata con corrección de redondeo a centavos
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

    // Recalcular NETO / GANANCIA con el nuevo ENVÍO
    arr.forEach(it => recomputeNetoYGanancia(it.row));
  }
}

// ===== Debug crudo =====
app.get('/debug/orders-raw', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);

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

    const includeShipment = req.query.includeShipment !== 'false';
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    const sellerId  = await getUserId();
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);

    let offset = 0;
    const items = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO: searchFromISO,
        toISO: searchToISO,
        limit: limitPage,
        offset,
      });
      const results = data?.results || [];
      if (!results.length) break;

      for (const order of results) {
        if (order?.status === 'cancelled') continue;

        let payments = order?.payments || [];
        let paidMs = (dateBy !== 'created') ? getPaidMs(order, payments) : NaN;
        if ((dateBy === 'paid' || dateBy === 'both') && !Number.isFinite(paidMs) && fetchPayments) {
          try { payments = await getOrderPayments(order.id); } catch {}
          paidMs = getPaidMs(order, payments);
        }

        let inRange;
        const createdMs = Date.parse(order?.date_created);
        if (dateBy === 'created') {
          inRange = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
        } else if (dateBy === 'paid') {
          inRange = Number.isFinite(paidMs) && paidMs >= fromMs && paidMs <= toMs;
        } else {
          const createdIn = Number.isFinite(createdMs) && createdMs >= fromMs && createdMs <= toMs;
          const paidIn    = Number.isFinite(paidMs)    && paidMs    >= fromMs && paidMs    <= toMs;
          inRange = createdIn || paidIn;
        }
        if (!inRange) continue;

        let shipmentData = null;
        if (includeShipment) {
          const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
          if (shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }
        }

        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        const createdMsOut = Date.parse(order?.date_created);
        items.push({
          order,
          payments,
          shipment: shipmentData,
          createdMs: createdMsOut,
          paidMs: paidMs,
          pivot: (dateBy === 'created') ? 'created' : (dateBy === 'paid') ? 'paid' : (Number.isFinite(paidMs) ? 'paid' : 'created'),
          fechaPivot: (Number.isFinite(paidMs) ? new Date(paidMs) : new Date(createdMsOut)).toISOString().replace('T',' ').slice(0,19),
          taxesFromMp,
        });
      }

      if (results.length < limitPage) break;
      offset += limitPage;
    }

    res.json({ from: fromYMD, to: toYMD, dateBy, count: items.length, items });
  } catch (err) {
    console.error('[debug/orders-raw] error:', err);
    res.status(500).json({ error: err?.message || 'Internal error' });
  }
});

// ====== /orders ======
app.get('/orders', async (req, res) => {
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

    const includeShipment = (req.query.includeShipment ?? '').length ? (req.query.includeShipment === 'true') : INCLUDE_SHIPMENT_DEFAULT;
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    // --- Búsqueda por IDs (order/pack) ---
    const idsList = String(req.query.id || req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);
    if (idsList.length) {
      const sellerId = await getUserId();
      const baseOrders = await fetchOrdersByIdsOrPacks(idsList, sellerId, token);
      const orders = baseOrders.filter(o => o?.status !== 'cancelled');

      const batch = await Promise.all(orders.map(order => limit(async () => {
        const createdMs = Date.parse(order?.date_created);
        let payments = order?.payments || [];
        let paidMs = getPaidMs(order, payments);

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

        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        const row = mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
        const item = { row, order: { ...order }, shipment: shipmentData || null };
        fixRowNETO(item.row);
        return item;
      })));

      const items = batch.filter(Boolean);
      applyShippingSplit(items);
      const rows = items.map(it => it.row);

      return res.json({ from: fromYMD, to: toYMD, dateBy, count: rows.length, headers: HEADERS, rows, note: 'by id/pack' });
    }

    // --- Búsqueda por fecha (paginada) ---
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);
    const sellerId  = await getUserId();

    let offset = 0;
    const allItems = [];

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

        let taxesFromMp = null;
        if (ENABLE_MP_TAXES || req.query.includeMpTaxes === 'true') {
          try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}
        }

        let shipmentData = null;
        const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
        if (includeShipment && shipmentId) { try { shipmentData = await getShipment(shipmentId); } catch {} }

        const row = mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
        const item = { row, order: { ...order }, shipment: shipmentData || null };
        fixRowNETO(item.row);
        return item;
      })));

      allItems.push(...batch.filter(Boolean));
      if (raw.length < limitPage) break;
      offset += limitPage;
    }

    applyShippingSplit(allItems);
    const rows = allItems.map(it => it.row);

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

    const includeShipment = (req.query.includeShipment ?? '').length ? (req.query.includeShipment === 'true') : INCLUDE_SHIPMENT_DEFAULT;
    const fetchPayments = req.query.fetchPayments === 'true' || FETCH_PAYMENTS_DEFAULT;

    const sellerId  = await getUserId();
    const limitPage = Math.min(parseInt(req.query.pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(req.query.maxPages || '20', 10), 200);

    // -------- Config de export --------
    const fmt = (req.query.fmt || CSV_FMT_DEFAULT).toLowerCase(); // ars | raw | excel
    const locale = String(req.query.locale || process.env.EXCEL_LOCALE || 'latam').toLowerCase(); // latam | us
    const delim = (fmt === 'excel' && locale === 'latam') ? ';' : ',';
    const headerSep = (fmt === 'excel' && locale === 'latam') ? 'sep=;\r\n' : '';

    // helpers CSV
    function formatArs(n) {
      const value = Number(n);
      if (!Number.isFinite(value)) return '';
      const parts = value.toFixed(2).split('.');
      const int = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, '.');
      const dec = parts[1];
      return `$ ${int},${dec}`;
    }
    const CURRENCY_COLUMNS = new Set([
      'Precio Final','NETO','COSTO','PRECIO BASE','PRECIO FINAL','ENVIO','IMPUESTO','CARGO X VENTA'
    ]);
    const esc = (v) => { if (v == null) v = ''; const s = String(v).replace(/"/g,'""'); return `"${s}"`; };
    const maybeFmt = (header, val) => (fmt === 'ars' && CURRENCY_COLUMNS.has(header)) ? formatArs(val) : val;

    const NUMERIC_HEADERS = new Set([
      'Precio Final','PRECIO FINAL','NETO','COSTO','PRECIO BASE','ENVIO','IMPUESTO','CARGO X VENTA','CUOTAS'
    ]);
    const PCT_HEADERS = new Set(['GANANCIA','% DESCUENTO']);
    function toExcelFriendly(header, value, locale) {
      const raw = (value ?? '').toString().trim();
      if (PCT_HEADERS.has(header)) {
        const n = Number(raw.replace('%','').replace(/\./g,'').replace(',','.'));
        return Number.isFinite(n) ? n : raw;
      }
      if (NUMERIC_HEADERS.has(header)) {
        const n = Number(raw.replace(/\./g,'').replace(',','.'));
        return Number.isFinite(n) ? n : raw;
      }
      return raw;
    }
    const escTxt = (s) => `"${String(s).replace(/"/g,'""')}"`;
    const cellOut = (v) => (typeof v === 'number'
      ? String(v).replace('.', locale === 'latam' ? ',' : '.')
      : escTxt(v)
    );

    function buildCsv(rows, fnameBase) {
      if (fmt === 'excel') {
        const lines = [];
        lines.push(HEADERS.map(h => escTxt(h)).join(delim));
        for (const row of rows) {
          const out = row.map((v, idx) => {
            const header = HEADERS[idx] || '';
            if (idx === 0 && v != null && v !== '') return `="${String(v)}"`; // ID como texto
            const norm = toExcelFriendly(header, v, locale);
            return cellOut(norm);
          });
          lines.push(out.join(delim));
        }
        const csv = '\uFEFF' + headerSep + lines.join('\r\n');
        const fname = `${fnameBase}.csv`;
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
        return res.send(csv);
      }

      // ars/raw
      const lines = [HEADERS.map(h => esc(h)).join(',')];
      for (const row of rows) {
        const cells = row.map((v, idx) => {
          const header = HEADERS[idx] || '';
          const val = (idx === 0 && v != null && v !== '') ? `="${String(v)}"` : maybeFmt(header, v);
          return esc(val);
        });
        lines.push(cells.join(',')); 
      }
      const csv = '\uFEFF' + lines.join('\r\n');
      const fname = `${fnameBase}.csv`;
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
      return res.send(csv);
    }

    // -------- Rama por IDs --------
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

        const row = mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
        const item = { row, order: { ...order }, shipment: shipmentData || null };
        fixRowNETO(item.row);
        return item;
      })));

      const items = batch.filter(Boolean);
      applyShippingSplit(items);
      const rows = items.map(it => it.row);
      return buildCsv(rows, `orders_by_id_${idsList.join('-')}`);
    }

    // -------- Rama por fechas --------
    let offset = 0;
    const allItems = [];

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

        const row = mapOrderToGridRow(
          { ...order, _taxesFromMP: taxesFromMp },
          summarizePayments(payments),
          shipmentData,
          costsMap
        );
        const item = { row, order: { ...order }, shipment: shipmentData || null };
        fixRowNETO(item.row);
        return item;
      })));

      allItems.push(...batch.filter(Boolean));
      if (raw.length < limitPage) break;
      offset += limitPage;
    }

    applyShippingSplit(allItems);
    const rows = allItems.map(it => it.row);
    return buildCsv(rows, `orders_${fromYMD}_${toYMD}_${dateBy}`);
  } catch (err) {
    console.error('[orders.csv] error:', err);
    res.status(500).send('Error generando CSV');
  }
});

// Ping de debug simple
app.get('/debug/ping', (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`ML backend listo en http://localhost:${port}`));
