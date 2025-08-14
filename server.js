// // server.js
import 'dotenv/config';
import express from 'express';
import dayjs from 'dayjs';
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

// --- impuesto desde Mercado Pago (suma charges_details.type === "tax") ---
async function computeTaxesFromMercadoPago(payments = []) {
  const mpToken = process.env.MP_ACCESS_TOKEN;
  if (!mpToken) return null; // si no hay token, no hacemos nada

  let totalTaxes = 0;

  for (const p of payments) {
    const paymentId = p?.id;
    if (!paymentId) continue;

    try {
      const r = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
        headers: { Authorization: `Bearer ${mpToken}` },
        timeout: 10000,
      });

      const charges = r.data?.charges_details || [];
      for (const ch of charges) {
        if (ch?.type === 'tax') {
          const amt =
            ch?.amounts?.original ??
            ch?.amounts?.payer ??
            ch?.amounts?.collector ??
            ch?.amount ??
            0;
          totalTaxes += Number(amt) || 0;
        }
      }
    } catch (e) {
      console.warn('[mp/taxes] error pago', paymentId, e?.response?.status || e.message);
    }
  }

  return totalTaxes;
}

// Log: ruta de credencial (única var)
console.log('GAC =', process.env.GOOGLE_APPLICATION_CREDENTIALS);

const app = express();
app.use(express.json());
app.use(express.static('public'));


// =====================
// Helpers Google Sheets + caches
let _mlTokenCache = { value: null, exp: 0 };   // cache token ML (5 min)
let _costsCache   = { map: {}, exp: 0 };       // cache costos     (5 min)

async function getSheetsClient() {
  const keyFile =
    process.env.GOOGLE_APPLICATION_CREDENTIALS || './credentials/service-account.json';
  const auth = new google.auth.GoogleAuth({
    keyFile,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

async function getTokenFromSheetsCached() {
  const now = Date.now();
  if (_mlTokenCache.value && now < _mlTokenCache.exp) return _mlTokenCache.value;

  const sheets = await getSheetsClient();
  const spreadsheetId =
    process.env.GS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';
  const sheetName = process.env.GS_TOKENS_SHEET || 'Tokens';
  const cell = process.env.GS_TOKENS_CELL || 'A2';
  const range = `${sheetName}!${cell}`;

  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const token = resp.data.values?.[0]?.[0] || '';

  _mlTokenCache = { value: token, exp: now + 5 * 60 * 1000 };
  return token;
}

// === Costos desde "Comparador" (A=item_id, M=costo) con parseo robusto ===
async function getCostsMapFromSheetsCached() {
  const now = Date.now();
  if (_costsCache.map && now < _costsCache.exp) return _costsCache.map;

  const sheets = await getSheetsClient();
  const spreadsheetId =
    process.env.GS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';

  const sheetName = process.env.GS_COSTS_SHEET || 'Comparador';
  const range = `${sheetName}!A2:M`; // Col A..M (A=0 ... M=12)

  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const rows = resp.data.values || [];

  const map = {};
  for (const row of rows) {
    const id = (row[0] || '').trim();   // A = ITEM_ID de publicación
    const costRaw = row[12];            // M = costo unitario
    if (!id) continue;

    const cleaned = String(costRaw ?? '')
      .replace(/\s+/g, '')
      .replace(/[^\d,.\-]/g, '')
      .replace(/\./g, '')
      .replace(',', '.');

    const cost = Number(cleaned);
    if (!Number.isFinite(cost)) continue;

    map[id] = cost;
  }

  _costsCache = { map, exp: now + 5 * 60 * 1000 };
  return map;
}
// =====================

// Ruta raíz informativa
app.get('/', (_req, res) => {
  res
    .type('text')
    .send('API ML OK. Endpoints: /health, /orders, /debug/creds, /debug/token, /debug/ml, /debug/orders_raw, /debug/costs, /debug/costs_raw, /debug/missing_costs');
});

// Healthcheck
app.get('/health', (_req, res) => res.json({ ok: true }));

// ---------- Debug helpers ----------
app.get('/debug/creds', (_req, res) => {
  try {
    const keyPath =
      process.env.GOOGLE_APPLICATION_CREDENTIALS || './credentials/service-account.json';
    const fileExists = fs.existsSync(keyPath);
    res.json({ keyPath, fileExists });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/debug/token', async (_req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    res.json({
      ok: true,
      tokenLength: token.length,
      preview: token ? token.slice(0, 12) + '...' : '',
    });
  } catch (e) {
    console.error('[debug] error /debug/token:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/debug/ml', async (_req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    if (!token) return res.status(400).json({ ok: false, error: 'Token vacío en la hoja' });

    const r = await axios.get('https://api.mercadolibre.com/users/me', {
      headers: { Authorization: `Bearer ${token}` },
      timeout: 10000,
    });

    const { id, nickname, site_id, status } = r.data || {};
    res.json({ ok: true, id, nickname, site_id, status });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.response?.data || e.message });
  }
});

app.get('/debug/orders_raw', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);

    const { from, to } = req.query;
    const start = from ? dayjs(from).startOf('day') : dayjs().startOf('day');
    const end   = to   ? dayjs(to).endOf('day')   : dayjs().endOf('day');

    const fromISO = start.toDate().toISOString();
    const toISO   = end.toDate().toISOString();

    const sellerId = await getUserId();
    const { data } = await searchOrdersByDate({
      sellerId,
      fromISO,
      toISO,
      limit: 5,
      offset: 0,
    });

    const ordersRaw = data?.results || [];
    const orders = ordersRaw.filter(o => o?.status !== 'cancelled');

    return res.json({
      ok: true,
      count: orders.length,
      sample: orders.slice(0, 2),
    });
  } catch (e) {
    console.error('[debug] /orders_raw error:', e?.response?.data || e.message);
    res.status(500).json({ ok: false, error: e?.response?.data || e.message });
  }
});

// Inspección cruda de la hoja de costos: A..M + parseo
app.get('/debug/costs_raw', async (_req, res) => {
  try {
    const sheets = await getSheetsClient();
    const spreadsheetId =
      process.env.GS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';
    const sheetName = process.env.GS_COSTS_SHEET || 'Comparador';

    const range = `${sheetName}!A1:M50`;
    const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const rows = resp.data.values || [];

    const normalizeCost = (v) => {
      const raw = String(v ?? '');
      const cleaned = raw
        .replace(/\s+/g, '')
        .replace(/[^\d,.\-]/g, '')
        .replace(/\./g, '')
        .replace(',', '.');
      const num = Number(cleaned);
      return { raw, cleaned, num: Number.isFinite(num) ? num : 0 };
    };

    const preview = rows.map((r, i) => {
      const A = r[0] ?? null;     // ITEM_ID
      const M = r[12] ?? null;    // COSTO
      const parsed = normalizeCost(M);
      return { row: i + 1, A, M, parsed };
    });

    res.json({ ok: true, sheet: sheetName, rows: preview });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Ver costos leídos desde "Comparador" (A=item_id, M=costo)
app.get('/debug/costs', async (_req, res) => {
  try {
    const map = await getCostsMapFromSheetsCached();
    const entries = Object.entries(map);
    res.json({
      ok: true,
      count: entries.length,
      sample: entries.slice(0, 5),
    });
  } catch (e) {
    console.error('[debug] /debug/costs error:', e);
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Detecta publicaciones sin costo en la hoja "Comparador"
app.get('/debug/missing_costs', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);
    const costsMap = await getCostsMapFromSheetsCached();

    const { from, to, pageSize = '50', maxPages = '5' } = req.query;
    const start = from ? dayjs(from).startOf('day') : dayjs().startOf('day');
    const end   = to   ? dayjs(to).endOf('day')   : dayjs().endOf('day');
    const fromISO = start.toDate().toISOString();
    const toISO   = end.toDate().toISOString();

    const sellerId = await getUserId();
    const limitPage = Math.min(parseInt(pageSize, 10), 50);
    const pages     = Math.min(parseInt(maxPages, 10), 50);

    const missing = new Map();
    let offset = 0;

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO,
        toISO,
        limit: limitPage,
        offset,
      });

      const raw = data?.results || [];
      const orders = raw.filter(o => o?.status !== 'cancelled');
      if (!raw.length) break;

      for (const order of orders) {
        const it = order?.order_items?.[0];
        const itemId = it?.item?.id;
        const title  = it?.item?.title || null;
        if (itemId && costsMap[itemId] == null && !missing.has(itemId)) {
          missing.set(itemId, title);
        }
      }

      if (raw.length < limitPage) break;
      offset += limitPage;
    }

    res.json({
      ok: true,
      from: start.format('YYYY-MM-DD'),
      to: end.format('YYYY-MM-DD'),
      missingCount: missing.size,
      missing: Array.from(missing.entries())
        .map(([id, title]) => ({ id, title }))
        .slice(0, 200),
      note: 'Cargá estos item_id en Comparador!A (y su costo en M) para completar COSTO y GANANCIA.',
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.response?.data || e.message });
  }
});
// -----------------------------------

// ========= Endpoint principal: /orders =========
/**
 * GET /orders
 * Query:
 *  from=YYYY-MM-DD   (ARG local) | default: hoy 00:00
 *  to=YYYY-MM-DD     (ARG local) | default: hoy 23:59:59
 *  pageSize=50                    | máx ML 50
 *  maxPages=20                    | páginas a iterar
 *  includeShipment=true|false     | si consulta /shipments/{id} por orden
 */
app.get('/orders', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);

    const costsMap = await getCostsMapFromSheetsCached();

    const { from, to, pageSize, maxPages, includeShipment } = req.query;

    const start = from ? dayjs(from).startOf('day') : dayjs().startOf('day');
    const end = to ? dayjs(to).endOf('day') : dayjs().endOf('day');

    const fromISO = start.toDate().toISOString();
    const toISO = end.toDate().toISOString();

    const limitPage = Math.min(parseInt(pageSize || '50', 10), 50);
    const pages = Math.min(parseInt(maxPages || '20', 10), 200);

    const sellerId = await getUserId();

    let offset = 0;
    const rows = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO,
        toISO,
        limit: limitPage,
        offset,
      });

      const raw = data?.results || [];
      const orders = raw.filter(o => o?.status !== 'cancelled');
      if (!raw.length) break;

      const batch = await Promise.all(
        orders.map((order) =>
          limit(async () => {
            // Pagos
            let payments = [];
            try {
              payments = await getOrderPayments(order.id);
            } catch (e) {
              if (e?.response?.status === 403) {
                payments = order?.payments || [];
                console.warn(
                  '[payments] 403 en /orders/{id}/payments. Usando order.payments como fallback.',
                  { orderId: order?.id, fallbackCount: payments?.length || 0 }
                );
              } else {
                throw e;
              }
            }

            // Impuestos desde MP
            let taxesFromMp = null;
            try {
              taxesFromMp = await computeTaxesFromMercadoPago(payments);
            } catch {}

            // Envío (si se pidió)
            let shipmentData = null;
            const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
            if (includeShipment === 'true' && shipmentId) {
              try {
                shipmentData = await getShipment(shipmentId);
              } catch {}
            }

            // Pasamos _taxesFromMP para que el mapper lo use si está presente
            return mapOrderToGridRow(
              { ...order, _taxesFromMP: taxesFromMp },
              summarizePayments(payments),
              shipmentData,
              costsMap
            );
          })
        )
      );

      rows.push(...batch);

      if (raw.length < limitPage) break;  // <-- clave: usar raw
      offset += limitPage;
    }

    res.json({
      from: start.format('YYYY-MM-DD'),
      to: end.format('YYYY-MM-DD'),
      count: rows.length,
      headers: HEADERS,
      rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err?.message || 'Internal error' });
  }
});

// Exporta las órdenes a CSV
app.get('/orders.csv', async (req, res) => {
  try {
    const token = await getTokenFromSheetsCached();
    setMeliToken(token);
    const costsMap = await getCostsMapFromSheetsCached();

    const { from, to, pageSize, maxPages, includeShipment } = req.query;
    const start = from ? dayjs(from).startOf('day') : dayjs().startOf('day');
    const end   = to   ? dayjs(to).endOf('day')   : dayjs().endOf('day');

    const fromISO = start.toDate().toISOString();
    const toISO   = end.toDate().toISOString();

    const limitPage = Math.min(parseInt(pageSize || '50', 10), 50);
    const pages     = Math.min(parseInt(maxPages || '20', 10), 200);

    const sellerId = await getUserId();

    let offset = 0;
    const allRows = [];

    for (let i = 0; i < pages; i++) {
      const { data } = await searchOrdersByDate({
        sellerId,
        fromISO,
        toISO,
        limit: limitPage,
        offset,
      });

      const raw = data?.results || [];
      const orders = raw.filter(o => o?.status !== 'cancelled');
      if (!raw.length) break;

      const batch = await Promise.all(
        orders.map(order =>
          limit(async () => {
            let payments = [];
            try {
              payments = await getOrderPayments(order.id);
            } catch (e) {
              if (e?.response?.status === 403) payments = order?.payments || [];
              else throw e;
            }

            let taxesFromMp = null;
            try { taxesFromMp = await computeTaxesFromMercadoPago(payments); } catch {}

            let shipmentData = null;
            const shipmentId = order?.shipping?.id || order?.order_items?.[0]?.shipping?.id;
            if (includeShipment === 'true' && shipmentId) {
              try { shipmentData = await getShipment(shipmentId); } catch {}
            }

            return mapOrderToGridRow(
              { ...order, _taxesFromMP: taxesFromMp },
              summarizePayments(payments),
              shipmentData,
              costsMap
            );
          })
        )
      );

      allRows.push(...batch);

      if (raw.length < limitPage) break;  // <-- clave: usar raw
      offset += limitPage;
    }

    // CSV helpers (ID como texto y BOM UTF-8)
    const headers = HEADERS;
    const esc = (v) => {
      if (v === null || v === undefined) v = '';
      const s = String(v).replace(/"/g, '""');
      return `"${s}"`;
    };

    const lines = [];
    lines.push(headers.map(esc).join(','));

    for (const row of allRows) {
      const cells = row.map((v, idx) => {
        if (idx === 0 && v !== null && v !== undefined && v !== '') {
          return `="${String(v)}"`; // fuerza texto en Excel para ID
        }
        return esc(v);
      });
      lines.push(cells.join(','));
    }

    const bom = '\uFEFF';
    const csv = bom + lines.join('\r\n');

    const fname = `orders_${start.format('YYYY-MM-DD')}_${end.format('YYYY-MM-DD')}.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(csv);
  } catch (err) {
    console.error('[orders.csv] error:', err);
    res.status(500).send('Error generando CSV');
  }
});

// Debug: inspeccionar impuestos de una orden puntual
app.get('/debug/taxes', async (req, res) => {
  try {
    const { id } = req.query;
    if (!id) return res.status(400).json({ ok: false, error: 'Falta ?id=ORDER_ID' });

    const token = await getTokenFromSheetsCached();

    const ord = await axios.get(`https://api.mercadolibre.com/orders/${id}`, {
      headers: { Authorization: `Bearer ${token}` },
      timeout: 10000,
    });

    let payments = [];
    try {
      const r = await axios.get(`https://api.mercadolibre.com/orders/${id}/payments`, {
        headers: { Authorization: `Bearer ${token}` },
        timeout: 10000,
      });
      payments = r.data || [];
    } catch {
      payments = ord.data?.payments || [];
    }

    const orderTaxes = {
      order_taxes_amount: ord.data?.taxes?.amount ?? null,
      order_taxes_obj: ord.data?.taxes ?? null,
      payments_taxes_amounts: payments.map(p => p?.taxes_amount ?? null),
      payments_taxes_raw: payments.map(p => ({
        id: p?.id,
        taxes_amount: p?.taxes_amount,
        fee_details: p?.fee_details,
      })),
    };

    res.json({ ok: true, order_id: id, orderTaxes, samplePayment: payments[0] || null });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.response?.data || e.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`ML backend listo en http://localhost:${port}`);
});

