// src/utils/mapOrder.js

// ==========================
// Helpers
// ==========================
const LOG_NETO = String(process.env.LOG_NETO || '').toLowerCase() === 'true';

const round = (n) => Math.round((Number(n) || 0) * 100) / 100;
const round1 = (n) => Math.round((Number(n) || 0) * 10) / 10; // 1 decimal para %
const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const formatDateTime = (d) => {
  if (!d) return '';
  const date = typeof d === 'number' ? new Date(d) : new Date(String(d));
  if (Number.isNaN(date.getTime())) return '';
  const pad = (x) => String(x).padStart(2, '0');
  const yyyy = date.getFullYear();
  const mm = pad(date.getMonth() + 1);
  const dd = pad(date.getDate());
  const hh = pad(date.getHours());
  const mi = pad(date.getMinutes());
  const ss = pad(date.getSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
};

// ==========================
// Extractores
// ==========================
function getTitulo(order) {
  return order?.order_items?.[0]?.item?.title ?? '';
}

function getPrecioBase(order) {
  return num(order?.order_items?.[0]?.full_unit_price);
}

function getPrecioFinal(order) {
  // total_amount = total de ítems (sin envío)
  return num(order?.total_amount ?? order?.order_items?.[0]?.unit_price);
}

function getDescuentoPct(order) {
  const base = getPrecioBase(order);
  const final = getPrecioFinal(order);
  if (!base || base <= 0) return 0;
  return round((1 - final / base) * 100);
}

function getCargoVenta(order) {
  return num(order?.order_items?.[0]?.sale_fee);
}

function getEnvio(order, shipment) {
  const s = shipment ?? order?.shipment ?? {};
  return num(s?.shipping_option?.list_cost ?? s?.shipping_option?.cost ?? 0);
}

function getImpuesto(order) {
  if (order && order._taxesFromMP != null) return num(order._taxesFromMP);
  const approved = (order?.payments || []).find(
    (p) => String(p?.status).toLowerCase() === 'approved'
  );
  return num(approved?.taxes_amount);
}

function getCuotas(order) {
  const approved = (order?.payments || []).find(
    (p) => String(p?.status).toLowerCase() === 'approved'
  );
  return approved?.installments ?? 0;
}

function getFechaPivot(order) {
  if (order?.fechaPivot) return order.fechaPivot;
  return (
    formatDateTime(order?.paidMs) ||
    formatDateTime(order?.date_closed) ||
    formatDateTime(order?.date_created)
  );
}

// ==========================
// Costo desde costsMap (sumado por ítem * cantidad)
// ==========================
function getCostoFromMap(order, costsMap = {}) {
  const items = Array.isArray(order?.order_items) ? order.order_items : [];
  let total = 0;
  for (const it of items) {
    const qty = num(it?.quantity || 1);
    const candIds = [
      String(it?.item?.id ?? ''),
      String(it?.item?.seller_sku ?? ''),
      String(it?.variation_id ?? ''),
    ].filter(Boolean);

    let unitCost = 0;
    for (const key of candIds) {
      if (key && costsMap[key] != null) {
        unitCost = num(costsMap[key]);
        break;
      }
    }
    total += round(unitCost * qty);
  }
  return round(total);
}

// ==========================
// Mapeo a la fila esperada
// Headers (13):
// ["ID DE VENTA","FECHA","TITULO","Precio Final","NETO","COSTO","GANANCIA","PRECIO BASE","% DESCUENTO","ENVIO","IMPUESTO","CARGO X VENTA","CUOTAS"]
// ==========================
export function mapOrderToGridRow(order, _paymentsSummary, shipmentData, costsMap) {
  const orderId = order?.id ?? 0;
  const fecha = getFechaPivot(order);
  const titulo = getTitulo(order);

  // Valores base
  const precioFinal = round(getPrecioFinal(order));
  const precioBase = round(getPrecioBase(order));
  const descuentoPct = round(getDescuentoPct(order));

  // Cargos
  let cargoVenta = 0;
  if (Array.isArray(order?.order_items)) {
    for (const it of order.order_items) cargoVenta += num(it?.sale_fee);
  } else {
    cargoVenta = getCargoVenta(order);
  }
  cargoVenta = round(cargoVenta);

  const envio = round(getEnvio(order, shipmentData));
  const impuesto = round(getImpuesto(order));
  const costo = round(getCostoFromMap(order, costsMap));

  // NETO = lo que te queda antes del costo
  const neto = round(precioFinal - (envio + impuesto + cargoVenta));

  // NUEVO: GANANCIA en %
  // (neto - costo) / costo * 100
  const gananciaPct = costo > 0 ? round1(((neto - costo) / costo) * 100) : 0;

  const cuotas = getCuotas(order);

  if (LOG_NETO) {
    console.log('[NETO DEBUG]', orderId, {
      precioFinal,
      envio,
      impuesto,
      cargoVenta,
      neto,
      costo,
      gananciaPct,
      precioBase,
      descuentoPct,
    });
  }

  return [
    orderId,     // ID DE VENTA
    fecha,       // FECHA (pivot)
    titulo,      // TITULO
    precioFinal, // Precio Final
    neto,        // NETO
    costo,       // Costo
    gananciaPct, // GANANCIA (% sobre costo)
    precioBase,  // PRECIO BASE
    descuentoPct,// % DESCUENTO
    envio,       // ENVIO
    impuesto,    // IMPUESTO
    cargoVenta,  // CARGO X VENTA
    cuotas,      // CUOTAS
  ];
}

export default { mapOrderToGridRow };
