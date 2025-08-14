// src/utils/mapOrder.js
import dayjs from 'dayjs';

function r2(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return null;
  return Math.round(Number(v) * 100) / 100;
}

/**
 * HEADERS:
 * ["ID DE VENTA","FECHA","TITULO","Precio Final","NETO","COSTO","GANANCIA",
 *  "PRECIO BASE","% DESCUENTO","PRECIO FINAL","ENVIO","IMPUESTO","CARGO X VENTA","CUOTAS"]
 *
 * costsMap: objeto { [item_id]: costo_unitario }
 * taxOverride: número opcional (impuestos calculados vía Mercado Pago)
 */
export function mapOrderToGridRow(
  order,
  _paymentsSummaryIgnored,
  shipmentData,
  costsMap = {},
  taxOverride = null
) {
  const item = order?.order_items?.[0] || {};
  const itemInfo = item?.item || {};
  const qty = item?.quantity ?? 1;

  // Precios base/lista vs. con descuento
  const fullUnit = Number(item?.full_unit_price ?? 0);
  const unit     = Number(item?.unit_price ?? 0);
  const basePrice = r2(fullUnit * qty);      // PRECIO BASE
  const discountedSubtotal = r2(unit * qty); // PRECIO FINAL (subtotal ítem)

  // Pagos / totales
  const pay = (order?.payments && order.payments[0]) || {};
  const totalPaid = r2(pay?.total_paid_amount ?? order?.total_amount ?? discountedSubtotal);

  // ================== ENVÍO (mejorado) ==================
  const priceForShipment = discountedSubtotal ?? totalPaid ?? 0;
  const UMBRAL = Number(process?.env?.FREE_SHIPPING_THRESHOLD ?? 33000);

  const shippingFromPay = Number.isFinite(pay?.shipping_cost) ? Number(pay.shipping_cost) : 0;

  let shippingCostCalc = shippingFromPay;
  if (shipmentData) {
    const receiverCost =
      Number(
        shipmentData?.receiver_cost ??
        shipmentData?.cost_components?.receiver?.cost ??
        0
      );

    const paidBy =
      shipmentData?.shipping_option?.cost_components?.paid_by ??
      shipmentData?.cost_components?.paid_by ??
      null;

    const listCost =
      Number(
        shipmentData?.shipping_option?.list_cost ??
        shipmentData?.shipping_option?.cost_components?.list_cost ??
        shipmentData?.costs?.list ??
        shipmentData?.costs?.receiver ??
        0
      );

    if (receiverCost > 0) {
      // Lo pagó el comprador => costo vendedor = 0
      shippingCostCalc = 0;
    } else if (listCost > 0 && (paidBy === 'seller' || priceForShipment >= UMBRAL)) {
      // Lo paga el vendedor explícitamente o por umbral
      shippingCostCalc = listCost;
    }
  }
  const shippingCost = r2(shippingCostCalc || 0);
  // ======================================================

  // Impuestos: priorizamos override desde MP; si no, lo que traiga ML
  const taxesML = r2(pay?.taxes_amount ?? order?.taxes?.amount ?? 0);
  const taxes   = taxOverride != null ? r2(taxOverride) : taxesML;

  // CARGO X VENTA (fee ML que viene en el item)
  const saleFee = r2(item?.sale_fee ?? 0);

  // Neto
  const neto = r2((totalPaid ?? 0) - (saleFee ?? 0) - (taxes ?? 0) - (shippingCost ?? 0));

  // % descuento
  const discountPct = fullUnit > 0 ? r2((1 - unit / fullUnit) * 100) : null;

  // Cuotas
  const installments = pay?.installments ?? null;

  // COSTO desde hoja "Comparador"
  const itemId = itemInfo?.id || null;
  const unitCost =
    itemId && Number.isFinite(Number(costsMap[itemId])) ? Number(costsMap[itemId]) : null;
  const totalCost = unitCost !== null ? r2(unitCost * qty) : null;

  // GANANCIA = NETO - COSTO
  const profit = (neto !== null && totalCost !== null) ? r2(neto - totalCost) : null;

  return [
    order?.id ?? null,                                                // ID DE VENTA
    order?.date_created ? dayjs(order.date_created).format('YYYY-MM-DD HH:mm:ss') : null, // FECHA
    itemInfo?.title ?? null,                                          // TITULO
    totalPaid,                                                        // Precio Final (total pagado)
    neto,                                                             // NETO
    totalCost,                                                        // COSTO
    profit,                                                           // GANANCIA
    basePrice,                                                        // PRECIO BASE
    discountPct,                                                      // % DESCUENTO
    discountedSubtotal,                                               // PRECIO FINAL (subtotal ítem)
    shippingCost,                                                     // ENVIO
    taxes,                                                            // IMPUESTO
    saleFee,                                                          // CARGO X VENTA
    installments,                                                     // CUOTAS
  ];
}

