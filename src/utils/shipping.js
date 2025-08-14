// src/utils/shipping.js
export const UMBRAL_ENVIO_GRATIS =
  Number(process.env.UMBRAL_ENVIO_GRATIS || 33000);

/**
 * Replica la lógica del Apps Script:
 * - Si precioFinal < UMBRAL => 0
 * - Si receiver_cost > 0 => 0 (lo pagó el comprador)
 * - Sino => list_cost (independientemente de paid_by)
 *
 * @param {Object} p
 * @param {number} p.precioFinal  Precio FINAL del ítem usado para el umbral
 * @param {Object|null} p.shipmentData Respuesta de /shipments/{id}
 * @returns {number} costo de envío TOTAL de la orden (no unitario)
 */
export function computeShipmentCost({ precioFinal, shipmentData }) {
  if (!shipmentData) return 0;

  const price = Number(precioFinal) || 0;
  if (price < UMBRAL_ENVIO_GRATIS) return 0;

  const receiverCost = Number(shipmentData?.receiver_cost) || 0;
  const listCost = Number(shipmentData?.shipping_option?.list_cost) || 0;
  const paidBy = shipmentData?.shipping_option?.cost_components?.paid_by ?? null;

  if (receiverCost > 0) return 0; // lo pagó el comprador
  // En tu script haces list_cost tanto si paid_by === 'seller' como si no.
  return listCost;
}
