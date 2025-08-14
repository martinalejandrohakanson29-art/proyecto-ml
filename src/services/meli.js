// src/services/meli.js
import axios from 'axios';

let _mlToken = null;

// Cliente axios para ML
const meliApi = axios.create({
  baseURL: 'https://api.mercadolibre.com',
  timeout: 15000,
});

// Inyecta el token en el cliente
export function setMeliToken(token) {
  _mlToken = token || null;
  if (_mlToken) {
    meliApi.defaults.headers.common['Authorization'] = `Bearer ${_mlToken}`;
  } else {
    delete meliApi.defaults.headers.common['Authorization'];
  }
}

// /users/me  → devuelve el ID del vendedor autenticado
export async function getUserId() {
  const { data } = await meliApi.get('/users/me');
  return data?.id;
}

/**
 * Busca órdenes por rango de creación.
 * Params:
 * - sellerId: número (obligatorio)
 * - fromISO, toISO: strings ISO (obligatorios)
 * - limit: 1..50
 * - offset: >= 0
 *
 * ⚠️ Modificación pedida: sort=date_desc (más nuevas primero)
 */
export async function searchOrdersByDate({ sellerId, fromISO, toISO, limit = 50, offset = 0 }) {
  const params = new URLSearchParams({
    seller: String(sellerId),
    'order.date_created.from': fromISO,
    'order.date_created.to': toISO,
    sort: 'date_desc',          // <— clave pedida
    limit: String(Math.min(Math.max(limit, 1), 50)),
    offset: String(Math.max(offset, 0)),
  });

  // Nota: el filtrado de canceladas lo hace el server (para no romper la paginación)
  return meliApi.get(`/orders/search?${params.toString()}`);
}

// /orders/{id}/payments
export async function getOrderPayments(orderId) {
  const { data } = await meliApi.get(`/orders/${orderId}/payments`);
  return data;
}

// /shipments/{id}
export async function getShipment(shipmentId) {
  const { data } = await meliApi.get(`/shipments/${shipmentId}`);
  return data;
}
