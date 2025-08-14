// src/services/payments.js

/** Resume los pagos de una orden.
 *  - netReceived: suma de net_received_amount (cuando viene desde MP)
 *  - installments: mayor cantidad de cuotas detectada entre los pagos
 */
export function summarizePayments(payments = []) {
  let netReceived = 0;
  let installments = 0;

  for (const p of payments) {
    if (typeof p.net_received_amount === 'number') {
      netReceived += p.net_received_amount;
    }
    if (typeof p.installments === 'number' && p.installments > installments) {
      installments = p.installments;
    }
  }

  return { netReceived, installments };
}
