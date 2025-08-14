// fetch_orders.js
require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const accessToken = process.env.ML_ACCESS_TOKEN;
const fromDate = process.env.FROM_DATE;
const toDate = process.env.TO_DATE;

if (!accessToken || !fromDate || !toDate) {
  console.error("Faltan datos en el archivo .env");
  process.exit(1);
}

async function getUserId() {
  const res = await axios.get('https://api.mercadolibre.com/users/me', {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  return res.data.id;
}

async function fetchOrders(userId) {
  let offset = 0;
  let orders = [];
  const limit = 50;

  while (true) {
    const url = `https://api.mercadolibre.com/orders/search?seller=${userId}&order.date_created.from=${fromDate}&order.date_created.to=${toDate}&limit=${limit}&offset=${offset}`;
    console.log(`Consultando: ${url}`);

    const res = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });

    if (!res.data.results || res.data.results.length === 0) break;

    orders = orders.concat(res.data.results);
    offset += limit;

    if (res.data.results.length < limit) break;
  }
  return orders;
}

function saveFiles(orders) {
  const salidaDir = path.join(__dirname, 'salida');
  if (!fs.existsSync(salidaDir)) fs.mkdirSync(salidaDir);

  const jsonPath = path.join(salidaDir, 'ventas.json');
  fs.writeFileSync(jsonPath, JSON.stringify(orders, null, 2));
  console.log(`Archivo JSON guardado en ${jsonPath}`);

  const csvPath = path.join(salidaDir, 'ventas.csv');
  const headers = ["order_id", "date_created", "status", "total_amount"];
  const csvRows = [headers.join(",")];
  orders.forEach(o => {
    csvRows.push([o.id, o.date_created, o.status, o.total_amount].join(","));
  });
  fs.writeFileSync(csvPath, csvRows.join("\n"));
  console.log(`Archivo CSV guardado en ${csvPath}`);
}

(async () => {
  try {
    const userId = await getUserId();
    console.log(`User ID: ${userId}`);

    const orders = await fetchOrders(userId);
    console.log(`Total órdenes: ${orders.length}`);

    saveFiles(orders);
  } catch (err) {
    console.error("Error:", err.response?.data || err.message);
  }
})();
