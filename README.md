\# proyecto-ml



Backend + frontend para listar ventas de Mercado Libre, calcular neto, costos, envío e impuestos (vía Mercado Pago), y exportar a CSV.



\## Requisitos

\- Node 18+

\- Service Account de Google (Sheets API)

\- Hoja de cálculo con:

&nbsp; - `Tokens!A2` → Access Token de ML

&nbsp; - `Comparador!A` → item\_id

&nbsp; - `Comparador!M` → costo unitario



\## Variables de entorno

Ver `.env.example` y crear un `.env` local a partir de ese archivo (no subir `.env` al repo).



\## Scripts

```bash

npm install

npm start   # http://localhost:3000



