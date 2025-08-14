// readSheet.js
const path = require('path');
const { google } = require('googleapis');

// ⬅️ Ajustá solo estas dos constantes:
const SHEET_ID = '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0'; // lo que va entre /d/ y /edit en la URL
const RANGE = 'Tokens!A2';                     // la celda donde guardás el access token

// Ruta a tu JSON de la service account
const KEYFILEPATH = path.join(__dirname, 'credentials', 'service-account.json');
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

function mask(t) {
  if (!t || typeof t !== 'string') return '(vacío)';
  return t.slice(0, 4) + '...' + t.slice(-4) + ` (len=${t.length})`;
}

async function main() {
  try {
    // 1) Autenticación
    const auth = new google.auth.GoogleAuth({ keyFile: KEYFILEPATH, scopes: SCOPES });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    // 2) Leer la celda
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: RANGE });
    const token = res.data.values?.[0]?.[0];

    console.log('GS_SHEET_ID:', SHEET_ID);
    console.log('RANGE:', RANGE);
    console.log('Token leído desde Sheets:', mask(token));
  } catch (e) {
    console.error('ERROR leyendo Sheets:', e.message);
  }
}

main();
