import 'dotenv/config';
import { google } from 'googleapis';
import path from 'path';

// Lee las variables del .env
const SHEET_ID = process.env.GS_SHEET_ID;
const RANGE = `${process.env.GS_TOKENS_SHEET}!${process.env.GS_TOKENS_CELL}`;
const KEYFILEPATH = process.env.GOOGLE_APPLICATION_CREDENTIALS_PATH;
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

function mask(t) {
  if (!t || typeof t !== 'string') return '(vacío)';
  return t.slice(0, 4) + '...' + t.slice(-4) + ` (len=${t.length})`;
}

(async () => {
  try {
    console.log('🔍 KEYFILEPATH:', KEYFILEPATH);
    const auth = new google.auth.GoogleAuth({
      keyFile: KEYFILEPATH,
      scopes: SCOPES
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: RANGE });
    const token = res.data.values?.[0]?.[0];

    console.log('✅ Token leído correctamente:', mask(token));
  } catch (err) {
    console.error('❌ Error leyendo token:', err.message);
  }
})();
