// src/services/googleSheetToken.js
import { google } from 'googleapis';
import fs from 'node:fs';

function loadCreds() {
  const keyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS_PATH;   // ← usamos *_PATH
  const keyJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;   // ← fallback inline

  // Logs de diagnóstico (se ven en la consola del server)
  console.log('[creds] keyPath =', keyPath);
  if (keyPath) console.log('[creds] exists(keyPath) =', fs.existsSync(keyPath));
  console.log('[creds] hasInlineJson =', !!keyJson);

  // 1) Archivo por PATH
  if (keyPath && fs.existsSync(keyPath)) {
    const raw = fs.readFileSync(keyPath, 'utf8');
    return JSON.parse(raw);
  }

  // 2) JSON inline
  if (keyJson) {
    return JSON.parse(keyJson);
  }

  // 3) Error claro
  throw new Error('No key or keyFile set. Definí GOOGLE_APPLICATION_CREDENTIALS_PATH o GOOGLE_APPLICATION_CREDENTIALS_JSON');
}

function getAuth() {
  const creds = loadCreds();
  const scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly'];
  return new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
}

export async function readTokenFromSheet() {
  const sheetId = process.env.GS_SHEET_ID;
  const sheetName = process.env.GS_TOKENS_SHEET || 'Tokens';
  const cell = process.env.GS_TOKENS_CELL || 'A2';
  if (!sheetId) throw new Error('Falta GS_SHEET_ID');

  const auth = getAuth();
  await auth.authorize();

  const sheets = google.sheets({ version: 'v4', auth });
  const range = `${sheetName}!${cell}:${cell}`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range });
  return res.data.values?.[0]?.[0] || null;
}

