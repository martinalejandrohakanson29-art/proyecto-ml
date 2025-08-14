console.log('[gs] LOADED (ESM) ->', import.meta.url);
console.log('[gs] KEYFILEPATH desde env ->', process.env.GOOGLE_APPLICATION_CREDENTIALS_PATH);




// googleSheets.js (ESM)
import { google } from 'googleapis';

const KEYFILEPATH =
  process.env.GOOGLE_APPLICATION_CREDENTIALS_PATH || './credentials/service-account.json';

const SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

export async function getSheets() {
  console.log('[creds] usando keyFile:', KEYFILEPATH);
  const auth = new google.auth.GoogleAuth({ keyFile: KEYFILEPATH, scopes: SCOPES });
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

export async function readRange(spreadsheetId, range) {
  const sheets = await getSheets();
  const res = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  return res.data.values ?? [];
}

export async function readTokenFromEnv() {
  const spreadsheetId =
    process.env.TOKENS_SHEET_ID || '1AUw7IrTmuODu_WrogVienkxfUzG12j5DfH4jbMFvas0';
  const range = process.env.TOKENS_RANGE || 'Tokens!A2';

  const values = await readRange(spreadsheetId, range);
  const token = (values[0] && values[0][0]) || '';
  return token;
}
