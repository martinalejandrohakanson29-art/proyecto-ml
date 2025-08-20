// data/authStore.js
import fs from 'node:fs';
import path from 'node:path';
import bcrypt from 'bcryptjs';

const DATA_DIR = path.resolve('data');
const FILE = path.join(DATA_DIR, 'users.json');

function ensureFile() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(FILE)) fs.writeFileSync(FILE, JSON.stringify({ users: [] }, null, 2));
}
function readAll() {
  ensureFile();
  const raw = fs.readFileSync(FILE, 'utf8');
  return JSON.parse(raw || '{"users":[]}');
}
function writeAll(data) {
  fs.writeFileSync(FILE, JSON.stringify(data, null, 2));
}

export function getUserByEmail(email) {
  const db = readAll();
  return db.users.find(u => u.email.toLowerCase() === String(email).toLowerCase()) || null;
}
export async function createUser({ email, password, roles = [] }) {
  const db = readAll();
  if (db.users.some(u => u.email.toLowerCase() === email.toLowerCase())) {
    throw new Error('email_exists');
  }
  const passHash = await bcrypt.hash(password, 10);
  const user = { id: Date.now().toString(36), email, passHash, roles: Array.from(new Set(roles)) };
  db.users.push(user);
  writeAll(db);
  return { id: user.id, email: user.email, roles: user.roles };
}
export async function verifyPassword(email, password) {
  const user = getUserByEmail(email);
  if (!user) return null;
  const ok = await bcrypt.compare(password, user.passHash);
  if (!ok) return null;
  return { id: user.id, email: user.email, roles: user.roles };
}
