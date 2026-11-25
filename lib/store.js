// Accounts, joint-op factions, the moderator audit log and a few settings.
// Everything lives in memory (there are only ever a few hundred accounts) and every change is saved right away:
//   - to Postgres when DATABASE_URL is set   <- use this on Render: free hosting has no permanent disk
//   - otherwise to a JSON file in ./data     <- fine on your own PC; WIPED whenever free hosting restarts
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const AUDIT_KEEP = 500;

class Store {
  constructor() {
    this.kind = 'memory';
    this.users = new Map();      // id -> user
    this.byName = new Map();     // lower-case username -> user
    this.factions = new Map();   // id -> faction
    this.audit = [];             // newest last
    this.meta = {};
    this.nextUid = 1; this.nextFid = 1; this.nextAid = 1;
  }

  async init() {
    await this._load();
    // IDs are never reused: a deleted account's saved login names its ID, so a new account must never get that ID.
    // The highest ID ever handed out is kept in meta, because deleting the newest rows would otherwise lower max(id).
    this.nextUid = Math.max(this.nextUid, Number(this.meta.next_uid) || 0);
    this.nextFid = Math.max(this.nextFid, Number(this.meta.next_fid) || 0);
    for (const u of this.users.values()) { this.byName.set(u.username.toLowerCase(), u); this.nextUid = Math.max(this.nextUid, u.id + 1); }
    for (const f of this.factions.values()) this.nextFid = Math.max(this.nextFid, f.id + 1);
    for (const a of this.audit) this.nextAid = Math.max(this.nextAid, a.id + 1);
    this.audit.sort((a, b) => a.id - b.id);
    return this;
  }

  userByName(name) { return this.byName.get(String(name || '').toLowerCase()) || null; }

  async createUser(fields) {
    const id = this.nextUid++;
    await this.setMeta('next_uid', this.nextUid);                     // saved before the account, so a crash leaves a gap, never a reused ID
    const u = {
      // tokver starts random: a login saved for some other (deleted) account can't line up with this one by chance
      id, username: '', passHash: '', role: 'user', factionId: null, title: '', disabled: false, tokver: crypto.randomInt(1, 2 ** 30),
      createdBy: '', createdAt: Date.now(), lastLogin: 0, lastCallsign: '', ...fields,
    };
    this.users.set(u.id, u); this.byName.set(u.username.toLowerCase(), u);
    await this._saveUser(u);
    return u;
  }
  async saveUser(u) { this.byName.set(u.username.toLowerCase(), u); await this._saveUser(u); }
  async deleteUser(id) {
    const u = this.users.get(id); if (!u) return;
    this.users.delete(id); this.byName.delete(u.username.toLowerCase());
    await this._delUser(id);
  }

  async createFaction(name, createdBy = '') {
    const id = this.nextFid++;
    await this.setMeta('next_fid', this.nextFid);
    const f = { id, name, enabled: true, createdAt: Date.now() };
    this.factions.set(f.id, f); await this._saveFaction(f); return f;
  }
  async saveFaction(f) { await this._saveFaction(f); }
  async deleteFaction(id) {
    for (const u of [...this.users.values()]) if (u.factionId === id) await this.deleteUser(u.id);
    this.factions.delete(id); await this._delFaction(id);
  }

  async addAudit(actor, action, target = '', detail = '') {
    const e = { id: this.nextAid++, at: Date.now(), actor: String(actor || ''), action: String(action), target: String(target || ''), detail: String(detail || '').slice(0, 200) };
    this.audit.push(e);
    if (this.audit.length > AUDIT_KEEP) this.audit.splice(0, this.audit.length - AUDIT_KEEP);
    try { await this._saveAudit(e); } catch (err) { console.error('audit save failed:', err.message); }
    return e;
  }

  async setMeta(k, v) { this.meta[k] = String(v); await this._saveMeta(k, String(v)); }
  async close() {}
}

// ---------- JSON file ----------
class FileStore extends Store {
  constructor(dir) { super(); this.kind = 'file'; this.dir = dir; this.file = path.join(dir, 'accounts.json'); this._timer = null; }
  async _load() {
    fs.mkdirSync(this.dir, { recursive: true });
    if (!fs.existsSync(this.file)) return;
    const d = JSON.parse(fs.readFileSync(this.file, 'utf8'));
    for (const u of d.users || []) this.users.set(u.id, u);
    for (const f of d.factions || []) this.factions.set(f.id, f);
    this.audit = d.audit || []; this.meta = d.meta || {};
  }
  _dirty() { if (!this._timer) this._timer = setTimeout(() => this._flush(), 150); }
  _flush() {
    clearTimeout(this._timer); this._timer = null;
    const tmp = this.file + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ users: [...this.users.values()], factions: [...this.factions.values()], audit: this.audit, meta: this.meta }));
    fs.renameSync(tmp, this.file);                                   // atomic: a crash can't leave a half-written file
  }
  async _saveUser() { this._dirty(); } async _delUser() { this._dirty(); }
  async _saveFaction() { this._dirty(); } async _delFaction() { this._dirty(); }
  async _saveAudit() { this._dirty(); } async _saveMeta() { this._dirty(); }
  async close() { if (this._timer) this._flush(); }
}

// ---------- Postgres ----------
class PgStore extends Store {
  constructor(pool) {
    super(); this.kind = 'postgres';
    // Free database hosts put idle databases to sleep, so the first query after a quiet spell can hit a dead
    // connection. Retry once on a connection-type error; anything else (a real SQL error) is passed straight up.
    const raw = pool;
    this.pool = {
      query: async (sql, params) => {
        try { return await raw.query(sql, params); }
        catch (e) { if (/terminat|ECONNRESET|ETIMEDOUT|closed|timeout|ended/i.test(String(e && e.message))) return raw.query(sql, params); throw e; }
      },
      end: () => raw.end(),
    };
  }
  async _load() {
    const q = (s, p) => this.pool.query(s, p);
    const ensure = async (table, ddl) => {                       // create a table only if it isn't there yet
      const have = await q('select 1 from information_schema.tables where table_name = $1', [table]);
      if (!have.rows.length) await q(ddl);
    };
    await ensure('meta', `create table meta (k text primary key, v text not null)`);
    await ensure('factions', `create table factions (id integer primary key, name text not null, enabled boolean not null default true, created_at double precision not null default 0)`);
    await ensure('users', `create table users (
      id integer primary key, username text not null, uname_lc text not null unique, pass_hash text not null, role text not null,
      faction_id integer, title text not null default '', disabled boolean not null default false, tokver integer not null default 0,
      created_by text not null default '', created_at double precision not null default 0, last_login double precision not null default 0,
      last_callsign text not null default '')`);
    await ensure('audit', `create table audit (id integer primary key, at double precision not null, actor text not null default '', action text not null, target text not null default '', detail text not null default '')`);
    for (const r of (await q('select * from factions')).rows) this.factions.set(r.id, { id: r.id, name: r.name, enabled: r.enabled, createdAt: Number(r.created_at) });
    for (const r of (await q('select * from users')).rows) this.users.set(r.id, {
      id: r.id, username: r.username, passHash: r.pass_hash, role: r.role, factionId: r.faction_id, title: r.title, disabled: r.disabled,
      tokver: r.tokver, createdBy: r.created_by, createdAt: Number(r.created_at), lastLogin: Number(r.last_login), lastCallsign: r.last_callsign,
    });
    this.audit = (await q('select * from audit order by id desc limit $1', [AUDIT_KEEP])).rows
      .map((r) => ({ id: r.id, at: Number(r.at), actor: r.actor, action: r.action, target: r.target, detail: r.detail })).reverse();
    for (const r of (await q('select * from meta')).rows) this.meta[r.k] = r.v;
  }
  _saveUser(u) {
    return this.pool.query(
      `insert into users (id, username, uname_lc, pass_hash, role, faction_id, title, disabled, tokver, created_by, created_at, last_login, last_callsign)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       on conflict (id) do update set username = excluded.username, uname_lc = excluded.uname_lc, pass_hash = excluded.pass_hash, role = excluded.role,
         faction_id = excluded.faction_id, title = excluded.title, disabled = excluded.disabled, tokver = excluded.tokver,
         last_login = excluded.last_login, last_callsign = excluded.last_callsign`,
      [u.id, u.username, u.username.toLowerCase(), u.passHash, u.role, u.factionId, u.title || '', !!u.disabled, u.tokver | 0, u.createdBy || '', u.createdAt, u.lastLogin || 0, u.lastCallsign || '']);
  }
  _delUser(id) { return this.pool.query('delete from users where id = $1', [id]); }
  _saveFaction(f) {
    return this.pool.query(
      `insert into factions (id, name, enabled, created_at) values ($1,$2,$3,$4)
       on conflict (id) do update set name = excluded.name, enabled = excluded.enabled`, [f.id, f.name, !!f.enabled, f.createdAt]);
  }
  _delFaction(id) { return this.pool.query('delete from factions where id = $1', [id]); }
  async _saveAudit(e) {
    await this.pool.query('insert into audit (id, at, actor, action, target, detail) values ($1,$2,$3,$4,$5,$6)', [e.id, e.at, e.actor, e.action, e.target, e.detail]);
    if (e.id % 50 === 0) await this.pool.query('delete from audit where id <= $1', [e.id - AUDIT_KEEP]);
  }
  _saveMeta(k, v) {
    return this.pool.query('insert into meta (k, v) values ($1,$2) on conflict (k) do update set v = excluded.v', [k, v]);
  }
  async close() { await this.pool.end(); }
}

// Trims stray spaces/quotes from a pasted value and removes the sslmode / channel_binding options (we handle SSL).
function cleanDatabaseUrl(raw) {
  let s = String(raw).trim().replace(/^['"]|['"]$/g, '');
  if (!/^postgres(ql)?:\/\//i.test(s)) throw new Error('DATABASE_URL does not look right: it should start with postgresql://');
  const q = s.indexOf('?');
  if (q === -1) return s;
  const keep = s.slice(q + 1).split('&').filter((p) => p && !/^(sslmode|channel_binding)=/i.test(p));
  return s.slice(0, q) + (keep.length ? '?' + keep.join('&') : '');
}

async function createStore({ databaseUrl, dataDir, pool } = {}) {
  if (pool) return new PgStore(pool).init();                                   // (tests pass an in-memory pool)
  if (databaseUrl) {
    const { Pool } = require('pg');
    const cleaned = cleanDatabaseUrl(databaseUrl);
    const local = /@(localhost|127\.0\.0\.1)[:/]/.test(cleaned);
    // Hosts like Neon give a URL ending "?sslmode=require&channel_binding=require". We set SSL ourselves (below), so
    // those two options are removed. We try a fully verified secure connection first and only relax it if the
    // host's certificate can't be verified, with a warning in the log.
    const open = async (relaxed) => {
      const p = new Pool({ connectionString: cleaned, max: 4, connectionTimeoutMillis: 15000, idleTimeoutMillis: 30000, ssl: local ? undefined : { rejectUnauthorized: !relaxed } });
      p.on('error', (e) => console.error('database connection problem:', e.message));
      try { await p.query('select 1'); return p; } catch (e) { await p.end().catch(() => {}); throw e; }
    };
    let pool;
    try { pool = await open(false); }
    catch (e) {
      if (!local && /certificate|self.signed|unable to verify/i.test(String(e.message))) { console.warn('Database certificate could not be verified; continuing with an encrypted but unverified connection.'); pool = await open(true); }
      else throw new Error(`Could not connect to the database: ${e.message}`);
    }
    return new PgStore(pool).init();
  }
  return new FileStore(dataDir || path.join(__dirname, '..', 'data')).init();
}

module.exports = { createStore, cleanDatabaseUrl };
