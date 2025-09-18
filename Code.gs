/** 
 * Controle$e — Backend Google Apps Script (Code.gs)
 * Atualizado: 2025-09-18
 *
 * Recursos:
 * - Login (Usuarios) + Licenças (Licencas) + Provisionamento (Instâncias)
 * - Sessões com TTL via CacheService
 * - Listagem com filtros server-side + paginação (10k+ linhas)
 * - Métricas agregadas (entradas, saídas, saldo, top categorias)
 * - CRUD mínimo (upsert/delete)
 * - Atualizações a cada 30s por polling no frontend; pronto para WebSocket (relé externo)
 * - Funções utilitárias robustas para BRL/data/acentos
 */

/**********************************
 * 0) CONFIG
 **********************************/
const CONFIG = {
  // IDs das planilhas-mãe
  ACCESS_SHEET_ID: '1eubSNey1RKM4DmTNhFLFxBdewjzQ0iiP2XCfmvrGA4E', // Planilha de Acessos
  TEMPLATE_DB_ID: '1ZhJ82Te40gYVMMuip7ullqLXONgyK1qcuyIEtNteuFE',  // Template Controle$e

  // Nomes das abas
  TABS: {
    INSTANCIAS: 'Instâncias',
    LICENCAS: 'Licencas',
    USUARIOS: 'Usuarios',
    LANCAMENTOS: 'Lançamentos'
  },

  // Cache TTLs
  CACHE_SECONDS: 30,           // para métricas/consultas rápidas
  SESSION_TTL_MIN: 120,        // 2h de sessão

  // Limites
  MAX_PAGE_SIZE: 1000
};

/**********************************
 * 1) HELPERS & CACHE
 **********************************/
const _scriptCache = CacheService.getScriptCache();

function _nowISO() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd'T'HH:mm:ssXXX");
}

function _b64(obj) {
  return Utilities.base64EncodeWebSafe(JSON.stringify(obj));
}

function _safeCachePut(key, obj, seconds) {
  try {
    const s = JSON.stringify(obj);
    // Limite por item no CacheService ~100KB
    if (s.length < 90 * 1024) _scriptCache.put(key, s, seconds);
  } catch (e) {
    // ignora
  }
}

function _safeCacheGet(key) {
  const s = _scriptCache.get(key);
  if (!s) return null;
  try { return JSON.parse(s); } catch (e) { return null; }
}

function norm(v) {
  if (v === null || v === undefined) return '';
  return String(v)
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // remove acentos
    .toLowerCase();
}

function parseMoney(v) {
  if (typeof v === 'number') return v;
  if (!v && v !== 0) return 0;
  const s = String(v).trim();
  if (!s) return 0;
  // "1.234,56" → 1234.56; também aceita "1234.56"
  const n = Number(s.replace(/\./g, '').replace(',', '.'));
  return isNaN(n) ? 0 : n;
}

function parseBool(v) {
  if (typeof v === 'boolean') return v;
  const s = norm(v);
  return s === 'sim' || s === 'true' || s === '1' || s === 'yes';
}

function parseDateBR(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const s = String(v).trim();
  // aceita "dd/MM/yyyy" ou ISO "yyyy-MM-dd"
  let m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

function fmtDateBR(d) {
  if (!(d instanceof Date)) return '';
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'dd/MM/yyyy');
}

function getAccessSS() { return SpreadsheetApp.openById(CONFIG.ACCESS_SHEET_ID); }
function getTemplateSS() { return SpreadsheetApp.openById(CONFIG.TEMPLATE_DB_ID); }

function getSheetByName(ss, name) {
  const s = ss.getSheetByName(name);
  if (!s) throw new Error('Aba não encontrada: ' + name);
  return s;
}

function readTable(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastCol === 0) return { headers: [], rows: [] };
  const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  if (lastRow <= 1) return { headers, rows: [] };
  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const rows = values.map(r => {
    const o = {};
    for (let i = 0; i < headers.length; i++) o[headers[i]] = r[i];
    return o;
  });
  return { headers, rows };
}

function writeRow(sheet, headers, obj, rowIndex1) {
  const row = headers.map(h => (obj[h] !== undefined ? obj[h] : ''));
  sheet.getRange(rowIndex1, 1, 1, headers.length).setValues([row]);
}

/**********************************
 * 2) SESSÃO / AUTH / LICENÇA / PROVISIONAMENTO
 **********************************/
function login(email, senha) {
  if (!email || !senha) throw new Error('Informe email e senha.');
  const accSS = getAccessSS();

  // Usuários
  const usuarios = readTable(getSheetByName(accSS, CONFIG.TABS.USUARIOS)).rows;
  const user = usuarios.find(u => norm(u.Email) === norm(email));
  if (!user || String(user.Senha) !== String(senha)) throw new Error('Credenciais inválidas.');

  // Licença
  const lic = readTable(getSheetByName(accSS, CONFIG.TABS.LICENCAS)).rows
    .find(l => norm(l.Email) === norm(email));
  if (!lic || norm(lic.Status) !== 'sim') throw new Error('Licença inativa. Contate o suporte.');

  // Instância
  const ssId = ensureInstanceFor(email);

  // Sessão (token simples cacheado)
  const token = _b64({ email, ssId, exp: Date.now() + CONFIG.SESSION_TTL_MIN * 60 * 1000 });
  _scriptCache.put('sess:' + token, JSON.stringify({ email, ssId }), CONFIG.SESSION_TTL_MIN * 60);

  return { ok: true, token, email, ssId };
}

function ensureInstanceFor(email) {
  const accSS = getAccessSS();
  const instSheet = getSheetByName(accSS, CONFIG.TABS.INSTANCIAS);
  const tbl = readTable(instSheet);
  let inst = tbl.rows.find(r => norm(r.Email) === norm(email));
  if (inst && inst.SS_ID) return String(inst.SS_ID);

  // copia do template
  const template = getTemplateSS();
  const copy = DriveApp.getFileById(template.getId()).makeCopy('Controle$e — ' + email);
  const newId = copy.getId();
  try { copy.addEditor(email); } catch(_) { /* pode falhar se email não for Google */ }

  const headers = tbl.headers.length ? tbl.headers : ['Email', 'SS_ID', 'CriadoEm'];
  if (inst) {
    inst.SS_ID = newId;
    inst.CriadoEm = _nowISO();
    writeRow(instSheet, headers, inst, tbl.rows.findIndex(r => norm(r.Email) === norm(email)) + 2);
  } else {
    const newRow = { Email: email, SS_ID: newId, CriadoEm: _nowISO() };
    instSheet.appendRow(headers.map(h => newRow[h] ?? ''));
  }
  return newId;
}

function _requireSession(token) {
  const raw = _scriptCache.get('sess:' + token);
  if (!raw) throw new Error('Sessão expirada. Faça login novamente.');
  try {
    const s = JSON.parse(raw); // { email, ssId }
    return s;
  } catch (e) {
    throw new Error('Sessão inválida. Faça login novamente.');
  }
}

/**********************************
 * 3) DATA ACCESS — LISTAGEM + FILTROS + PAGINAÇÃO
 **********************************/
function _openClientSS(ssId) { return SpreadsheetApp.openById(ssId); }

function _filterLancamentosAll(ssId, opts) {
  const ss = _openClientSS(ssId);
  const sh = getSheetByName(ss, CONFIG.TABS.LANCAMENTOS);
  const { rows } = readTable(sh);

  const f = Object.assign({
    dataIni: null, dataFim: null,
    tipo: '', categoria: '', subcategoria: '', forma: '', parcelado: '', status: ''
  }, opts || {});

  const dIni = f.dataIni ? parseDateBR(f.dataIni) : null;
  const dFim = f.dataFim ? parseDateBR(f.dataFim) : null;
  if (dFim) dFim.setHours(23, 59, 59, 999); // inclusivo

  const filtered = rows.filter(r => {
    const d = parseDateBR(r['Data']);
    if (dIni && (!d || d < dIni)) return false;
    if (dFim && (!d || d > dFim)) return false;

    if (f.tipo && norm(r['Tipo']) !== norm(f.tipo)) return false;
    if (f.categoria && !norm(r['Categoria']).includes(norm(f.categoria))) return false;
    if (f.subcategoria && !norm(r['Subcategoria']).includes(norm(f.subcategoria))) return false;
    if (f.forma && !norm(r['Forma Pagamento']).includes(norm(f.forma))) return false;

    if (f.parcelado) {
      const want = norm(f.parcelado); // 'sim' | 'nao'
      const has = parseBool(r['Parcelado']) ? 'sim' : 'nao';
      if (want && want !== has) return false;
    }

    if (f.status) {
      const want = norm(f.status); // 'true' | 'false' | 'sim' | 'nao'
      const has = parseBool(r['Status']) ? 'true' : 'false';
      if (want && want !== has) return false;
    }

    return true;
  });

  // Ordena por Data desc
  filtered.sort((a, b) => {
    const da = parseDateBR(a['Data']);
    const db = parseDateBR(b['Data']);
    return (db ? db.getTime() : 0) - (da ? da.getTime() : 0);
  });

  // Mapeia para estrutura padrão
  return filtered.map(r => ({
    data: fmtDateBR(parseDateBR(r['Data'])),
    descricao: r['Descrição'],
    valor: parseMoney(r['Valor Total']),
    parcelado: parseBool(r['Parcelado']),
    parcelas: Number(r['Parcelas'] || 1),
    tipo: r['Tipo'],
    categoria: r['Categoria'],
    subcategoria: r['Subcategoria'],
    forma: r['Forma Pagamento'],
    obs: r['Observações'] || '',
    status: parseBool(r['Status']),
    _key: [r['Data'], r['Descrição'], parseMoney(r['Valor Total'])].join('||')
  }));
}

function listLancamentos(token, opts) {
  const { ssId } = _requireSession(token);

  // cache (somente se pequeno)
  const cacheKey = 'lanc:' + ssId + ':' + Utilities.base64EncodeWebSafe(JSON.stringify(opts || {}));
  const cached = _safeCacheGet(cacheKey);
  if (cached) return cached;

  const fAll = _filterLancamentosAll(ssId, opts);

  // Paginação
  const page = Math.max(1, Number((opts && opts.page) || 1));
  const pageSize = Math.min(CONFIG.MAX_PAGE_SIZE, Math.max(1, Number((opts && opts.pageSize) || 200)));
  const start = (page - 1) * pageSize;
  const items = fAll.slice(start, start + pageSize);

  const result = { total: fAll.length, page, pageSize, items };
  _safeCachePut(cacheKey, result, CONFIG.CACHE_SECONDS);
  return result;
}

/**********************************
 * 4) MÉTRICAS (sempre sobre o conjunto TOTAL filtrado)
 **********************************/
function getMetrics(token, opts) {
  const { ssId } = _requireSession(token);
  const key = 'met:' + ssId + ':' + Utilities.base64EncodeWebSafe(JSON.stringify(opts || {}));
  const c = _safeCacheGet(key);
  if (c) return c;

  // usa todas as linhas filtradas, sem paginação
  const all = _filterLancamentosAll(ssId, opts || {});

  let entradas = 0, saidas = 0;
  const porCategoria = {};

  for (const it of all) {
    if (norm(it.tipo) === 'entrada') entradas += it.valor; else saidas += it.valor;
    const cat = it.categoria || '—';
    porCategoria[cat] = (porCategoria[cat] || 0) + (norm(it.tipo) === 'entrada' ? it.valor : -it.valor);
  }

  const saldo = entradas - saidas;
  const topCategorias = Object.entries(porCategoria)
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .slice(0, 5)
    .map(([categoria, valor]) => ({ categoria, valor }));

  const out = { ts: _nowISO(), entradas, saidas, saldo, topCategorias, sample: all.length };
  _safeCachePut(key, out, CONFIG.CACHE_SECONDS);
  return out;
}

/**********************************
 * 5) FILTROS (opções distintas para dropdowns)
 **********************************/
function getFilterOptions(token) {
  const { ssId } = _requireSession(token);
  const key = 'opts:' + ssId;
  const c = _safeCacheGet(key);
  if (c) return c;

  const ss = _openClientSS(ssId);
  const sh = getSheetByName(ss, CONFIG.TABS.LANCAMENTOS);
  const { rows } = readTable(sh);

  const set = (arr) => Array.from(new Set(arr.filter(x => (x !== null && x !== undefined && String(x).trim() !== '')))).sort((a, b) => norm(a).localeCompare(norm(b)));

  const tipos = set(rows.map(r => r['Tipo']));
  const categorias = set(rows.map(r => r['Categoria']));
  const subcategorias = set(rows.map(r => r['Subcategoria']));
  const formas = set(rows.map(r => r['Forma Pagamento']));

  const anosMeses = rows.reduce((acc, r) => {
    const d = parseDateBR(r['Data']);
    if (!d) return acc;
    const y = d.getFullYear();
    const m = d.getMonth() + 1;
    acc.years.add(y);
    acc.months.add(`${String(m).padStart(2, '0')}/${y}`);
    return acc;
  }, { years: new Set(), months: new Set() });

  const out = {
    tipos,
    categorias,
    subcategorias,
    formas,
    anos: Array.from(anosMeses.years).sort((a, b) => b - a),
    meses: Array.from(anosMeses.months).sort((a, b) => {
      const [ma, ya] = a.split('/').map(Number);
      const [mb, yb] = b.split('/').map(Number);
      return yb - ya || mb - ma;
    })
  };
  _safeCachePut(key, out, CONFIG.CACHE_SECONDS);
  return out;
}

/**********************************
 * 6) CRUD
 **********************************/
function upsertLancamento(token, rowObj) {
  const { ssId } = _requireSession(token);
  const ss = _openClientSS(ssId);
  const sh = getSheetByName(ss, CONFIG.TABS.LANCAMENTOS);
  const lock = LockService.getDocumentLock();
  lock.waitLock(20000);
  try {
    const { headers, rows } = readTable(sh);

    const keyOf = (o) => [o['Data'], o['Descrição'], parseMoney(o['Valor Total'])].join('||');
    const incomingKey = keyOf(rowObj);
    const idx = rows.findIndex(r => keyOf(r) === incomingKey);

    const payload = Object.assign({}, rowObj);

    // Normalizações
    if (payload['Valor Total'] !== undefined) payload['Valor Total'] = parseMoney(payload['Valor Total']);
    if (payload['Parcelado'] !== undefined) payload['Parcelado'] = parseBool(payload['Parcelado']) ? 'Sim' : 'Não';
    if (payload['Status'] !== undefined) payload['Status'] = parseBool(payload['Status']);

    if (idx >= 0) {
      writeRow(sh, headers, payload, idx + 2);
    } else {
      // se faltar alguma coluna, completa
      const row = headers.map(h => (payload[h] !== undefined ? payload[h] : ''));
      sh.appendRow(row);
    }

    return { ok: true };
  } finally {
    lock.releaseLock();
  }
}

function deleteLancamento(token, rowKey) {
  const { ssId } = _requireSession(token);
  const ss = _openClientSS(ssId);
  const sh = getSheetByName(ss, CONFIG.TABS.LANCAMENTOS);
  const lock = LockService.getDocumentLock();
  lock.waitLock(20000);
  try {
    const { rows } = readTable(sh);
    const idx = rows.findIndex(r => {
      const k = [r['Data'], r['Descrição'], parseMoney(r['Valor Total'])].join('||');
      return k === rowKey;
    });
    if (idx < 0) return { ok: false, msg: 'Registro não encontrado.' };
    sh.deleteRow(idx + 2);
    return { ok: true };
  } finally {
    lock.releaseLock();
  }
}

/**********************************
 * 7) DIAGNÓSTICO (opcional)
 **********************************/
function whoami(token) { return _requireSession(token); }
function version() { return { name: 'Controle$e Backend', updatedAt: '2025-09-18', tz: Session.getScriptTimeZone() }; }

/**********************************
 * 8) WEB APP
 **********************************/
// Router multi-page: login por padrão
function doGet(e) {
  const view = (e && e.parameter && e.parameter.view) ? String(e.parameter.view) : 'login';
  return renderView(view)
    .setTitle('Controle$e')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function renderView(view) {
  const allowed = new Set(['login','dashboard','formulario','calendario','inteligencia']);
  const v = allowed.has(view) ? view : 'login';
  const t = HtmlService.createTemplateFromFile('Layout'); // TEMPLATE
  t.view = v;
  return t.evaluate();
}

// Necessária para <?!= include('...') ?>
function include(name) {
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}
