/**
 * Simulation Time - page logic
 */
const base = '';

function showMsg(t, err) {
  const el = document.getElementById('msg');
  el.textContent = t;
  el.style.display = t ? 'block' : 'none';
  el.className = 'msg' + (err ? ' err' : ' ok');
}

async function fetchNow() {
  const r = await fetch(base + '/now');
  const d = await r.json();
  const el = document.getElementById('now');
  el.textContent = d.now || '—';
  el.className = 'card-value';
  return d.now;
}

function fillSetTimeFromNow(isoStr) {
  if (!isoStr) return;
  const m = isoStr.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  if (m) {
    const dateEl = document.getElementById('setDate');
    const timeEl = document.getElementById('setTimeTime');
    if (dateEl) dateEl.value = m[1] + '-' + m[2] + '-' + m[3];
    if (timeEl) timeEl.value = m[4] + ':' + m[5];
  }
}

async function setTime() {
  const dateEl = document.getElementById('setDate');
  const timeEl = document.getElementById('setTimeTime');
  const dateVal = (dateEl && dateEl.value) || '';
  const timeVal = (timeEl && timeEl.value) || '00:00';
  if (!dateVal) { showMsg('Select a date (UTC)', true); return; }
  const timePart = /^\d{1,2}:\d{2}(:\d{2})?$/.test(timeVal) ? (timeVal.length === 5 ? timeVal + ':00' : timeVal) : '00:00:00';
  const nowUtc = dateVal + 'T' + timePart + 'Z';
  const dt = new Date(nowUtc);
  if (isNaN(dt.getTime())) { showMsg('Invalid date/time', true); return; }
  const nowUtcNorm = dt.toISOString().replace(/\.\d{3}Z$/, 'Z');
  try {
    const r = await fetch(base + '/set', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ now: nowUtcNorm })
    });
    const d = await r.json();
    if (!r.ok) { showMsg(d.error || r.statusText, true); return; }
    await fetchNow();
    showMsg('Set to ' + d.now);
  } catch (e) { showMsg(e.message, true); }
}

async function advanceTime() {
  const unit = document.getElementById('advUnit').value;
  const val = parseInt(document.getElementById('advVal').value, 10) || 1;
  const body = { [unit]: val };
  try {
    const r = await fetch(base + '/advance', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!r.ok) { showMsg(d.error || r.statusText, true); return; }
    await fetchNow();
    showMsg('Advanced to ' + d.now);
  } catch (e) { showMsg(e.message, true); }
}

let statusPollTimer = null;
function setStatusHtml(html) {
  const el = document.getElementById('advanceTickStatus');
  if (el) el.innerHTML = html;
}
function setCancelVisible(visible) {
  const el = document.getElementById('btnTickCancel');
  if (el) el.style.display = visible ? 'block' : 'none';
}
function setNowDisplay(isoStr) {
  const el = document.getElementById('now');
  if (el && isoStr) { el.textContent = isoStr; el.className = 'card-value'; }
}
async function pollAdvanceTickStatus() {
  try {
    const r = await fetch(base + '/advance-and-tick/status');
    const s = await r.json();
    if (s.now) setNowDisplay(s.now);
    if (s.running) {
      setStatusHtml('Running: step ' + (s.steps_done || 0) + '/' + (s.steps_total || 0) + ', executed=' + (s.executed_total || 0));
      setCancelVisible(true);
      stopNowPoll();
      return;
    }
    if (statusPollTimer) { clearInterval(statusPollTimer); statusPollTimer = null; }
    startNowPoll();
    setCancelVisible(false);
    await fetchNow();
    if (s.error) {
      setStatusHtml('Error: ' + s.error);
      showMsg('Advance-and-tick error: ' + s.error, true);
    } else if (s.cancelled) {
      setStatusHtml('Cancelled at step ' + (s.steps_done || 0) + '/' + (s.steps_total || 0) + ', executed=' + (s.executed_total || 0));
      showMsg('Cancelled. ' + (s.now ? 'Sim time: ' + s.now : ''));
    } else {
      setStatusHtml('Done: ' + (s.steps_done || 0) + ' steps, executed=' + (s.executed_total || 0) + (s.now ? ', now=' + s.now : ''));
      showMsg('Advanced ' + (s.steps_done || 0) + ' steps, executed=' + (s.executed_total ?? '?'));
    }
  } catch (e) {
    setStatusHtml('');
    setCancelVisible(false);
    if (statusPollTimer) { clearInterval(statusPollTimer); statusPollTimer = null; }
  }
}
async function cancelAdvanceTick() {
  try {
    const r = await fetch(base + '/advance-and-tick/cancel', { method: 'POST' });
    const d = await r.json().catch(() => ({}));
    showMsg(d.status === 'cancel_requested' ? 'Cancel requested.' : (d.status || ''));
  } catch (e) { showMsg('Cancel failed: ' + e.message, true); }
}
// POST /advance-and-tick (server runs tick in background); on 202 poll status and show progress
async function advanceAndTick() {
  const chk = document.getElementById('chkFineStep');
  const unit = document.getElementById('advUnit').value;
  const val = Math.max(1, parseInt(document.getElementById('advVal').value, 10) || 1);
  let body;
  if (chk && chk.checked) {
    const stepMin = parseInt(document.getElementById('fineStepMinutes').value, 10) || 30;
    const stepsPerDay = stepMin === 5 ? 288 : stepMin === 15 ? 96 : 48;
    // Multiply by advance amount so e.g. "3 days" => 3 * 48 = 144 steps (continuous ticks over 3 days)
    let stepCount = stepsPerDay;
    if (unit === 'days') stepCount = stepsPerDay * val;
    else if (unit === 'hours') stepCount = Math.max(1, Math.floor(stepsPerDay * val / 24));
    else if (unit === 'minutes') stepCount = Math.max(1, Math.floor(val / stepMin));
    else if (unit === 'seconds') stepCount = Math.max(1, Math.floor(val / (stepMin * 60)));
    body = { minutes: stepMin, steps: stepCount, snap_to_boundary: true };
  } else {
    body = { [unit]: val };
  }
  try {
    const r = await fetch(base + '/advance-and-tick', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const d = await r.json().catch(() => ({}));
    if (r.status === 202) {
      const msg = body.steps
        ? 'Started ' + body.steps + ' steps × ' + body.minutes + ' min. Polling status...'
        : 'Started ' + Object.values(body)[0] + ' ' + Object.keys(body)[0] + '. Polling status...';
      showMsg(msg);
      setStatusHtml('Started...');
      setCancelVisible(true);
      if (statusPollTimer) clearInterval(statusPollTimer);
      statusPollTimer = setInterval(pollAdvanceTickStatus, 2000);
      stopNowPoll();
      pollAdvanceTickStatus();
      return;
    }
    if (r.ok) {
      await fetchNow();
      const desc = body.steps ? body.steps + ' steps × ' + body.minutes + ' min' : (Object.values(body)[0] + ' ' + Object.keys(body)[0]);
      showMsg('Advanced ' + desc + ', executed=' + (d.executed_total ?? '?'));
      return;
    }
    if (r.status === 409) {
      showMsg('Already running. Check status or cancel.', true);
      setStatusHtml('Already running.');
      setCancelVisible(true);
      if (!statusPollTimer) statusPollTimer = setInterval(pollAdvanceTickStatus, 2000);
      pollAdvanceTickStatus();
      return;
    }
    showMsg(d.error || r.statusText, true);
  } catch (e) { showMsg('Failed: ' + e.message, true); }
}

async function fetchConfig() {
  try {
    const r = await fetch(base + '/config');
    const d = await r.json();
    const urls = Array.isArray(d.tick_urls) ? d.tick_urls : [];
    const urlStr = urls.join(', ');
    const timeout = d.zuilow_tick_timeout != null ? Number(d.zuilow_tick_timeout) : 600;
    const inp = document.getElementById('zuilowTickUrl');
    const timeoutInp = document.getElementById('zuilowTickTimeout');
    const disp = document.getElementById('zuilowTickUrlDisplay');
    if (inp) inp.value = urlStr;
    if (timeoutInp) timeoutInp.value = timeout >= 1 ? timeout : '';
    if (disp) disp.textContent = (urls.length ? 'Current (' + urls.length + '): ' + urls.join(' → ') : 'Not set (set env TICK_URLS)')
      + (timeout >= 1 ? ' · Timeout: ' + timeout + ' s' : '');
  } catch (e) {
    const disp = document.getElementById('zuilowTickUrlDisplay');
    if (disp) disp.textContent = 'Failed to load config';
  }
}
async function setZuilowTickUrl() {
  const inp = document.getElementById('zuilowTickUrl');
  const timeoutInp = document.getElementById('zuilowTickTimeout');
  const raw = (inp && inp.value || '').trim();
  let payload;
  if (raw.includes(',')) {
    payload = { tick_urls: raw.split(',').map(s => s.trim()).filter(Boolean) };
  } else if (raw) {
    payload = { zuilow_tick_url: raw.replace(/\/+$/, '') };
  } else {
    payload = { tick_urls: [] };
  }
  const t = timeoutInp && timeoutInp.value !== '' ? parseInt(timeoutInp.value, 10) : null;
  if (t != null && !isNaN(t) && t >= 1) payload.zuilow_tick_timeout = t;
  try {
    const r = await fetch(base + '/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const d = await r.json();
    if (!r.ok) { showMsg(d.error || r.statusText, true); return; }
    const urls = Array.isArray(d.tick_urls) ? d.tick_urls : [];
    const timeout = d.zuilow_tick_timeout != null ? Number(d.zuilow_tick_timeout) : 600;
    const disp = document.getElementById('zuilowTickUrlDisplay');
    if (disp) disp.textContent = (urls.length ? 'Current (' + urls.length + '): ' + urls.join(' → ') : 'Cleared (using env TICK_URLS)')
      + (timeout >= 1 ? ' · Timeout: ' + timeout + ' s' : '');
    showMsg('Config saved.' + (payload.zuilow_tick_timeout != null ? ' Timeout: ' + payload.zuilow_tick_timeout + ' s' : ''));
  } catch (e) { showMsg('Failed: ' + e.message, true); }
}
async function setZuilowTickTimeout() {
  const timeoutInp = document.getElementById('zuilowTickTimeout');
  const t = timeoutInp && timeoutInp.value !== '' ? parseInt(timeoutInp.value, 10) : null;
  if (t == null || isNaN(t) || t < 1) {
    showMsg('Enter a valid timeout (seconds, 1–86400)', true);
    return;
  }
  try {
    const r = await fetch(base + '/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zuilow_tick_timeout: t })
    });
    const d = await r.json();
    if (!r.ok) { showMsg(d.error || r.statusText, true); return; }
    const disp = document.getElementById('zuilowTickUrlDisplay');
    const urls = Array.isArray(d.tick_urls) ? d.tick_urls : [];
    const timeout = d.zuilow_tick_timeout != null ? Number(d.zuilow_tick_timeout) : 600;
    if (disp) disp.textContent = (urls.length ? 'Current (' + urls.length + '): ' + urls.join(' → ') : 'Not set')
      + (timeout >= 1 ? ' · Timeout: ' + timeout + ' s' : '');
    showMsg('Timeout set to ' + t + ' s');
  } catch (e) { showMsg('Failed: ' + e.message, true); }
}

function updateFineStep() {
  const chk = document.getElementById('chkFineStep');
  const hint = document.getElementById('fineStepHint');
  const row = document.getElementById('fineStepRow');
  if (chk && chk.checked) {
    if (hint) hint.style.display = 'block';
    if (row) row.style.display = 'flex';
  } else {
    if (hint) hint.style.display = 'none';
    if (row) row.style.display = 'none';
  }
}

function setDefaultSetTime() {
  // Leave date/time empty on load; use "Fill current" or fetchNow then fillSetTimeFromNow
}

var nowPollTimer = null;
function startNowPoll() {
  if (nowPollTimer) return;
  nowPollTimer = setInterval(function () { fetchNow().catch(function () {}); }, 10000);
}
function stopNowPoll() {
  if (nowPollTimer) { clearInterval(nowPollTimer); nowPollTimer = null; }
}

// Init on DOM ready
document.addEventListener('DOMContentLoaded', function () {
  const chkFine = document.getElementById('chkFineStep');
  if (chkFine) chkFine.addEventListener('change', updateFineStep);
  updateFineStep();
  setDefaultSetTime();

  document.getElementById('btnSet').addEventListener('click', setTime);
  const btnFill = document.getElementById('btnFillNow');
  if (btnFill) btnFill.addEventListener('click', function () {
    fetchNow().then(function (now) {
      fillSetTimeFromNow(now);
      showMsg('Filled with current sim time');
    }).catch(function () { showMsg('Could not fetch current time', true); });
  });
  document.getElementById('btnAdvance').addEventListener('click', advanceTime);
  document.getElementById('btnTick').addEventListener('click', advanceAndTick);
  document.getElementById('btnTickCancel').addEventListener('click', cancelAdvanceTick);
  document.getElementById('btnSetTickUrl').addEventListener('click', setZuilowTickUrl);
  document.getElementById('btnSetTickTimeout').addEventListener('click', setZuilowTickTimeout);

  fetchNow().then(fillSetTimeFromNow).catch(function () { document.getElementById('now').textContent = '—'; });
  fetchConfig();
  startNowPoll();
});
