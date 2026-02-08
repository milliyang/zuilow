/**
 * DMS Frontend JavaScript
 */

// Footer time: Real Time = browser (UTC/HKT). Layout/font match ZuiLow/PPT.
var FOOTER_TIME_LABEL_REAL = 'Real_ Time:';

function formatInTZ(d, tz) {
    if (typeof d === 'string') d = new Date(d);
    var opts = { timeZone: tz, year: 'numeric', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
    var parts = new Intl.DateTimeFormat('en-CA', opts).formatToParts(d);
    var get = function(k) { return (parts.find(function(p) { return p.type === k; }) || {}).value || ''; };
    var y = get('year');
    var m = String(parseInt(get('month'), 10)).padStart(2, '0');
    var day = String(parseInt(get('day'), 10)).padStart(2, '0');
    var h = get('hour');
    var min = get('minute');
    var s = get('second');
    return y + '/' + m + '/' + day + ' ' + h + ':' + min + ':' + s;
}

function refreshFooterTime(elementId) {
    var el = document.getElementById(elementId);
    if (!el) return;
    var real = new Date();
    var realUtc = formatInTZ(real, 'UTC');
    var realHkt = formatInTZ(real, 'Asia/Hong_Kong');
    el.textContent = FOOTER_TIME_LABEL_REAL + ' ' + realUtc + ' (UTC) / ' + realHkt + ' (HKT)';
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    refreshAllNodes();
    loadSyncStatus();
    loadSyncHistory();
    loadTasks();
    if (document.getElementById('footer-time')) {
        refreshFooterTime('footer-time');
    }
    
    // Bind button click events (direct binding only to avoid duplicate triggers)
    setTimeout(function() {
        const btnTriggerAll = document.getElementById('btn-trigger-all');
        const btnTriggerIncremental = document.getElementById('btn-trigger-incremental');
        
        if (btnTriggerAll) {
            btnTriggerAll.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                triggerAllTasks(e);
            });
        }
    }, 500);
});

// Refresh all nodes status
async function refreshAllNodes() {
    try {
        const response = await fetch('/api/dms/nodes');
        const data = await response.json();
        
        // Update summary
        const summaryEl = document.getElementById('nodes-summary');
        if (summaryEl && data.nodes) {
            const online = data.online_nodes || 0;
            const total = data.total_nodes || 0;
            summaryEl.textContent = `(${online}/${total} online)`;
        }
        
        // Webhook Token (API Key for server-to-server)
        const webhookEl = document.getElementById('webhook-token-info');
        if (webhookEl) {
            const token = data.webhook_token;
            webhookEl.innerHTML = '<strong style="display:inline-block;width:120px;">Webhook Token:</strong> ' + (token ? token : '(not set)');
        }
        
        // Display nodes
        displayNodes(data.nodes);
        
        // Show/hide panels based on role
        const currentRole = getCurrentRole(data.nodes);
        if (currentRole === 'master') {
            showMasterPanel();
        } else if (currentRole === 'slave') {
            showSlavePanel();
        }
        
    } catch (e) {
        console.error('Failed to refresh nodes:', e);
    }
}

// Display nodes
function displayNodes(nodes) {
    const container = document.getElementById('nodes-container');
    
    if (!nodes || nodes.length === 0) {
        container.innerHTML = '<div class="empty-state">No nodes</div>';
        return;
    }
    
    container.innerHTML = nodes.map(node => {
        const statusClass = getStatusClass(node.status);
        const roleClass = node.role === 'master' ? 'master' : 'slave';
        const roleText = node.role === 'master' ? 'Master' : 'Slave';
        
        return `
            <div class="node-card">
                <div class="node-header">
                    <span class="node-name">
                        <span class="status-dot ${statusClass}"></span>
                        ${node.name || 'Unknown'}
                    </span>
                    <span class="node-role ${roleClass}">${roleText}</span>
                </div>
                <div class="node-info">
                    <div><strong style="display:inline-block;width:80px;">Address:</strong> ${node.host}:${node.port}</div>
                    <div><strong style="display:inline-block;width:80px;">Status:</strong> ${getStatusText(node.status)}</div>
                    ${node.uptime !== undefined ? `<div><strong style="display:inline-block;width:80px;">Uptime:</strong> ${formatUptime(node.uptime)}</div>` : ''}
                    ${node.tasks_count !== undefined ? `<div><strong style="display:inline-block;width:80px;">Tasks:</strong> ${node.tasks_count}</div>` : ''}
                    ${node.last_sync ? `<div><strong style="display:inline-block;width:80px;">Last Sync:</strong> ${formatTime(node.last_sync)}</div>` : ''}
                    ${node.sync_delay !== undefined ? `<div><strong style="display:inline-block;width:80px;">Sync Delay:</strong> ${node.sync_delay}s</div>` : ''}
                    ${node.error ? `<div style="color:#f85149;"><strong style="display:inline-block;width:80px;">Error:</strong> ${node.error}</div>` : ''}
                </div>
            </div>
        `;
    }).join('');
}

// Get status class
function getStatusClass(status) {
    if (status === 'running' || status === 'online') {
        return 'running';
    } else if (status === 'stopped' || status === 'offline') {
        return 'stopped';
    } else {
        return 'error';
    }
}

// Get status text
function getStatusText(status) {
    const statusMap = {
        'running': 'Running',
        'online': 'Online',
        'stopped': 'Stopped',
        'offline': 'Offline',
    };
    return statusMap[status] || status;
}

// Get current node role
function getCurrentRole(nodes) {
    if (nodes && nodes.length > 0) {
        // Find local node (usually the first one or the one matching current host)
        for (const node of nodes) {
            if (node.role === 'slave' && node.status === 'running') {
                return 'slave';
            }
        }
        // Default to master if we have master node
        for (const node of nodes) {
            if (node.role === 'master') {
                return 'master';
            }
        }
    }
    return 'master'; // Default
}

// Show master panel
function showMasterPanel() {
    const masterPanel = document.getElementById('master-panel');
    const masterSyncPanel = document.getElementById('master-panel-sync');
    const slavePanel = document.getElementById('slave-panel');
    
    if (masterPanel) masterPanel.classList.remove('hidden');
    if (masterSyncPanel) masterSyncPanel.classList.remove('hidden');
    if (slavePanel) slavePanel.classList.add('hidden');
}

// Show slave panel
function showSlavePanel() {
    const masterPanel = document.getElementById('master-panel');
    const masterSyncPanel = document.getElementById('master-panel-sync');
    const slavePanel = document.getElementById('slave-panel');
    
    if (masterPanel) masterPanel.classList.add('hidden');
    if (masterSyncPanel) masterSyncPanel.classList.add('hidden');
    if (slavePanel) slavePanel.classList.remove('hidden');
}

// Load sync status
async function loadSyncStatus() {
    try {
        const response = await fetch('/api/dms/sync/status');
        const data = await response.json();
        
        const container = document.getElementById('sync-status-container');
        if (!container) return;
        
        if (!data.backups || data.backups.length === 0) {
            container.innerHTML = '<div class="empty-state">No backup nodes</div>';
            return;
        }
        
        container.innerHTML = data.backups.map(backup => {
            const lastSync = backup.last_sync ? formatTime(backup.last_sync) : 'Never synced';
            return `
                <div style="padding: 8px 0; border-bottom: 1px solid #21262d;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                        <strong style="color: #f0f6fc;">${backup.name}</strong>
                        <span style="color: #8b949e; font-size: 11px;">${backup.host}:${backup.port}</span>
                    </div>
                    <div style="color: #8b949e; font-size: 11px;">Last Sync: ${lastSync}</div>
                </div>
            `;
        }).join('');
        
    } catch (e) {
        console.error('Failed to load sync status:', e);
    }
}

// Load sync history
async function loadSyncHistory() {
    try {
        const response = await fetch('/api/dms/sync/history?limit=10');
        const data = await response.json();
        
        const container = document.getElementById('sync-history-container');
        if (!container) return;
        
        if (!data || data.length === 0) {
            container.innerHTML = '<div class="empty-state">No sync history</div>';
            return;
        }
        
        container.innerHTML = data.map(item => {
            const statusClass = item.status || 'unknown';
            const statusText = {
                'success': 'Success',
                'failed': 'Failed',
                'running': 'Running',
            }[statusClass] || statusClass;
            
            const startTime = item.start_time ? formatTime(item.start_time) : '-';
            const endTime = item.end_time ? formatTime(item.end_time) : '-';
            const dataCount = item.data_count || 0;
            
            return `
                <div class="sync-history-item">
                    <div class="sync-meta">
                        <div>
                            <strong>${item.backup_name}</strong>
                            ${item.symbol ? `<span style="color:#8b949e;margin-left:8px;">${item.symbol}</span>` : ''}
                        </div>
                        <span class="sync-status ${statusClass}">${statusText}</span>
                    </div>
                    <div style="color:#8b949e;font-size:10px;">
                        ${item.sync_mode || ''} | ${startTime} → ${endTime} | ${dataCount} records
                    </div>
                    ${item.error_message ? `<div style="color:#f85149;font-size:10px;margin-top:4px;">${item.error_message}</div>` : ''}
                </div>
            `;
        }).join('');
        
    } catch (e) {
        console.error('Failed to load sync history:', e);
    }
}

// Request sync (slave only)
async function requestSync() {
    try {
        const response = await fetch('/api/dms/sync/request', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({}),
        });
        
        const result = await response.json();
        
        if (result.success) {
            refreshAllNodes();
            loadSyncHistory();
        } else {
            console.error('Sync request failed:', result.error || 'Unknown error');
        }
    } catch (e) {
        console.error('Failed to request sync:', e);
    }
}

// Format time
function formatTime(timeStr) {
    if (!timeStr) return '-';
    const date = new Date(timeStr);
    return date.toLocaleString('en-US', {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
    });
}

// Format time as UTC / HKT for Last Run column (backend sends UTC)
function formatTimeUtcHkt(timeStr) {
    if (!timeStr) return 'Never run';
    // Ensure UTC: ISO without Z is parsed as local in JS, so normalize to UTC
    const s = timeStr.endsWith('Z') || timeStr.includes('+') ? timeStr : timeStr + 'Z';
    const date = new Date(s);
    if (isNaN(date.getTime())) return timeStr;
    const pad = (n) => String(n).padStart(2, '0');
    const utcY = date.getUTCFullYear();
    const utcM = date.getUTCMonth() + 1;
    const utcD = date.getUTCDate();
    const utcH = date.getUTCHours();
    const utcMin = date.getUTCMinutes();
    const utcStr = `${utcY}/${pad(utcM)}/${pad(utcD)} ${pad(utcH)}:${pad(utcMin)} UTC`;
    // HKT = UTC+8
    const hktDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const hktY = hktDate.getUTCFullYear();
    const hktM = hktDate.getUTCMonth() + 1;
    const hktD = hktDate.getUTCDate();
    const hktH = hktDate.getUTCHours();
    const hktMin = hktDate.getUTCMinutes();
    const hktStr = `${hktY}/${pad(hktM)}/${pad(hktD)} ${pad(hktH)}:${pad(hktMin)} HKT`;
    return `${utcStr} / ${hktStr}`;
}

// Format uptime
function formatUptime(seconds) {
    if (!seconds) return '-';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else {
        return `${minutes}m`;
    }
}

// Load tasks (master only)
async function loadTasks() {
    try {
        const response = await fetch('/api/dms/tasks');
        const tasks = await response.json();
        
        const container = document.getElementById('tasks-container');
        if (!container) return;
        
        if (!tasks || tasks.length === 0) {
            container.innerHTML = '<div class="empty-state">No tasks</div>';
            return;
        }
        
        // Add header row
        const header = `
            <div style="padding: 8px 0; border-bottom: 2px solid #30363d; display: grid; grid-template-columns: 180px 100px 1fr 1fr 1fr 100px; align-items: center; gap: 12px; font-weight: 600; color: #8b949e; font-size: 11px;">
                <div>Task Name</div>
                <div>Status</div>
                <div>Schedule</div>
                <div style="min-width:0;">Notes</div>
                <div style="min-width:0;">Last Run</div>
                <div style="justify-self: end;">Action</div>
            </div>
        `;
        
        container.innerHTML = header + tasks.map(task => {
            const statusClass = task.status || 'idle';
            const statusText = {
                'idle': 'Idle',
                'running': 'Running',
                'completed': 'Completed',
                'failed': 'Failed',
            }[statusClass] || statusClass;
            
            const lastRun = formatTimeUtcHkt(task.last_run_time);
            const schedule = task.schedule || 'Not scheduled';
            const notes = task.notes || '—';
            const isRunning = statusClass === 'running';
            
            return `
                <div style="padding: 8px 0; border-bottom: 1px solid #21262d; display: grid; grid-template-columns: 180px 100px 1fr 1fr 1fr 100px; align-items: center; gap: 12px;">
                    <strong style="color: #f0f6fc; white-space: nowrap;">${task.name}</strong>
                    <span class="sync-status ${statusClass}" style="font-size: 12px; white-space: nowrap;">${statusText}</span>
                    <span style="color: #8b949e; font-size: 11px;">${schedule}</span>
                    <span style="color: #8b949e; font-size: 11px; min-width:0; text-align: left;">${notes}</span>
                    <span style="color: #8b949e; font-size: 11px; min-width:0;">${lastRun}</span>
                    <button class="btn btn-sm" onclick="triggerSingleTask('${task.name}', this)" 
                            ${isRunning ? 'disabled' : ''} 
                            style="background: ${isRunning ? '#21262d' : '#1f6feb'}; color: #fff; padding: 4px 12px; font-size: 11px; border: none; border-radius: 4px; cursor: ${isRunning ? 'not-allowed' : 'pointer'}; white-space: nowrap; justify-self: end;">
                        ${isRunning ? '⏳ Running...' : '▶️ Start'}
                    </button>
                </div>
            `;
        }).join('');
        
    } catch (e) {
        console.error('Failed to load tasks:', e);
    }
}

// Trigger all tasks
async function triggerAllTasks(event) {
    // Show loading state
    const button = event ? event.target : document.querySelector('button[onclick*="triggerAllTasks"]');
    const originalText = button ? button.innerHTML : '';
    if (button) {
        button.disabled = true;
        button.innerHTML = '⏳ Triggering...';
    }
    
    try {
        const response = await fetch('/api/dms/tasks/trigger-all', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        
        if (!response.ok) {
            let errorData;
            try {
                errorData = await response.json();
            } catch (e) {
                errorData = { detail: response.statusText };
            }
            throw new Error(errorData.detail || errorData.message || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            const msg = result.result.message || 'All tasks triggered';
            const triggeredCount = result.result.triggered_count || 0;
            
            // Check if tasks are running in background
            const allRunning = result.result.results && 
                Object.values(result.result.results).every(r => r.status === 'running');
            
            loadTasks();
        } else {
            const errorMsg = result.error || result.message || 'Unknown error';
            console.error('Failed to trigger tasks:', errorMsg);
        }
    } catch (e) {
        console.error('Failed to trigger all tasks:', e);
    } finally {
        // Restore button state
        if (button) {
            button.disabled = false;
            button.innerHTML = originalText;
        }
    }
}

// Trigger single task
async function triggerSingleTask(taskName, buttonElement) {
    const originalText = buttonElement ? buttonElement.innerHTML : '';
    if (buttonElement) {
        buttonElement.disabled = true;
        buttonElement.innerHTML = '⏳ Starting...';
    }
    
    try {
        const response = await fetch(`/api/dms/tasks/trigger?task_name=${encodeURIComponent(taskName)}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        
        if (!response.ok) {
            let errorData;
            try {
                errorData = await response.json();
            } catch (e) {
                errorData = { detail: response.statusText };
            }
            throw new Error(errorData.detail || errorData.message || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            loadTasks();
        } else {
            const errorMsg = result.error || result.message || 'Unknown error';
            console.error(`Failed to trigger task ${taskName}:`, errorMsg);
            if (buttonElement) {
                buttonElement.innerHTML = '❌ Failed';
                setTimeout(() => {
                    buttonElement.innerHTML = originalText;
                    buttonElement.disabled = false;
                }, 2000);
            }
        }
    } catch (e) {
        console.error(`Failed to trigger task ${taskName}:`, e);
        if (buttonElement) {
            buttonElement.innerHTML = '❌ Error';
            setTimeout(() => {
                buttonElement.innerHTML = originalText;
                buttonElement.disabled = false;
            }, 2000);
        }
    }
}

// Trigger all incremental tasks (deprecated, kept for compatibility)
async function triggerAllIncrementalTasks(event) {
    // Show loading state
    const button = event ? event.target : document.querySelector('button[onclick*="triggerAllIncrementalTasks"]');
    const originalText = button ? button.innerHTML : '';
    if (button) {
        button.disabled = true;
        button.innerHTML = '⏳ Triggering...';
    }
    
    try {
        const response = await fetch('/api/dms/tasks/trigger-all?task_type=incremental', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        
        if (!response.ok) {
            let errorData;
            try {
                errorData = await response.json();
            } catch (e) {
                errorData = { detail: response.statusText };
            }
            throw new Error(errorData.detail || errorData.message || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            const msg = result.result.message || 'All incremental update tasks triggered';
            const triggeredCount = result.result.triggered_count || 0;
            
            // Check if tasks are running in background
            const allRunning = result.result.results && 
                Object.values(result.result.results).every(r => r.status === 'running');
            
            loadTasks();
        } else {
            const errorMsg = result.error || result.message || 'Unknown error';
            console.error('Failed to trigger incremental updates:', errorMsg);
        }
    } catch (e) {
        console.error('Failed to trigger incremental tasks:', e);
    } finally {
        // Restore button state
        if (button) {
            button.disabled = false;
            button.innerHTML = originalText;
        }
    }
}

// ============================================================================
// Export Functions
// ============================================================================

// Show export dialog
function showExportDialog() {
    const dialog = document.getElementById('export-dialog');
    dialog.style.display = 'flex';
    
    // Setup export all checkbox handler
    const exportAll = document.getElementById('export-all-symbols');
    const symbolsInput = document.getElementById('export-symbols');
    if (exportAll && symbolsInput) {
        exportAll.addEventListener('change', function() {
            symbolsInput.disabled = this.checked;
            if (this.checked) {
                symbolsInput.value = '';
            }
        });
    }
}

// Close export dialog
function closeExportDialog() {
    const dialog = document.getElementById('export-dialog');
    dialog.style.display = 'none';
    // Reset form
    document.getElementById('export-symbols').value = '';
    document.getElementById('export-interval').value = '1d';
    document.getElementById('export-create-zip').checked = true;
    const exportAll = document.getElementById('export-all-symbols');
    if (exportAll) exportAll.checked = false;
    document.getElementById('export-progress').style.display = 'none';
}

// Start export
async function startExport() {
    const symbolsInput = document.getElementById('export-symbols').value.trim();
    const interval = document.getElementById('export-interval').value;
    const createZip = document.getElementById('export-create-zip').checked;
    const exportAll = document.getElementById('export-all-symbols');
    const exportAllChecked = exportAll ? exportAll.checked : false;
    
    // Determine symbols to export
    let symbols = [];
    if (exportAllChecked) {
        // Export all symbols
        symbols = ["*"];
    } else {
        if (!symbolsInput) {
            const progressEl = document.getElementById('export-progress');
            progressEl.style.display = 'block';
            progressEl.innerHTML = '<div style="color:#f85149;font-size:12px;">Please enter stock symbols or check "Export All Symbols"</div>';
            return;
        }
        // Parse symbols (split by comma)
        symbols = symbolsInput.split(',').map(s => s.trim()).filter(s => s);
    }
    
    // Show progress
    const progressEl = document.getElementById('export-progress');
    progressEl.style.display = 'block';
    const symbolCount = exportAllChecked ? 'all' : symbols.length;
    progressEl.innerHTML = `<div style="color:#8b949e;font-size:12px;">Exporting ${symbolCount} stock(s)...</div>`;
    
    try {
        const response = await fetch('/api/dms/export', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                symbols: symbols,
                interval: interval,
                create_zip: createZip,
            }),
        });
        
        const data = await response.json();
        
        if (data.success) {
            const result = data.result;
            progressEl.innerHTML = `
                <div style="color:#3fb950;font-size:12px;">
                    ✓ Export completed!<br>
                    Exported: ${result.exported_count}/${result.total_symbols} stock(s)<br>
                    ${result.zip_filename ? `ZIP: ${result.zip_filename}` : ''}
                </div>
            `;
            
            // Refresh exports list
            setTimeout(() => {
                refreshExportsList();
                closeExportDialog();
            }, 2000);
        } else {
            progressEl.innerHTML = `<div style="color:#f85149;font-size:12px;">Export failed</div>`;
        }
        
    } catch (e) {
        console.error('Export failed:', e);
        progressEl.innerHTML = `<div style="color:#f85149;font-size:12px;">Export failed: ${e.message}</div>`;
    }
}

// Refresh exports list
async function refreshExportsList() {
    const container = document.getElementById('exports-list-container');
    
    try {
        const response = await fetch('/api/dms/exports');
        const data = await response.json();
        
        if (!data.success || !data.files) {
            container.innerHTML = '<div style="color:#8b949e;font-size:12px;">No export files</div>';
            return;
        }
        
        // Filter only ZIP files
        const zipFiles = data.files.filter(file => file.is_zip);
        
        if (zipFiles.length === 0) {
            container.innerHTML = '<div style="color:#8b949e;font-size:12px;">No export files</div>';
            return;
        }
        
        // Display files (only ZIP files)
        container.innerHTML = `
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr style="border-bottom:1px solid #30363d;text-align:left;">
                        <th style="padding:8px;color:#8b949e;font-size:12px;">Filename</th>
                        <th style="padding:8px;color:#8b949e;font-size:12px;">Size</th>
                        <th style="padding:8px;color:#8b949e;font-size:12px;">Created</th>
                        <th style="padding:8px;color:#8b949e;font-size:12px;">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${zipFiles.map(file => {
                        const createTime = formatTime(file.created);
                        
                        return `
                            <tr style="border-bottom:1px solid #21262d;">
                                <td style="padding:8px;color:#c9d1d9;font-size:12px;">📦 ${file.filename}</td>
                                <td style="padding:8px;color:#8b949e;font-size:12px;">${file.size_mb} MB</td>
                                <td style="padding:8px;color:#8b949e;font-size:12px;">${createTime}</td>
                                <td style="padding:8px;">
                                    <button class="btn btn-sm" onclick="downloadExport('${file.filename}')" 
                                            style="background:#1f6feb;color:#fff;margin-right:4px;">Download</button>
                                    <button class="btn btn-sm" onclick="deleteExport('${file.filename}')" 
                                            style="background:#21262d;border:1px solid #30363d;">Delete</button>
                                </td>
                            </tr>
                        `;
                    }).join('')}
                </tbody>
            </table>
        `;
        
    } catch (e) {
        console.error('Failed to refresh exports:', e);
        container.innerHTML = '<div style="color:#f85149;font-size:12px;">Failed to load</div>';
    }
}

// Download export file
function downloadExport(filename) {
    window.location.href = `/api/dms/exports/${encodeURIComponent(filename)}`;
}

// Delete export file
async function deleteExport(filename) {
    try {
        const response = await fetch(`/api/dms/exports/${encodeURIComponent(filename)}`, {
            method: 'DELETE',
        });
        
        const data = await response.json();
        
        if (data.success) {
            refreshExportsList();
        } else {
            console.error('Delete failed:', data.message || 'Unknown error');
        }
        
    } catch (e) {
        console.error('Delete failed:', e);
    }
}

// Load exports list on init
document.addEventListener('DOMContentLoaded', function() {
    refreshExportsList();
    
    // Setup clear database dialog checkbox
    const confirmCheckbox = document.getElementById('clear-database-confirm-check');
    const confirmBtn = document.getElementById('clear-database-confirm-btn');
    if (confirmCheckbox && confirmBtn) {
        confirmCheckbox.addEventListener('change', function() {
            confirmBtn.disabled = !this.checked;
        });
    }
});

// ============================================================================
// Clear Database Functions
// ============================================================================

// Show clear database dialog
function showClearDatabaseDialog() {
    const dialog = document.getElementById('clear-database-dialog');
    if (dialog) {
        dialog.style.display = 'flex';
        // Reset checkbox
        const checkbox = document.getElementById('clear-database-confirm-check');
        const btn = document.getElementById('clear-database-confirm-btn');
        if (checkbox) checkbox.checked = false;
        if (btn) btn.disabled = true;
    }
}

// Close clear database dialog
function closeClearDatabaseDialog() {
    const dialog = document.getElementById('clear-database-dialog');
    if (dialog) {
        dialog.style.display = 'none';
    }
}

// Confirm clear database
async function confirmClearDatabase() {
    const checkbox = document.getElementById('clear-database-confirm-check');
    if (!checkbox || !checkbox.checked) {
        return;
    }
    
    const btn = document.getElementById('clear-database-confirm-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = 'Clearing...';
    }
    
    try {
        const response = await fetch('/api/dms/database/clear?confirm=true', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            closeClearDatabaseDialog();
        } else {
            console.error('Failed to clear database:', result.message || 'Unknown error');
        }
    } catch (e) {
        console.error('Failed to clear database:', e);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = 'Confirm Clear';
        }
        if (checkbox) {
            checkbox.checked = false;
        }
    }
}

// ============================================================================
// Symbol Info Query Functions
// ============================================================================

// Query all symbols (GET /api/dms/symbols)
async function queryAllSymbols() {
    const container = document.getElementById('symbol-info-container');
    const btn = document.getElementById('btn-all-symbols');
    if (!container) return;
    if (btn) {
        btn.disabled = true;
        btn.textContent = '⏳ Loading...';
    }
    container.innerHTML = '<div class="empty-state">Loading symbols...</div>';
    try {
        const response = await fetch('/api/dms/symbols');
        if (!response.ok) {
            const err = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(err.detail || err.message || 'HTTP ' + response.status);
        }
        const data = await response.json();
        const symbols = data.symbols || [];
        const count = symbols.length;
        const listText = symbols.length ? symbols.join(', ') : '(none)';
        container.innerHTML = `
            <div style="padding: 12px 16px; background: #161b22; border: 1px solid #21262d; border-radius: 6px;">
                <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
                    <span style="color: #c9d1d9; font-weight: 600;">All Symbols (from task config)</span>
                    <span style="color: #8b949e; font-size: 12px;">Total: ${count}</span>
                </div>
                <div style="max-height: 200px; overflow-y: auto; font-size: 12px; color: #8b949e; font-family: monospace; white-space: pre-wrap; word-break: break-all;">${listText}</div>
            </div>
        `;
    } catch (e) {
        container.innerHTML = `<div class="empty-state" style="color: #f85149;">Error: ${e.message}</div>`;
        console.error('Failed to query all symbols:', e);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📋 All Symbols';
        }
    }
}

// Query symbol information
async function querySymbolInfo() {
    const symbolInput = document.getElementById('symbol-query-input');
    const intervalSelect = document.getElementById('symbol-query-interval');
    const container = document.getElementById('symbol-info-container');
    
    if (!symbolInput || !intervalSelect || !container) {
        return;
    }
    
    const symbol = symbolInput.value.trim();
    const interval = intervalSelect.value;
    
    if (!symbol) {
        container.innerHTML = '<div class="empty-state" style="color: #f85149;">Please enter a symbol</div>';
        return;
    }
    
    // Show loading
    container.innerHTML = '<div class="empty-state">Querying...</div>';
    
    try {
        const response = await fetch(`/api/dms/symbol/${encodeURIComponent(symbol)}/info?interval=${encodeURIComponent(interval)}`);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const data = await response.json();
        displaySymbolInfo(data);
        
    } catch (e) {
        container.innerHTML = `<div class="empty-state" style="color: #f85149;">Error: ${e.message}</div>`;
        console.error('Failed to query symbol info:', e);
    }
}

// Display symbol information
function displaySymbolInfo(info) {
    const container = document.getElementById('symbol-info-container');
    if (!container) return;
    
    if (!info.has_data) {
        container.innerHTML = `
            <div style="padding: 12px 16px; background: #161b22; border: 1px solid #21262d; border-radius: 6px;">
                <div style="color: #8b949e; font-size: 12px; display: flex; align-items: center; gap: 16px; flex-wrap: nowrap;">
                    <span style="color: #c9d1d9; font-weight: 600; font-size: 13px;">${info.symbol} (${info.interval})</span>
                    <span style="color: #f85149;">⚠️ No data found in database</span>
                </div>
            </div>
        `;
        return;
    }
    
    const latestDate = info.latest_date ? formatTime(info.latest_date) : 'N/A';
    const earliestDate = info.earliest_date ? formatTime(info.earliest_date) : 'N/A';
    
    // Calculate days gap: calendar days from latest data date to "today" (browser local date).
    // Gap = missing calendar days; e.g. latest 02/05 and today 02/07 => gap 2 (02/06, 02/07 not in DB).
    let daysGap = 'N/A';
    let gapStatus = '';
    let gapHint = '';
    if (info.latest_date) {
        const latest = new Date(info.latest_date);
        const now = new Date();
        // Use date-only for gap so it's calendar days (latest 02/05 00:00, today 02/07 => 2)
        const latestDay = new Date(latest.getFullYear(), latest.getMonth(), latest.getDate());
        const todayDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const diffDays = Math.max(0, Math.round((todayDay - latestDay) / (1000 * 60 * 60 * 24)));
        daysGap = diffDays;
        const todayStr = todayDay.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
        gapHint = diffDays > 0 ? ` <span style="color:#6e7681;">(latest vs today ${todayStr})</span>` : '';
        if (diffDays > 7) {
            gapStatus = '<span style="color: #f85149;">⚠️ Data gap > 7 days</span>';
        } else if (diffDays > 1) {
            gapStatus = '<span style="color: #f0883e;">⚠️ Data gap > 1 day</span>';
        } else {
            gapStatus = '<span style="color: #3fb950;">✅ Data is up to date</span>';
        }
    }
    
    container.innerHTML = `
        <div style="padding: 12px 16px; background: #161b22; border: 1px solid #21262d; border-radius: 6px;">
            <div style="color: #8b949e; font-size: 12px; display: flex; align-items: center; gap: 16px; flex-wrap: nowrap;">
                <span style="color: #c9d1d9; font-weight: 600; font-size: 13px;">${info.symbol} (${info.interval})</span>
                <span><strong style="color: #c9d1d9;">Latest:</strong> <span style="color: #c9d1d9;">${latestDate}</span></span>
                <span><strong style="color: #c9d1d9;">Earliest:</strong> <span style="color: #c9d1d9;">${earliestDate}</span></span>
                <span><strong style="color: #c9d1d9;">Count:</strong> <span style="color: #c9d1d9;">${info.data_count.toLocaleString()}</span></span>
                <span><strong style="color: #c9d1d9;">Gap:</strong> <span style="color: #c9d1d9;">${daysGap} days</span>${gapHint}</span>
                <span>${gapStatus}</span>
                <span style="margin-left: auto;">
                    <button type="button" class="btn btn-primary btn-view-data" 
                            data-symbol="${info.symbol}"
                            data-interval="${info.interval}"
                            style="padding: 4px 10px; font-size: 11px;">
                        📊 View Data
                    </button>
                </span>
            </div>
            <div id="symbol-data-container" style="margin-top: 16px; display: none;"></div>
        </div>
    `;
}

// Load and display symbol data with pagination
let currentSymbolData = {
    symbol: '',
    interval: '',
    page: 1,
    pageSize: 50,
    order: 'desc',
    totalPages: 0
};

let isLoadingSymbolData = false; // Flag to prevent concurrent requests
let currentRequestKey = null; // Track current request to prevent duplicates

async function loadSymbolData(symbol, interval, page = 1, order = 'desc') {
    const container = document.getElementById('symbol-data-container');
    if (!container) return;
    
    // Create unique request key
    const requestKey = `${symbol}_${interval}_${page}_${order}`;
    
    // Prevent concurrent requests - if same request is already loading, skip
    if (isLoadingSymbolData) {
        if (currentRequestKey === requestKey) {
            console.log('Same request already in progress, skipping...');
            return;
        }
        // If different request, allow it (user changed page/order)
    }
    
    isLoadingSymbolData = true;
    currentRequestKey = requestKey;
    
    // Immediately disable all pagination buttons to prevent multiple clicks
    const paginationButtons = container.querySelectorAll('.btn-pagination-first, .btn-pagination-prev, .btn-pagination-next, .btn-pagination-last, .select-pagination-order');
    paginationButtons.forEach(btn => {
        if (btn) {
            btn.disabled = true;
            btn.style.opacity = '0.5';
            btn.style.cursor = 'not-allowed';
        }
    });
    
    currentSymbolData.symbol = symbol;
    currentSymbolData.interval = interval;
    currentSymbolData.page = page;
    currentSymbolData.order = order;
    
    // Check if table already exists - if yes, just show loading in tbody
    const existingTbody = container.querySelector('.symbol-data-tbody');
    if (existingTbody) {
        // Table exists, just update tbody with loading
        existingTbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px; color: #8b949e;">Loading...</td></tr>';
    } else {
        // Table doesn't exist, show full loading
        container.style.display = 'block';
        container.innerHTML = '<div class="empty-state">Loading data...</div>';
    }
    
    try {
        const response = await fetch(
            `/api/dms/symbol/${encodeURIComponent(symbol)}/data?` +
            `interval=${encodeURIComponent(interval)}&` +
            `page=${page}&` +
            `page_size=${currentSymbolData.pageSize}&` +
            `order=${order}`
        );
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const data = await response.json();
        currentSymbolData.totalPages = data.total_pages;
        
        // Save scroll position before update
        const tableContainer = container.querySelector('.symbol-data-table-container');
        const scrollTop = tableContainer ? tableContainer.scrollTop : 0;
        
        displaySymbolData(data);
        
        // Restore scroll position after update (reset to top for new page)
        if (tableContainer) {
            tableContainer.scrollTop = 0;
        }
        
    } catch (e) {
        const existingTbody = container.querySelector('.symbol-data-tbody');
        if (existingTbody) {
            existingTbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #f85149;">Error: ${e.message}</td></tr>`;
        } else {
            container.innerHTML = `<div class="empty-state" style="color: #f85149;">Error: ${e.message}</div>`;
        }
        console.error('Failed to load symbol data:', e);
    } finally {
        // Reset loading state - buttons will be re-enabled by displaySymbolData
        isLoadingSymbolData = false;
        currentRequestKey = null;
    }
}

// Display symbol data table
function displaySymbolData(data) {
    const container = document.getElementById('symbol-data-container');
    if (!container) return;
    
    if (!data.data || data.data.length === 0) {
        container.innerHTML = '<div class="empty-state">No data found</div>';
        return;
    }
    
    // Format number with commas
    function formatNumber(num) {
        if (typeof num === 'number') {
            return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
        return num;
    }
    
    // Format date
    function formatDate(dateStr) {
        const date = new Date(dateStr);
        return date.toLocaleString('en-US', {
            month: '2-digit',
            day: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            hour12: true
        });
    }
    
    // Check if table already exists
    let tableContainer = container.querySelector('.symbol-data-table-container');
    let tbody = container.querySelector('.symbol-data-tbody');
    let paginationDiv = container.querySelector('.symbol-data-pagination');
    
    // If table doesn't exist, create it
    if (!tableContainer) {
        container.innerHTML = `
            <div style="margin-top: 16px; border-top: 1px solid #21262d; padding-top: 16px;">
                <div class="symbol-data-table-container" style="overflow-x: auto; overflow-y: auto; max-height: 600px; border: 1px solid #21262d; border-radius: 4px;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 11px;">
                        <thead style="position: sticky; top: 0; background: #161b22; z-index: 10;">
                            <tr style="border-bottom: 2px solid #21262d;">
                                <th style="padding: 8px; text-align: left; color: #8b949e; font-weight: 600; background: #161b22;">Time</th>
                                <th style="padding: 8px; text-align: right; color: #8b949e; font-weight: 600; background: #161b22;">Open</th>
                                <th style="padding: 8px; text-align: right; color: #8b949e; font-weight: 600; background: #161b22;">High</th>
                                <th style="padding: 8px; text-align: right; color: #8b949e; font-weight: 600; background: #161b22;">Low</th>
                                <th style="padding: 8px; text-align: right; color: #8b949e; font-weight: 600; background: #161b22;">Close</th>
                                <th style="padding: 8px; text-align: right; color: #8b949e; font-weight: 600; background: #161b22;">Volume</th>
                            </tr>
                        </thead>
                        <tbody class="symbol-data-tbody">
                        </tbody>
                    </table>
                </div>
                <div class="symbol-data-pagination"></div>
            </div>
        `;
        tableContainer = container.querySelector('.symbol-data-table-container');
        tbody = container.querySelector('.symbol-data-tbody');
        paginationDiv = container.querySelector('.symbol-data-pagination');
    }
    
    // Only update tbody content
    const rows = data.data.map(record => `
        <tr style="border-bottom: 1px solid #21262d;">
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px;">${formatDate(record.time)}</td>
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px; text-align: right;">${formatNumber(record.Open)}</td>
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px; text-align: right;">${formatNumber(record.High)}</td>
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px; text-align: right;">${formatNumber(record.Low)}</td>
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px; text-align: right;">${formatNumber(record.Close)}</td>
            <td style="padding: 8px; color: #c9d1d9; font-size: 11px; text-align: right;">${record.Volume.toLocaleString()}</td>
        </tr>
    `).join('');
    
    tbody.innerHTML = rows;
    
    // Update pagination controls
    paginationDiv.innerHTML = `
        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 12px; padding-top: 12px; border-top: 1px solid #21262d;">
            <div style="color: #8b949e; font-size: 11px;">
                Page ${data.page} of ${data.total_pages} (${data.total.toLocaleString()} total records)
            </div>
            <div style="display: flex; gap: 8px; align-items: center;" id="symbol-data-pagination">
                <button type="button" class="btn-pagination-first" 
                        data-symbol="${data.symbol}"
                        data-interval="${data.interval}"
                        data-page="1"
                        data-order="${data.order}"
                        ${data.page === 1 ? 'disabled' : ''}
                        style="padding: 4px 8px; font-size: 11px; background: ${data.page === 1 ? '#21262d' : '#1f6feb'}; color: #fff; border: none; border-radius: 4px; cursor: ${data.page === 1 ? 'not-allowed' : 'pointer'};">
                    ⏮ First
                </button>
                <button type="button" class="btn-pagination-prev" 
                        data-symbol="${data.symbol}"
                        data-interval="${data.interval}"
                        data-page="${data.page - 1}"
                        data-order="${data.order}"
                        ${data.page === 1 ? 'disabled' : ''}
                        style="padding: 4px 8px; font-size: 11px; background: ${data.page === 1 ? '#21262d' : '#1f6feb'}; color: #fff; border: none; border-radius: 4px; cursor: ${data.page === 1 ? 'not-allowed' : 'pointer'};">
                    ◀ Prev
                </button>
                <button type="button" class="btn-pagination-next" 
                        data-symbol="${data.symbol}"
                        data-interval="${data.interval}"
                        data-page="${data.page + 1}"
                        data-order="${data.order}"
                        ${data.page === data.total_pages ? 'disabled' : ''}
                        style="padding: 4px 8px; font-size: 11px; background: ${data.page === data.total_pages ? '#21262d' : '#1f6feb'}; color: #fff; border: none; border-radius: 4px; cursor: ${data.page === data.total_pages ? 'not-allowed' : 'pointer'};">
                    Next ▶
                </button>
                <button type="button" class="btn-pagination-last" 
                        data-symbol="${data.symbol}"
                        data-interval="${data.interval}"
                        data-page="${data.total_pages}"
                        data-order="${data.order}"
                        ${data.page === data.total_pages ? 'disabled' : ''}
                        style="padding: 4px 8px; font-size: 11px; background: ${data.page === data.total_pages ? '#21262d' : '#1f6feb'}; color: #fff; border: none; border-radius: 4px; cursor: ${data.page === data.total_pages ? 'not-allowed' : 'pointer'};">
                    Last ⏭
                </button>
                <select class="select-pagination-order"
                        data-symbol="${data.symbol}"
                        data-interval="${data.interval}"
                        style="padding: 4px 8px; font-size: 11px; background: #0d1117; border: 1px solid #21262d; border-radius: 4px; color: #c9d1d9;">
                    <option value="desc" ${data.order === 'desc' ? 'selected' : ''}>Newest First</option>
                    <option value="asc" ${data.order === 'asc' ? 'selected' : ''}>Oldest First</option>
                </select>
            </div>
        </div>
    `;
    
    // Ensure all buttons are properly enabled/disabled after DOM update
    // The disabled attribute in the HTML should handle this, but we ensure it
    const paginationButtons = paginationDiv.querySelectorAll('.btn-pagination-first, .btn-pagination-prev, .btn-pagination-next, .btn-pagination-last, .select-pagination-order');
    paginationButtons.forEach(btn => {
        if (btn) {
            // Reset opacity and cursor for enabled buttons
            if (!btn.disabled) {
                btn.style.opacity = '1';
                btn.style.cursor = 'pointer';
            }
        }
    });
}

// Event delegation for pagination buttons and view data button
document.addEventListener('click', function(e) {
    // Handle "View Data" button
    if (e.target.classList.contains('btn-view-data') || e.target.closest('.btn-view-data')) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        
        const btn = e.target.classList.contains('btn-view-data') ? e.target : e.target.closest('.btn-view-data');
        const symbol = btn.getAttribute('data-symbol');
        const interval = btn.getAttribute('data-interval');
        
        loadSymbolData(symbol, interval, 1, 'desc');
        return false;
    }
    
    // Handle pagination buttons
    const paginationBtn = e.target.closest('.btn-pagination-first, .btn-pagination-prev, .btn-pagination-next, .btn-pagination-last');
    if (paginationBtn) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        
        // Check if already disabled or loading
        if (paginationBtn.disabled || isLoadingSymbolData) {
            return false;
        }
        
        // Immediately disable the clicked button to prevent double-click
        paginationBtn.disabled = true;
        paginationBtn.style.opacity = '0.5';
        paginationBtn.style.cursor = 'not-allowed';
        
        const symbol = paginationBtn.getAttribute('data-symbol');
        const interval = paginationBtn.getAttribute('data-interval');
        const page = parseInt(paginationBtn.getAttribute('data-page'));
        const order = paginationBtn.getAttribute('data-order');
        
        // Use setTimeout to ensure the button state is updated before async call
        setTimeout(() => {
            loadSymbolData(symbol, interval, page, order);
        }, 0);
        
        return false;
    }
}, true); // Use capture phase to catch events early

// Event delegation for order select
document.addEventListener('change', function(e) {
    if (e.target.classList.contains('select-pagination-order')) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        
        // Check if already loading
        if (isLoadingSymbolData) {
            return false;
        }
        
        // Immediately disable the select
        e.target.disabled = true;
        
        const symbol = e.target.getAttribute('data-symbol');
        const interval = e.target.getAttribute('data-interval');
        const order = e.target.value;
        
        // Use setTimeout to ensure the select state is updated before async call
        setTimeout(() => {
            loadSymbolData(symbol, interval, 1, order);
        }, 0);
        
        return false;
    }
}, true); // Use capture phase

// Allow Enter key to trigger query
document.addEventListener('DOMContentLoaded', function() {
    const symbolInput = document.getElementById('symbol-query-input');
    if (symbolInput) {
        symbolInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                querySymbolInfo();
            }
        });
    }
});
