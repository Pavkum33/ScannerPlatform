// ==================== Global Variables ====================
let currentResults = [];
let scanInterval = null;
let currentFilter = 'all';
let currentView = 'cards';
let miniCharts = {};

// ==================== Theme Management ====================
function initTheme() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    document.body.setAttribute('data-theme', savedTheme);
    updateThemeIcon(savedTheme);
}

function toggleTheme() {
    const currentTheme = document.body.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    document.body.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
}

function updateThemeIcon(theme) {
    const icon = document.getElementById('themeIcon');
    if (icon) {
        icon.className = theme === 'light' ? 'fas fa-moon' : 'fas fa-sun';
    }
}

// ==================== Sidebar Management ====================
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    sidebar.classList.toggle('open');
}

// ==================== Market Status ====================
function updateMarketStatus() {
    const now = new Date();
    const hours = now.getHours();
    const minutes = now.getMinutes();
    const day = now.getDay();

    const indicator = document.getElementById('marketIndicator');
    const statusText = document.getElementById('marketStatus');
    const timeText = document.getElementById('marketTime');

    // Check if market is open (Mon-Fri, 9:15 AM - 3:30 PM IST)
    const isWeekday = day >= 1 && day <= 5;
    const isMarketHours = (hours === 9 && minutes >= 15) ||
                          (hours > 9 && hours < 15) ||
                          (hours === 15 && minutes <= 30);

    if (isWeekday && isMarketHours) {
        indicator.classList.add('open');
        statusText.textContent = 'Market Open';
    } else {
        indicator.classList.remove('open');
        statusText.textContent = 'Market Closed';
    }

    // Update time
    timeText.textContent = now.toLocaleTimeString('en-IN', {
        hour: '2-digit',
        minute: '2-digit'
    });
}

// ==================== Panel Management ====================
function togglePanel(button) {
    const panel = button.closest('.scanner-panel, .results-panel');
    const body = panel.querySelector('.panel-body');
    const icon = button.querySelector('i');

    if (body.style.display === 'none') {
        body.style.display = 'block';
        icon.className = 'fas fa-chevron-up';
    } else {
        body.style.display = 'none';
        icon.className = 'fas fa-chevron-down';
    }
}

// ==================== Slider Management ====================
function updateSliderValue(targetId, value) {
    document.getElementById(targetId).value = value;
}

// ==================== Symbol Group Management ====================
function onSymbolGroupChange() {
    const symbolGroup = document.getElementById('symbolGroup').value;
    const customSection = document.getElementById('customSymbolsSection');

    if (symbolGroup === 'custom') {
        customSection.style.display = 'block';
    } else {
        customSection.style.display = 'none';
    }
}

// ==================== Scanning Functions ====================
async function startScan() {
    const scanButton = document.getElementById('scanButton');
    const progressSection = document.getElementById('progressSection');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressPercent = document.getElementById('progressPercent');

    // Get parameters
    const params = {
        timeframe: document.getElementById('timeframe').value,
        min_body_move: document.getElementById('minBodyMove').value,
        days_back: document.getElementById('daysBack').value,
        symbol_group: document.getElementById('symbolGroup').value,
        custom_symbols: document.getElementById('customSymbols').value
    };

    // Update button state
    scanButton.disabled = true;
    scanButton.querySelector('.btn-text').textContent = 'Scanning...';
    scanButton.querySelector('.btn-icon').style.display = 'none';
    scanButton.querySelector('.btn-loader').style.display = 'inline-flex';

    // Show progress
    progressSection.style.display = 'block';
    progressBar.style.width = '0%';
    progressPercent.textContent = '0%';
    progressText.textContent = 'Initializing scan...';

    try {
        // Start scan
        const response = await fetch('/api/scan', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(params)
        });

        const data = await response.json();

        if (response.ok) {
            // Start polling for progress
            scanInterval = setInterval(async () => {
                const statusResponse = await fetch('/api/scan/status');
                const status = await statusResponse.json();

                // Update progress
                const progress = status.progress || 0;
                progressBar.style.width = `${progress}%`;
                progressPercent.textContent = `${progress}%`;

                // Enhanced progress text
                let message = status.message || 'Scanning...';
                if (message.includes('Starting scan')) {
                    const dots = '.'.repeat((Math.floor(Date.now() / 500) % 3) + 1);
                    message = message + dots;
                }
                progressText.textContent = message;

                // Check if complete
                if (progress === 100 || status.error) {
                    clearInterval(scanInterval);
                    scanButton.disabled = false;
                    scanButton.querySelector('.btn-text').textContent = 'Start Full Scan';
                    scanButton.querySelector('.btn-icon').style.display = 'inline-flex';
                    scanButton.querySelector('.btn-loader').style.display = 'none';

                    if (status.results) {
                        displayResults(status.results);
                        const patternCount = status.results.results ? status.results.results.length : 0;
                        showToast(`Scan completed! Found ${patternCount} patterns`, 'success');
                    } else if (status.error) {
                        showToast('Scan failed: ' + status.message, 'error');
                    }

                    // Hide progress after animation
                    setTimeout(() => {
                        progressSection.style.display = 'none';
                    }, 2000);
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Failed to start scan');
        }
    } catch (error) {
        console.error('Scan error:', error);
        showToast('Failed to start scan: ' + error.message, 'error');
        scanButton.disabled = false;
        scanButton.querySelector('.btn-text').textContent = 'Start Full Scan';
        scanButton.querySelector('.btn-icon').style.display = 'inline-flex';
        scanButton.querySelector('.btn-loader').style.display = 'none';
        progressSection.style.display = 'none';
    }
}

async function startTodayScan() {
    const progressSection = document.getElementById('progressSection');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressPercent = document.getElementById('progressPercent');

    try {
        // Always run a fresh scan for today's signals to ensure we have latest data
        showToast('Scanning for today\'s signals...', 'info');
        progressSection.style.display = 'block';
        progressBar.style.width = '0%';
        progressText.textContent = 'Scanning for today\'s signals...';

        // Get scan parameters (use daily with recent history)
        const params = {
            timeframe: '1D',
            min_body_move: document.getElementById('minBodyMove').value,
            days_back: 5, // Just need recent data for today's signals
            symbol_group: document.getElementById('symbolGroup').value,
            custom_symbols: document.getElementById('customSymbols').value
        };

        // Start scan
        const response = await fetch('/api/scan', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(params)
        });

        const data = await response.json();
        if (data.message === 'Scan started') {
            // Poll for completion, then filter for today's signals
            scanInterval = setInterval(async () => {
                const statusResponse = await fetch('/api/scan/status');
                const status = await statusResponse.json();

                // Update progress
                const progress = status.progress || 0;
                progressBar.style.width = `${progress}%`;

                let message = status.message || 'Scanning...';
                if (message.includes('Starting scan')) {
                    const dots = '.'.repeat((Math.floor(Date.now() / 500) % 3) + 1);
                    message = message + dots;
                }
                progressText.textContent = message;

                // Check if complete
                if (progress === 100 || status.error) {
                    clearInterval(scanInterval);

                    if (status.results) {
                        // Scan completed, now get today's signals
                        progressText.textContent = 'Filtering for today\'s signals...';

                        const todayResponse = await fetch('/api/results/today');
                        const todayData = await todayResponse.json();

                        if (todayData.results && todayData.results.length > 0) {
                            displayResults(todayData);
                            showToast(`Found ${todayData.results.length} today's signals!`);
                        } else {
                            showToast('No today\'s signals found. ' + todayData.message, 'warning');
                            displayResults({ results: [], statistics: todayData.statistics || {} });
                        }
                    } else if (status.error) {
                        showToast('Scan failed: ' + status.message, 'error');
                    }

                    // Hide progress after 2 seconds
                    setTimeout(() => {
                        progressSection.style.display = 'none';
                    }, 2000);
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Failed to start scan');
        }
    } catch (error) {
        console.error('Today\'s scan error:', error);
        showToast('Failed to scan for today\'s signals: ' + error.message, 'error');
        progressSection.style.display = 'none';
    }
}

// ==================== Display Functions ====================
function displayResults(data) {
    currentResults = data.results || [];
    const statistics = data.statistics || {};

    // Update statistics with animations
    animateNumber('totalScans', statistics.total_symbols_requested || 0);
    animateNumber('symbolsScanned', statistics.symbols_scanned || 0);
    animateNumber('patternsFound', statistics.total_patterns_found || currentResults.length);
    document.getElementById('scanTime').textContent = `${statistics.scan_duration_seconds || 0}s`;

    // Update pattern count
    document.getElementById('patternCount').textContent = currentResults.length;

    // Update badges
    updatePatternBadges(currentResults);

    // Debug log
    console.log('Display Results - Total patterns:', currentResults.length);

    // Update progress bar for symbols scanned
    const progressBar = document.querySelector('.progress-fill');
    if (progressBar && statistics.total_symbols_requested > 0) {
        const percentage = (statistics.symbols_scanned / statistics.total_symbols_requested) * 100;
        progressBar.style.width = `${percentage}%`;
    }

    // Display filtered results
    filterResults(currentFilter);

    // Initialize mini charts
    setTimeout(() => initializeMiniCharts(), 500);
}

function animateNumber(elementId, targetValue) {
    const element = document.getElementById(elementId);
    const startValue = parseInt(element.textContent) || 0;
    const duration = 1000;
    const startTime = Date.now();

    function update() {
        const currentTime = Date.now();
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);

        const currentValue = Math.floor(startValue + (targetValue - startValue) * progress);
        element.textContent = currentValue;

        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }

    requestAnimationFrame(update);
}

function updatePatternBadges(results) {
    const bullishCount = results.filter(r => r.pattern_direction?.toLowerCase() === 'bullish').length;
    const bearishCount = results.filter(r => r.pattern_direction?.toLowerCase() === 'bearish').length;

    const badges = document.querySelector('.stat-badges');
    if (badges) {
        badges.innerHTML = `
            <span class="mini-badge bullish">${bullishCount} Bull</span>
            <span class="mini-badge bearish">${bearishCount} Bear</span>
        `;
    }
}

function filterResults(filter) {
    currentFilter = filter;
    let filtered = [...currentResults];

    // Apply filter (case-insensitive comparison)
    if (filter === 'bullish') {
        filtered = filtered.filter(r => r.pattern_direction && r.pattern_direction.toLowerCase() === 'bullish');
    } else if (filter === 'bearish') {
        filtered = filtered.filter(r => r.pattern_direction && r.pattern_direction.toLowerCase() === 'bearish');
    } else if (filter === 'today') {
        const today = new Date().toISOString().split('T')[0];
        filtered = filtered.filter(r => {
            const patternDate = r.doji?.date || '';
            return patternDate === today;
        });
    }

    console.log(`Filtering for: ${filter}, Results: ${filtered.length}/${currentResults.length}`);

    // Update filter buttons
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
        // Check if this button's onclick matches the current filter
        const onclickAttr = btn.getAttribute('onclick');
        if (onclickAttr && onclickAttr.includes(`'${filter}'`)) {
            btn.classList.add('active');
        }
    });

    // Render results
    renderResults(filtered);
}

function renderResults(results) {
    const container = document.getElementById('resultsContainer');

    if (results.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">
                    <i class="fas fa-chart-candlestick"></i>
                </div>
                <h3 class="empty-title">No Patterns Found</h3>
                <p class="empty-text">Try adjusting your filters or scan parameters</p>
            </div>
        `;
        return;
    }

    if (currentView === 'table') {
        renderTableView(results);
    } else {
        renderCardView(results);
    }
}

function renderCardView(results) {
    const container = document.getElementById('resultsContainer');

    container.innerHTML = results.map((result, index) => `
        <div class="result-card ${result.pattern_direction}" style="animation-delay: ${index * 0.05}s">
            <div class="result-header">
                <div class="result-symbol">
                    <a href="${getTradingViewUrl(result.symbol)}"
                       target="_blank"
                       class="chart-link"
                       title="Open ${result.symbol} chart in TradingView">
                        <i class="fas fa-chart-line chart-icon"></i>
                        <h3>${result.symbol}</h3>
                        <i class="fas fa-external-link-alt external-icon"></i>
                    </a>
                </div>
                <div>
                    <span class="direction-badge ${result.pattern_direction}">
                        ${result.pattern_direction.toUpperCase()}
                    </span>
                    ${result.timeframe ? `<span class="timeframe-badge">${result.timeframe}</span>` : ''}
                </div>
            </div>
            <div class="result-details">
                <div class="detail-group">
                    <div class="detail-label">Signal Date</div>
                    <div class="detail-value">${formatDateWithWeek(result)}</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Pattern Span</div>
                    <div class="detail-value">${formatPatternSpan(result)}</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Confidence</div>
                    <div class="detail-value">${calculateConfidence(result)}%</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Marubozu Body</div>
                    <div class="detail-value">${(result.marubozu?.body_move_pct || result.marubozu?.body_move_pct)?.toFixed(2) || 'N/A'}%</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Doji Body</div>
                    <div class="detail-value">${(result.doji?.body_pct_of_range || result.doji?.body_pct)?.toFixed(2) || 'N/A'}%</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Breakout</div>
                    <div class="detail-value">${result.doji?.high > result.marubozu?.high ? '✓ Yes' : '✗ No'}</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Close Price</div>
                    <div class="detail-value">₹${result.doji?.close?.toFixed(2) || 'N/A'}</div>
                </div>
                <div class="detail-group">
                    <div class="detail-label">Volume</div>
                    <div class="detail-value">${formatVolume(result.doji?.volume)}</div>
                </div>
            </div>
            <div class="chart-actions">
                <button onclick="openTradingView('${result.symbol}')" class="btn-chart">
                    <i class="fab fa-tradingview"></i> Open Chart
                </button>
                <button onclick="openTradingViewAdvanced('${result.symbol}', '${result.timeframe}')" class="btn-chart-advanced">
                    <i class="fas fa-chart-area"></i> Advanced Chart
                </button>
            </div>
        </div>
    `).join('');
}

function renderTableView(results) {
    const container = document.getElementById('resultsContainer');

    container.innerHTML = `
        <table class="results-table">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Direction</th>
                    <th>Signal Date</th>
                    <th>Confidence</th>
                    <th>Close Price</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                ${results.map(result => `
                    <tr>
                        <td><strong>${result.symbol}</strong></td>
                        <td>
                            <span class="direction-badge ${result.pattern_direction}">
                                ${result.pattern_direction.toUpperCase()}
                            </span>
                        </td>
                        <td>${formatDateWithWeek(result)}</td>
                        <td>${calculateConfidence(result)}%</td>
                        <td>₹${result.doji?.close?.toFixed(2) || 'N/A'}</td>
                        <td>
                            <button onclick="openTradingView('${result.symbol}')" class="btn-chart-mini">
                                <i class="fas fa-chart-line"></i>
                            </button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function setView(view) {
    currentView = view;

    // Update view buttons
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.closest('.view-btn').classList.add('active');

    // Re-render results
    renderResults(currentResults.filter(r => {
        if (currentFilter === 'all') return true;
        if (currentFilter === 'bullish') return r.pattern_direction?.toLowerCase() === 'bullish';
        if (currentFilter === 'bearish') return r.pattern_direction?.toLowerCase() === 'bearish';
        if (currentFilter === 'today') {
            const today = new Date().toISOString().split('T')[0];
            return (r.doji?.date || '') === today;
        }
        return true;
    }));
}

function formatVolume(volume) {
    if (!volume) return 'N/A';
    if (volume > 1000000) return `${(volume / 1000000).toFixed(2)}M`;
    if (volume > 1000) return `${(volume / 1000).toFixed(2)}K`;
    return volume.toString();
}

// ==================== Utility Functions ====================
function calculateConfidence(result) {
    // Calculate confidence based on pattern strength
    const marubozu_strength = result.marubozu?.body_pct_of_range || 0;
    const doji_strength = 100 - (result.doji?.body_pct_of_range || 0);
    const rejection = result.rejection_strength || 0;

    const confidence = ((marubozu_strength + doji_strength + rejection) / 3);
    return Math.min(95, Math.max(50, confidence)).toFixed(0);
}

function formatDate(dateStr) {
    if (!dateStr) return 'N/A';
    const date = new Date(dateStr);
    const options = { year: 'numeric', month: 'short', day: 'numeric' };
    return date.toLocaleDateString('en-US', options);
}

function formatDateWithWeek(result) {
    // Handle weekly/monthly patterns with period_start/period_end
    if (result.doji?.period_end) {
        const date = new Date(result.doji.period_end);
        const options = { year: 'numeric', month: 'short', day: 'numeric' };
        const formattedDate = date.toLocaleDateString('en-US', options);

        if (result.timeframe === 'WEEKLY') {
            return `Week ending ${formattedDate}`;
        } else if (result.timeframe === 'MONTHLY') {
            return `Month ending ${formattedDate}`;
        }
        return formattedDate;
    }

    // Handle daily patterns
    if (!result.doji?.date) return 'N/A';
    const date = new Date(result.doji.date);
    const options = { year: 'numeric', month: 'short', day: 'numeric' };
    const formattedDate = date.toLocaleDateString('en-US', options);

    // Add week info if available (for weekly patterns)
    if (result.timeframe === '1W' && result.doji?.week_info) {
        return `${formattedDate} ${result.doji.week_info}`;
    }
    return formattedDate;
}

function formatPatternSpan(result) {
    // Handle weekly/monthly patterns with period dates
    if (result.marubozu?.period_end && result.doji?.period_end) {
        const maruDate = new Date(result.marubozu.period_end);
        const dojiDate = new Date(result.doji.period_end);

        const options = { month: 'short', day: 'numeric' };
        const maruFormatted = maruDate.toLocaleDateString('en-US', options);
        const dojiFormatted = dojiDate.toLocaleDateString('en-US', options);

        if (result.timeframe === 'WEEKLY') {
            return `Week ${maruFormatted} → Week ${dojiFormatted}`;
        } else if (result.timeframe === 'MONTHLY') {
            const monthOptions = { month: 'short', year: 'numeric' };
            const maruMonth = maruDate.toLocaleDateString('en-US', monthOptions);
            const dojiMonth = dojiDate.toLocaleDateString('en-US', monthOptions);
            return `${maruMonth} → ${dojiMonth}`;
        }
        return `${maruFormatted} → ${dojiFormatted}`;
    }

    // Handle daily patterns
    if (!result.marubozu?.date || !result.doji?.date) return 'N/A';

    const maruDate = new Date(result.marubozu.date);
    const dojiDate = new Date(result.doji.date);

    // For weekly patterns, show week numbers
    if (result.timeframe === '1W') {
        const maruWeek = result.marubozu?.week_info || '';
        const dojiWeek = result.doji?.week_info || '';

        if (maruWeek && dojiWeek) {
            return `${maruWeek} → ${dojiWeek}`;
        }
    }

    // For daily patterns, just show date range
    const options = { month: 'short', day: 'numeric' };
    const maruFormatted = maruDate.toLocaleDateString('en-US', options);
    const dojiFormatted = dojiDate.toLocaleDateString('en-US', options);

    return `${maruFormatted} → ${dojiFormatted}`;
}

async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        const stats = await response.json();

        // Update connection status
        const statusDot = document.querySelector('.connection-dot');
        if (statusDot) {
            if (stats.scanner_status === 'online') {
                statusDot.classList.add('online');
            } else {
                statusDot.classList.remove('online');
            }
        }
    } catch (error) {
        console.error('Failed to load stats:', error);
    }
}

async function loadScanners() {
    try {
        const response = await fetch('/api/scanners');
        const scanners = await response.json();

        // Update scanner list if needed
        // This is for future multi-scanner support
    } catch (error) {
        console.error('Failed to load scanners:', error);
    }
}

async function checkLatestResults() {
    try {
        const response = await fetch('/api/results/latest');
        const data = await response.json();

        if (data.results && data.results.length > 0) {
            displayResults(data);
        }
    } catch (error) {
        console.error('Failed to load latest results:', error);
    }
}

async function runEODUpdate() {
    if (!confirm('Run EOD update to fetch latest market data? This will take about 2 minutes.')) {
        return;
    }

    showToast('Starting EOD update...', 'info');

    try {
        const response = await fetch('/api/eod-update', {
            method: 'POST'
        });

        if (!response.ok) {
            throw new Error('Failed to start EOD update');
        }

        // Poll for status
        const checkStatus = setInterval(async () => {
            const statusResp = await fetch('/api/eod-update/status');
            const status = await statusResp.json();

            if (status.status === 'completed') {
                clearInterval(checkStatus);
                showToast(`EOD Update Complete! Updated ${status.symbols_updated} symbols with ${status.new_records} new records.`, 'success');
                // Optionally refresh current view
                if (currentResults.length > 0) {
                    refreshData();
                }
            } else if (status.status === 'error') {
                clearInterval(checkStatus);
                showToast('EOD Update failed: ' + status.message, 'error');
            } else if (status.status === 'running') {
                showToast(`Updating... ${status.progress}% complete`, 'info');
            }
        }, 2000);

    } catch (error) {
        showToast('Failed to start EOD update: ' + error.message, 'error');
    }
}

async function loadDatabasePatterns() {
    showToast('Loading patterns from database...');

    try {
        const response = await fetch('/api/results/database');
        const data = await response.json();

        if (data.results) {
            // Combine all timeframe results
            const allPatterns = [
                ...data.results['1D'] || [],
                ...data.results['WEEKLY'] || [],
                ...data.results['MONTHLY'] || []
            ];

            // Display combined results
            displayResults({
                results: allPatterns,
                statistics: data.statistics
            });

            showToast(`Loaded ${data.statistics.daily_patterns} daily, ${data.statistics.weekly_patterns} weekly, ${data.statistics.monthly_patterns} monthly patterns!`);
        } else {
            showToast('No patterns found in database', 'warning');
        }
    } catch (error) {
        console.error('Failed to load database patterns:', error);
        showToast('Failed to load database patterns', 'error');
    }
}

async function refreshData() {
    showToast('Refreshing data...');
    await loadStats();
    await checkLatestResults();
    showToast('Data refreshed successfully!');
}

async function exportResults(format) {
    try {
        showToast(`Exporting as ${format.toUpperCase()}...`);

        const response = await fetch(`/api/export/${format}`);
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `scan_results.${format}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);

            showToast(`Export completed!`);
        } else {
            throw new Error('Export failed');
        }
    } catch (error) {
        console.error('Export error:', error);
        showToast('Failed to export results', 'error');
    }
}

// ==================== Toast Notification ====================
function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    const toastMessage = document.getElementById('toastMessage');
    const toastTitle = toast.querySelector('.toast-title');
    const toastIcon = toast.querySelector('.toast-icon i');

    toastMessage.textContent = message;

    // Update based on type
    switch(type) {
        case 'error':
            toastTitle.textContent = 'Error';
            toastIcon.className = 'fas fa-exclamation-circle';
            toastIcon.style.color = 'var(--danger)';
            break;
        case 'warning':
            toastTitle.textContent = 'Warning';
            toastIcon.className = 'fas fa-exclamation-triangle';
            toastIcon.style.color = 'var(--warning)';
            break;
        case 'info':
            toastTitle.textContent = 'Info';
            toastIcon.className = 'fas fa-info-circle';
            toastIcon.style.color = 'var(--info)';
            break;
        default:
            toastTitle.textContent = 'Success';
            toastIcon.className = 'fas fa-check-circle';
            toastIcon.style.color = 'var(--success)';
    }

    // Show toast
    toast.classList.add('show');

    // Hide after 3 seconds
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

function hideToast() {
    const toast = document.getElementById('toast');
    toast.classList.remove('show');
}

// ==================== TradingView Integration ====================
function getTradingViewUrl(symbol) {
    // Convert NSE symbol format for TradingView
    const tvSymbol = `NSE:${symbol}`;
    return `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol)}`;
}

function openTradingView(symbol) {
    const url = getTradingViewUrl(symbol);
    window.open(url, '_blank', 'noopener,noreferrer');
}

function openTradingViewAdvanced(symbol, timeframe) {
    // Map our timeframes to TradingView intervals
    const intervalMap = {
        '1D': 'D',  // Daily
        '1W': 'W',  // Weekly
        '1M': 'M'   // Monthly
    };

    const tvSymbol = `NSE:${symbol}`;
    const interval = intervalMap[timeframe] || 'D';

    // Advanced chart with specific interval and studies
    const url = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol)}&interval=${interval}&studies=BB@tv-basicstudies%1FVolume@tv-basicstudies`;

    window.open(url, '_blank', 'noopener,noreferrer');
}

// Update history unit when timeframe changes
function updateHistoryUnit() {
    const timeframe = document.getElementById('timeframe').value;
    const historyUnit = document.getElementById('historyUnit');
    const historyHelp = document.getElementById('historyHelp');
    const daysBack = document.getElementById('daysBack');

    if (timeframe === '1D') {
        historyUnit.textContent = 'days';
        historyHelp.textContent = 'Number of trading days to analyze';
        daysBack.value = '30';
    } else if (timeframe === '1W') {
        historyUnit.textContent = 'weeks';
        historyHelp.textContent = 'Number of weeks to analyze (fetches ~7 days per week)';
        daysBack.value = '60';
    } else if (timeframe === '1M') {
        historyUnit.textContent = 'months';
        historyHelp.textContent = 'Number of months to analyze (fetches ~30 days per month)';
        daysBack.value = '12';
    }
}

// ==================== Mini Charts ====================
function initializeMiniCharts() {
    // Initialize mini sparkline charts for statistics
    const chartOptions = {
        chart: {
            type: 'area',
            height: 40,
            sparkline: {
                enabled: true
            },
            animations: {
                enabled: true,
                easing: 'easeinout',
                speed: 800
            }
        },
        stroke: {
            curve: 'smooth',
            width: 2
        },
        fill: {
            opacity: 0.3
        },
        tooltip: {
            enabled: false
        }
    };

    // Scans chart
    if (document.querySelector('#scansChart') && !miniCharts.scans) {
        miniCharts.scans = new ApexCharts(document.querySelector('#scansChart'), {
            ...chartOptions,
            series: [{
                data: generateRandomData(8, 50, 100)
            }],
            colors: ['#667eea']
        });
        miniCharts.scans.render();
    }

    // Symbols chart
    if (document.querySelector('#symbolsChart') && !miniCharts.symbols) {
        miniCharts.symbols = new ApexCharts(document.querySelector('#symbolsChart'), {
            ...chartOptions,
            series: [{
                data: generateRandomData(8, 100, 158)
            }],
            colors: ['#f093fb']
        });
        miniCharts.symbols.render();
    }

    // Patterns chart
    if (document.querySelector('#patternsChart') && !miniCharts.patterns) {
        miniCharts.patterns = new ApexCharts(document.querySelector('#patternsChart'), {
            ...chartOptions,
            series: [{
                data: generateRandomData(8, 0, 50)
            }],
            colors: ['#4facfe']
        });
        miniCharts.patterns.render();
    }

    // Time chart
    if (document.querySelector('#timeChart') && !miniCharts.time) {
        miniCharts.time = new ApexCharts(document.querySelector('#timeChart'), {
            ...chartOptions,
            series: [{
                data: generateRandomData(8, 5, 30)
            }],
            colors: ['#43e97b']
        });
        miniCharts.time.render();
    }
}

function generateRandomData(count, min, max) {
    const data = [];
    for (let i = 0; i < count; i++) {
        data.push(Math.floor(Math.random() * (max - min + 1)) + min);
    }
    return data;
}

// ==================== Initialization ====================
document.addEventListener('DOMContentLoaded', function() {
    // Initialize theme
    initTheme();

    // Initialize market status
    updateMarketStatus();
    setInterval(updateMarketStatus, 60000); // Update every minute

    // Load initial data
    loadStats();
    loadScanners();
    updateHistoryUnit();

    // Add event listeners
    document.getElementById('timeframe').addEventListener('change', updateHistoryUnit);

    // Initialize tooltips
    initializeTooltips();

    // Initialize notifications
    checkNotifications();
});

function initializeTooltips() {
    // Add tooltip functionality if needed
}

function checkNotifications() {
    // Check for new notifications
    const count = 0; // Would fetch from API
    const badge = document.getElementById('notificationCount');
    if (badge) {
        badge.textContent = count;
        badge.style.display = count > 0 ? 'block' : 'none';
    }
}

// ==================== Real-time Updates ====================
// Poll for updates every 30 seconds when idle
setInterval(() => {
    if (!scanInterval) {
        loadStats();
        checkNotifications();
    }
}, 30000);