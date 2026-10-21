/**
 * QueueWise — Page Controllers Module
 * Binds API responses to UI templates, handles interactions, and orchestrates chart renders.
 */

const QueueWisePages = (() => {

  // Helper to safely format numbers
  function fmt(val, decimals = 1, suffix = '') {
    if (val === null || val === undefined || isNaN(val)) return '—';
    return Number(val).toFixed(decimals) + suffix;
  }

  // Helper to show/hide loading spinners
  function showLoading(containerId) {
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = `
      <div class="loading-wrapper">
        <div class="spinner"></div>
        <span>Loading analytical intelligence...</span>
      </div>
    `;
  }

  function showError(containerId, err) {
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = `
      <div class="alert-card alert-critical" style="margin: 20px 0;">
        <strong>Failed to load data:</strong>
        <p>${err.message || 'An unexpected error occurred while communicating with the backend.'}</p>
      </div>
    `;
  }

  return {
    // =========================================================================
    // 1. OVERVIEW PAGE CONTROLLER
    // =========================================================================
    async initOverviewPage() {
      showLoading('overview-content');

      try {
        const data = await QueueWiseAPI.fetchOverview();
        const kpis = data.kpis;

        const html = `
          <!-- KPI Cards Grid -->
          <div class="kpi-grid">
            <div class="kpi-card">
              <div class="kpi-title">Total Customers Served</div>
              <div class="kpi-number">${kpis.total_customers.toLocaleString()}</div>
              <div class="kpi-detail">Discrete service lifecycles across 4 departments</div>
            </div>

            <div class="kpi-card ${kpis.weighted_avg_wait > 30 ? 'kpi-accent-amber' : ''}">
              <div class="kpi-title">Average Waiting Time</div>
              <div class="kpi-number" style="color: ${kpis.weighted_avg_wait > 30 ? '#D97706' : '#0F172A'};">
                ${kpis.weighted_avg_wait.toFixed(1)}m
              </div>
              <div class="kpi-detail">Facility-wide customer arrival-to-start delay</div>
            </div>

            <div class="kpi-card ${kpis.breach_rate_30m > 50 ? 'kpi-accent-red' : ''}">
              <div class="kpi-title">30-Min SLA Breach Rate</div>
              <div class="kpi-number" style="color: ${kpis.breach_rate_30m > 50 ? '#DC2626' : '#0F172A'};">
                ${kpis.breach_rate_30m.toFixed(1)}%
              </div>
              <div class="kpi-detail">${kpis.tot_breached_30m} of ${kpis.tot_served} customers exceeded 30m target</div>
            </div>

            <div class="kpi-card kpi-accent-red">
              <div class="kpi-title">Primary Bottleneck</div>
              <div class="kpi-number" style="font-size: 1.28rem; color: #DC2626;">
                ${kpis.bottleneck_name.split('&')[0].trim()}
              </div>
              <div class="kpi-detail">${kpis.bottleneck_wait.toFixed(1)}m avg wait time (critical saturation)</div>
            </div>
          </div>

          <!-- Charts Row -->
          <div class="layout-2col">
            <div class="content-box">
              <div class="content-header">
                <span>Waiting-Time Benchmark by Queue</span>
                <span class="provenance-badge">SLA Target &le; 30m</span>
              </div>
              <div id="chart-benchmark" class="chart-container"></div>
            </div>

            <div class="content-box">
              <div class="content-header">
                <span>Customer Arrival Pattern</span>
                <span class="provenance-badge">Hourly Distribution</span>
              </div>
              <div id="chart-arrivals" class="chart-container"></div>
            </div>
          </div>

          <!-- Operational Alerts & Capacity Optimization Link -->
          <div class="content-box">
            <div class="content-header">
              <span>Operational Bottleneck & Capacity Insights</span>
              <a href="/capacity-planning" class="btn btn-primary" style="font-size: 0.78rem; padding: 4px 10px;">
                Review Capacity Optimization Plan &rarr;
              </a>
            </div>
            <div class="alert-grid">
              ${data.alerts.map(a => `
                <div class="alert-card alert-${a.type}">
                  <strong>${a.title}</strong>
                  ${a.message}
                </div>
              `).join('')}
            </div>
          </div>
        `;

        document.getElementById('overview-content').innerHTML = html;

        // Render interactive Plotly charts
        QueueWiseCharts.renderBenchmarkChart('chart-benchmark', data.benchmark);
        QueueWiseCharts.renderHourlyArrivalsChart('chart-arrivals', data.hourly_arrivals);

      } catch (err) {
        showError('overview-content', err);
      }
    },

    // =========================================================================
    // 2. QUEUE PERFORMANCE PAGE CONTROLLER
    // =========================================================================
    async initQueuePerformancePage() {
      const container = document.getElementById('queue-performance-content');
      showLoading('queue-performance-content');

      try {
        const queuesList = await QueueWiseAPI.fetchQueues();
        let selectedQueues = queuesList.map(q => q.queue_id);
        let thresholdMinutes = 30;

        async function updateView() {
          if (selectedQueues.length === 0) {
            document.getElementById('perf-dynamic-area').innerHTML = `
              <div class="content-box empty-state">
                <p><strong>No queues selected.</strong></p>
                <p>Please select at least one department queue above to inspect performance metrics.</p>
              </div>
            `;
            return;
          }

          try {
            const perfData = await QueueWiseAPI.fetchQueuePerformance(selectedQueues, thresholdMinutes);
            renderPerformanceResults(perfData);
          } catch (err) {
            document.getElementById('perf-dynamic-area').innerHTML = `
              <div class="alert-card alert-critical">${err.message}</div>
            `;
          }
        }

        function renderPerformanceResults(data) {
          const area = document.getElementById('perf-dynamic-area');
          if (!area) return;

          const stats = data.threshold_stats;

          area.innerHTML = `
            <!-- Threshold breach callout banner -->
            <div style="background-color: #F8FAFC; border: 1px solid var(--border-card); border-left: 4px solid var(--primary); border-radius: var(--radius-md); padding: 10px 14px; margin-bottom: 16px; font-size: 0.83rem; color: #334155;">
              <strong>Wait Time Threshold Benchmark (${thresholdMinutes} min):</strong> ${stats.info_text}
            </div>

            <!-- Charts Row -->
            <div class="layout-2col">
              <div class="content-box">
                <div class="content-header">
                  <span>Waiting Time vs. Service Duration</span>
                  <span class="provenance-badge">Filtered Queues</span>
                </div>
                <div id="chart-perf-comp" class="chart-container"></div>
              </div>

              <div class="content-box">
                <div class="content-header">
                  <span>Wait Time Cumulative Distribution (CDF)</span>
                  <span class="provenance-badge">${thresholdMinutes}m Threshold</span>
                </div>
                <div id="chart-cdf" class="chart-container"></div>
              </div>
            </div>

            <!-- Turnaround Metrics Table -->
            <div class="content-box">
              <div class="content-header">
                <span>Detailed Turnaround & Wait Time Metrics</span>
                <span class="provenance-badge">${data.metrics.length} Queues</span>
              </div>
              <div class="table-responsive">
                <table class="data-table">
                  <thead>
                    <tr>
                      <th>Queue ID</th>
                      <th>Department Service</th>
                      <th class="text-right">Customers</th>
                      <th class="text-right">Avg Wait</th>
                      <th class="text-right">Median Wait</th>
                      <th class="text-right">Max Wait</th>
                      <th class="text-right">Avg Service</th>
                      <th class="text-right">Turnaround</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${data.metrics.map(m => `
                      <tr>
                        <td><code>${m.queue_id}</code></td>
                        <td><strong>${m.service_name}</strong></td>
                        <td class="text-right">${m.customers.toLocaleString()}</td>
                        <td class="text-right">${m.avg_wait_minutes.toFixed(1)} min</td>
                        <td class="text-right">${m.median_wait_minutes.toFixed(1)} min</td>
                        <td class="text-right">${m.max_wait_minutes.toFixed(1)} min</td>
                        <td class="text-right">${m.avg_service_minutes.toFixed(1)} min</td>
                        <td class="text-right">${m.avg_turnaround_minutes.toFixed(1)} min</td>
                      </tr>
                    `).join('')}
                  </tbody>
                </table>
              </div>
            </div>

            <!-- ML Risk Cards -->
            <div class="content-box">
              <div class="content-header">
                <span>Predictive Long-Wait SLA Breach Risk (&gt;30 Minutes)</span>
                <span class="provenance-badge">Supervised Spark MLlib</span>
              </div>
              <div style="font-size: 0.82rem; color: var(--text-muted); margin-bottom: 12px;">
                Probabilities predicted from customer arrival features using trained Spark MLlib models:
              </div>
              <div class="kpi-grid">
                ${data.ml_risk.map(r => `
                  <div class="kpi-card" style="border-left: 4px solid ${r.color};">
                    <div class="kpi-title">${r.service_name.split('&')[0].trim()}</div>
                    <div class="kpi-number" style="color: ${r.color};">${r.predicted_risk_pct.toFixed(1)}%</div>
                    <div class="kpi-detail">
                      <span class="pill pill-blue">Predicted Risk</span> &gt;30m Breach Probability
                    </div>
                  </div>
                `).join('')}
              </div>

              <!-- ML Holdout Metrics Table -->
              ${data.ml_metrics.length > 0 ? `
                <div style="margin-top: 20px;">
                  <h4 style="font-size: 0.9rem; font-weight: 600; color: #1E293B; margin-bottom: 8px;">
                    Predictive Model Evaluation Comparison (Holdout Test Split)
                  </h4>
                  <div class="table-responsive">
                    <table class="data-table">
                      <thead>
                        <tr>
                          <th>Model</th>
                          <th class="text-right">Accuracy (%)</th>
                          <th class="text-right">F1 Score</th>
                          <th class="text-right">Precision</th>
                          <th class="text-right">Recall</th>
                          <th class="text-right">AUC-ROC</th>
                          <th class="text-right">AUC-PR</th>
                          <th class="text-right">TP</th>
                          <th class="text-right">FP</th>
                          <th class="text-right">TN</th>
                          <th class="text-right">FN</th>
                          <th class="text-right">Test Samples</th>
                        </tr>
                      </thead>
                      <tbody>
                        ${data.ml_metrics.map(mm => `
                          <tr>
                            <td><strong>${mm.model_name}</strong></td>
                            <td class="text-right">${mm.accuracy_pct.toFixed(3)}%</td>
                            <td class="text-right">${mm.f1_score.toFixed(3)}</td>
                            <td class="text-right">${mm.precision.toFixed(3)}</td>
                            <td class="text-right">${mm.recall.toFixed(3)}</td>
                            <td class="text-right">${mm.auc_roc.toFixed(3)}</td>
                            <td class="text-right">${mm.auc_pr.toFixed(3)}</td>
                            <td class="text-right">${mm.tp}</td>
                            <td class="text-right">${mm.fp}</td>
                            <td class="text-right">${mm.tn}</td>
                            <td class="text-right">${mm.fn}</td>
                            <td class="text-right">${mm.test_sample_count}</td>
                          </tr>
                        `).join('')}
                      </tbody>
                    </table>
                  </div>
                  <div style="background-color: #F8FAFC; border: 1px solid var(--border-card); border-radius: var(--radius-sm); padding: 8px 12px; font-size: 0.78rem; color: #475569; margin-top: 8px;">
                    <strong>Model Evaluation &amp; Synthetic Data Disclosure:</strong> The high AUC (~0.999&ndash;1.000) is an expected mathematical property of the synthetic discrete-event M/M/c queuing mechanics across 500 customers in a single simulated operating day. In this dataset, Campus ID has 0 breaches while Financial Aid remains persistently saturated after 10:42 AM, creating clean decision boundaries.
                  </div>
                </div>
              ` : ''}
            </div>
          `;

          QueueWiseCharts.renderPerformanceComparisonChart('chart-perf-comp', data.metrics);
          QueueWiseCharts.renderWaitDistributionChart('chart-cdf', data.wait_distribution, thresholdMinutes);
        }

        // Render Base Controls Shell
        container.innerHTML = `
          <div class="filter-panel">
            <div class="filter-group">
              <span class="filter-label">Filter Department Queues</span>
              <div class="checkbox-pills">
                ${queuesList.map(q => `
                  <label class="checkbox-pill-item">
                    <input type="checkbox" value="${q.queue_id}" checked class="q-filter-chk">
                    <span>${q.name}</span>
                  </label>
                `).join('')}
              </div>
            </div>

            <div class="filter-group" style="margin-left: auto;">
              <span class="filter-label">Wait Time Benchmark Threshold</span>
              <div class="range-control">
                <input type="range" min="10" max="120" step="5" value="30" class="range-slider" id="threshold-range">
                <span class="range-val-badge" id="threshold-badge">30m</span>
              </div>
            </div>
          </div>

          <div id="perf-dynamic-area"></div>
        `;

        // Bind interactive events
        const checkboxes = container.querySelectorAll('.q-filter-chk');
        checkboxes.forEach(chk => {
          chk.addEventListener('change', () => {
            selectedQueues = Array.from(checkboxes).filter(c => c.checked).map(c => c.value);
            updateView();
          });
        });

        const rangeSlider = document.getElementById('threshold-range');
        const rangeBadge = document.getElementById('threshold-badge');
        rangeSlider.addEventListener('input', (e) => {
          thresholdMinutes = parseInt(e.target.value, 10);
          rangeBadge.textContent = `${thresholdMinutes}m`;
        });
        rangeSlider.addEventListener('change', () => {
          updateView();
        });

        // Initial Load
        updateView();

      } catch (err) {
        showError('queue-performance-content', err);
      }
    },

    // =========================================================================
    // 3. COUNTER UTILIZATION PAGE CONTROLLER
    // =========================================================================
    async initCounterUtilizationPage() {
      showLoading('counter-utilization-content');

      try {
        const data = await QueueWiseAPI.fetchCounterUtilization();

        const html = `
          <div class="content-box">
            <div class="content-header">Counter Workload & Capacity Utilization</div>
            <div style="font-size: 0.84rem; color: #475569; line-height: 1.45;">
              Counter utilization measures the percentage of the operating window (<strong>${data.operating_window_seconds.toLocaleString()} seconds / ${data.operating_window_hours} hours</strong>) during which a counter was actively serving customers. 
              Customer waiting time in queue lines is strictly excluded from counter utilization calculations.
            </div>
          </div>

          <div class="layout-2col">
            <div class="content-box">
              <div class="content-header">
                <span>Active Service Utilization by Physical Counter</span>
                <span class="provenance-badge">8 Counters</span>
              </div>
              <div id="chart-counters" class="chart-container"></div>
            </div>

            <div class="content-box">
              <div class="content-header">Workload Distribution Analysis</div>
              <div style="font-size: 0.83rem; color: #334155; line-height: 1.45;">
                <p><strong>Severe Imbalance Across Department Counters:</strong></p>
                <ul style="padding-left: 18px; margin: 4px 0 10px 0;">
                  <li><strong>COUNTER_03 &amp; COUNTER_04 (Financial Aid):</strong> Critical saturation at <strong>98.9% and 96.0%</strong> utilization (averaging 97.4%). Zero idle buffer.</li>
                  <li><strong>COUNTER_01 &amp; COUNTER_02 (Admissions):</strong> Operating at <strong>83.2% and 83.8%</strong>, sustaining heavy volume.</li>
                  <li><strong>COUNTER_05 &amp; COUNTER_06 (Records):</strong> Healthy balanced utilization at <strong>63.5% and 62.6%</strong>.</li>
                  <li><strong>COUNTER_07 &amp; COUNTER_08 (ID General):</strong> Substantially underutilized at <strong>31.8% and 28.8%</strong> (averaging 30.3%).</li>
                </ul>
                <div style="background-color: #FEF3C7; border-left: 3px solid #D97706; padding: 8px 10px; font-size: 0.77rem; color: #92400E; border-radius: 4px;">
                  <strong>Capacity Opportunity:</strong> ${data.analysis.reallocation_opportunity}
                </div>
              </div>
            </div>
          </div>

          <!-- Counter Workload Table -->
          <div class="content-box">
            <div class="content-header">
              <span>Physical Counter Workload Table</span>
              <span class="provenance-badge">Baseline Allocation</span>
            </div>
            <div class="table-responsive">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Counter ID</th>
                    <th>Queue ID</th>
                    <th>Department Service</th>
                    <th class="text-right">Customers Served</th>
                    <th class="text-right">Active Busy Time</th>
                    <th class="text-right">Avg Service Duration</th>
                    <th class="text-right">Baseline Utilization</th>
                  </tr>
                </thead>
                <tbody>
                  ${data.counters.map(c => `
                    <tr>
                      <td><code>${c.counter_id}</code></td>
                      <td><code>${c.queue_id}</code></td>
                      <td><strong>${c.service_name}</strong></td>
                      <td class="text-right">${c.customers_served.toLocaleString()}</td>
                      <td class="text-right">${c.total_busy_hours.toFixed(2)} h</td>
                      <td class="text-right">${c.avg_service_minutes.toFixed(1)} m</td>
                      <td class="text-right"><strong>${c.utilization_pct.toFixed(1)}%</strong></td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
        `;

        document.getElementById('counter-utilization-content').innerHTML = html;
        QueueWiseCharts.renderCounterUtilizationChart('chart-counters', data);

      } catch (err) {
        showError('counter-utilization-content', err);
      }
    },

    // =========================================================================
    // 4. CAPACITY PLANNING PAGE CONTROLLER
    // =========================================================================
    async initCapacityPlanningPage() {
      showLoading('capacity-planning-content');

      try {
        const data = await QueueWiseAPI.fetchCapacityScenario();
        const o = data.outcomes;

        const html = `
          <div class="content-box">
            <div class="content-header">Optimize Service Capacity and Reduce Customer Waiting Times</div>
            <div style="font-size: 0.84rem; color: #334155; line-height: 1.45;">
              <strong>Scenario:</strong> Reallocate <strong>COUNTER_07</strong> from
              <strong>Campus ID &amp; General Help</strong> (underutilized at 30.3% baseline) to <strong>Financial Aid &amp; Scholarships</strong> (saturated at 97.4% baseline).
              <br>
              <span style="color: #64748B; font-size: 0.8rem;">
                <strong>Simulation Model:</strong> Evaluated using historical arrival timestamps and service durations under FIFO dispatching. Total facility service workload (<strong>${o.total_service_workload_seconds.toLocaleString()} seconds</strong>) is strictly conserved across all 8 counters.
              </span>
            </div>
          </div>

          <!-- 4 Outcome Cards -->
          <div class="kpi-grid">
            <div class="kpi-card kpi-accent-green">
              <div class="kpi-title">Financial Aid Wait Delta</div>
              <div class="kpi-number" style="color: #16A34A;">${o.fa_wait_delta_minutes.toFixed(1)} min</div>
              <div class="kpi-detail">${o.fa_base_wait.toFixed(1)}m &rarr; ${o.fa_sim_wait.toFixed(1)}m (${o.fa_wait_delta_pct.toFixed(1)}%)</div>
            </div>

            <div class="kpi-card kpi-accent-green">
              <div class="kpi-title">Max Wait Reduction</div>
              <div class="kpi-number" style="color: #16A34A;">${o.max_wait_reduction_minutes.toFixed(1)} min</div>
              <div class="kpi-detail">${o.max_base_wait.toFixed(1)}m &rarr; ${o.max_sim_wait.toFixed(1)}m (${o.max_wait_reduction_pct.toFixed(1)}%)</div>
            </div>

            <div class="kpi-card">
              <div class="kpi-title">Facility Closing Time</div>
              <div class="kpi-number" style="color: #0284C7;">${o.closing_time_delta_hours.toFixed(1)} Hours</div>
              <div class="kpi-detail">${o.base_closing_time} &rarr; ${o.sim_closing_time} (Early close)</div>
            </div>

            <div class="kpi-card kpi-accent-slate">
              <div class="kpi-title">Workload Conservation</div>
              <div class="kpi-number" style="color: #334155; font-size: 1.35rem;">${o.total_service_workload_seconds.toLocaleString()} s</div>
              <div class="kpi-detail" style="color: #16A34A; font-weight: 600;">Redistributed (0% workload loss)</div>
            </div>
          </div>

          <!-- Comparison Chart and Tradeoff Insights -->
          <div class="layout-2col">
            <div class="content-box">
              <div class="content-header">
                <span>Average Waiting Time Comparison</span>
                <span class="provenance-badge">Baseline vs. Scenario</span>
              </div>
              <div id="chart-scenario" class="chart-container"></div>
            </div>

            <div class="content-box" style="display: flex; flex-direction: column; gap: 10px;">
              <div class="content-header">Capacity Reallocation Trade-Offs</div>
              ${data.tradeoff_notes.map(t => `
                <div class="alert-card ${t.badge === 'Success' ? 'alert-success' : t.badge === 'Trade-off' ? 'alert-warning' : 'alert-info'}">
                  <strong>${t.title}</strong>
                  ${t.text}
                </div>
              `).join('')}
            </div>
          </div>

          <!-- Comparison Table -->
          <div class="content-box">
            <div class="content-header">
              <span>Baseline vs. Reallocated Scenario Comparison</span>
              <span class="provenance-badge">4 Department Queues</span>
            </div>
            <div class="table-responsive">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Queue ID</th>
                    <th>Department Service</th>
                    <th>Allocation Change</th>
                    <th class="text-right">Base Avg Wait</th>
                    <th class="text-right">Sim Avg Wait</th>
                    <th class="text-right">Wait Delta</th>
                    <th class="text-right">Change (%)</th>
                    <th class="text-right">Base Max</th>
                    <th class="text-right">Sim Max</th>
                    <th class="text-right">Base &gt;30m</th>
                    <th class="text-right">Sim &gt;30m</th>
                    <th class="text-right">Base &gt;60m</th>
                    <th class="text-right">Sim &gt;60m</th>
                  </tr>
                </thead>
                <tbody>
                  ${data.comparison.map(r => `
                    <tr>
                      <td><code>${r.queue_id}</code></td>
                      <td><strong>${r.service_name}</strong></td>
                      <td><span class="pill ${r.counter_allocation_change.includes('+') ? 'pill-green' : r.counter_allocation_change.includes('-') ? 'pill-amber' : 'pill-neutral'}">${r.counter_allocation_change}</span></td>
                      <td class="text-right">${r.base_avg_wait_minutes.toFixed(1)} min</td>
                      <td class="text-right">${r.sim_avg_wait_minutes.toFixed(1)} min</td>
                      <td class="text-right" style="color: ${r.wait_time_change_minutes < 0 ? '#16A34A' : r.wait_time_change_minutes > 0 ? '#D97706' : '#64748B'};">
                        ${r.wait_time_change_minutes > 0 ? '+' : ''}${r.wait_time_change_minutes.toFixed(1)} min
                      </td>
                      <td class="text-right">
                        ${r.wait_time_change_pct > 0 ? '+' : ''}${r.wait_time_change_pct.toFixed(1)}%
                      </td>
                      <td class="text-right">${r.base_max_wait_minutes.toFixed(1)} min</td>
                      <td class="text-right">${r.sim_max_wait_minutes.toFixed(1)} min</td>
                      <td class="text-right">${r.base_breach_30m_pct.toFixed(1)}%</td>
                      <td class="text-right">${r.sim_breach_30m_pct.toFixed(1)}%</td>
                      <td class="text-right">${r.base_breach_60m_pct.toFixed(1)}%</td>
                      <td class="text-right">${r.sim_breach_60m_pct.toFixed(1)}%</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>

          <!-- Counter Workload Under Reallocated Capacity Table -->
          <div class="content-box">
            <div class="content-header">
              <span>Counter Workload Under Reallocated Capacity</span>
              <span class="provenance-badge">Standardized vs Scenario</span>
            </div>
            <div style="background-color: #F8FAFC; border: 1px solid var(--border-card); border-radius: var(--radius-sm); padding: 8px 12px; font-size: 0.78rem; color: #475569; margin-bottom: 12px;">
              <strong>Understanding Reallocated Utilization Denominators:</strong>
              <ul style="margin: 3px 0 0 0; padding-left: 18px;">
                <li><strong>Standardized Utilization (37,311s Baseline Window):</strong> Evaluates workload against the fixed 10.36-hour baseline window. Admissions utilization remains exactly 83.2% / 83.8%, and Financial Aid counters normalize to ~65&ndash;66%.</li>
                <li><strong>Scenario Utilization (31,848s Scenario Window):</strong> Evaluates active time against the shorter 8.85-hour operating day. Because Financial Aid clears 1.5 hours sooner, counters at unchanged queues operate across a shorter facility window, mathematically registering higher utilization rates (~97&ndash;98%).</li>
              </ul>
            </div>
            <div class="table-responsive">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Counter ID</th>
                    <th>Reallocated Queue</th>
                    <th>Department Service</th>
                    <th class="text-right">Customers Served</th>
                    <th class="text-right">Active Busy Time</th>
                    <th class="text-right">Standardized Util (10.36h Window)</th>
                    <th class="text-right">Scenario Util (8.85h Window)</th>
                  </tr>
                </thead>
                <tbody>
                  ${data.reallocated_counters.map(rc => `
                    <tr>
                      <td><code>${rc.counter_id}</code></td>
                      <td><code>${rc.reallocated_queue_id}</code></td>
                      <td><strong>${rc.service_name}</strong></td>
                      <td class="text-right">${rc.customers_served.toLocaleString()}</td>
                      <td class="text-right">${rc.total_busy_hours.toFixed(2)} h</td>
                      <td class="text-right">${rc.standardized_utilization_pct.toFixed(1)}%</td>
                      <td class="text-right">${rc.scenario_utilization_pct.toFixed(1)}%</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
        `;

        document.getElementById('capacity-planning-content').innerHTML = html;
        QueueWiseCharts.renderCapacityScenarioChart('chart-scenario', data.comparison);

      } catch (err) {
        showError('capacity-planning-content', err);
      }
    },

    // =========================================================================
    // 5. DATA EXPLORER PAGE CONTROLLER
    // =========================================================================
    async initDataExplorerPage() {
      const container = document.getElementById('data-explorer-content');
      showLoading('data-explorer-content');

      try {
        const catalog = await QueueWiseAPI.fetchDatasets();

        container.innerHTML = `
          <div class="content-box">
            <div class="content-header">Analytical Datasets &amp; Export Center</div>
            <div style="font-size: 0.83rem; color: #475569;">
              Inspect and export analytical datasets produced by the QueueWise analytics pipeline.
              All records reflect synthetic simulation data (<code>is_synthetic = TRUE</code>).
            </div>
          </div>

          <div class="filter-panel">
            <div class="filter-group">
              <span class="filter-label">Select Analytical Dataset to Inspect</span>
              <select id="dataset-select" class="select-control">
                ${catalog.map(d => `
                  <option value="${d.key}">${d.title} (${d.filename})</option>
                `).join('')}
              </select>
            </div>
          </div>

          <div id="dataset-preview-area"></div>
        `;

        const selectEl = document.getElementById('dataset-select');
        selectEl.addEventListener('change', () => {
          loadPreview(selectEl.value);
        });

        async function loadPreview(datasetKey) {
          const area = document.getElementById('dataset-preview-area');
          area.innerHTML = `
            <div class="loading-wrapper">
              <div class="spinner"></div>
              <span>Loading dataset preview...</span>
            </div>
          `;

          try {
            const preview = await QueueWiseAPI.fetchDatasetPreview(datasetKey, 100);
            if (!preview || preview.total_rows === 0) {
              area.innerHTML = `
                <div class="content-box empty-state">
                  <p>Dataset <code>${datasetKey}</code> contains no records or is currently unavailable.</p>
                </div>
              `;
              return;
            }

            const downloadUrl = QueueWiseAPI.getDatasetDownloadUrl(datasetKey);

            area.innerHTML = `
              <div class="content-box">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
                  <div style="display: flex; gap: 8px; align-items: center; flex-wrap: wrap;">
                    <span class="provenance-badge">File: ${preview.filename}</span>
                    <span class="status-badge">${preview.total_rows.toLocaleString()} Records</span>
                    <span class="status-badge">${preview.columns.length} Fields</span>
                    ${preview.total_rows > 100 ? '<span class="status-badge" style="color: #0284C7;">Displaying first 100 rows</span>' : ''}
                  </div>

                  <a href="${downloadUrl}" class="btn btn-primary" download="${preview.filename}">
                    📥 Export CSV
                  </a>
                </div>

                <div class="table-responsive">
                  <table class="data-table">
                    <thead>
                      <tr>
                        ${preview.columns.map(col => `<th>${col}</th>`).join('')}
                      </tr>
                    </thead>
                    <tbody>
                      ${preview.data.map(row => `
                        <tr>
                          ${preview.columns.map(col => `<td>${row[col] !== null && row[col] !== undefined ? row[col] : '—'}</td>`).join('')}
                        </tr>
                      `).join('')}
                    </tbody>
                  </table>
                </div>
              </div>
            `;

          } catch (err) {
            area.innerHTML = `
              <div class="alert-card alert-critical">${err.message}</div>
            `;
          }
        }

        // Initial preview load with first dataset
        if (catalog.length > 0) {
          loadPreview(catalog[0].key);
        }

      } catch (err) {
        showError('data-explorer-content', err);
      }
    },
  };
})();

window.QueueWisePages = QueueWisePages;
