/**
 * QueueWise — Plotly.js Chart Rendering Module
 * Renders restrained, high-fidelity enterprise visualizations.
 */

const QueueWiseCharts = (() => {
  const DEFAULT_FONT = {
    family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    color: '#334155',
    size: 12,
  };

  const DEFAULT_HOVER = {
    bgcolor: '#FFFFFF',
    bordercolor: '#CBD5E1',
    font: {
      family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      size: 12,
      color: '#0F172A',
    },
  };

  function applyCleanLayout(customLayout = {}) {
    const base = {
      font: DEFAULT_FONT,
      paper_bgcolor: '#FFFFFF',
      plot_bgcolor: '#FFFFFF',
      hoverlabel: DEFAULT_HOVER,
      margin: { l: 45, r: 25, t: 30, b: 40 },
      xaxis: {
        gridcolor: '#F1F5F9',
        zerolinecolor: '#CBD5E1',
        automargin: true,
      },
      yaxis: {
        gridcolor: '#F1F5F9',
        zerolinecolor: '#CBD5E1',
        automargin: true,
      },
      legend: {
        orientation: 'h',
        yanchor: 'bottom',
        y: 1.02,
        xanchor: 'right',
        x: 1,
        font: { size: 11 },
      },
    };
    return Object.assign(base, customLayout);
  }

  return {
    renderBenchmarkChart(containerId, benchmarkData) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const items = benchmarkData.items || [];
      const yLabels = items.map(d => d.service_name);
      const xValues = items.map(d => d.avg_wait_minutes);
      const colors = items.map(d => d.color);
      const textLabels = items.map(d => `${d.avg_wait_minutes.toFixed(1)}m`);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: xValues,
        y: yLabels,
        marker: { color: colors },
        text: textLabels,
        textposition: 'auto',
        hovertemplate: '<b>%{y}</b><br>Average Wait: %{x:.1f} min<extra></extra>',
      };

      const maxVal = Math.max(...xValues, 35);

      const layout = applyCleanLayout({
        height: 280,
        margin: { l: 180, r: 25, t: 25, b: 35 },
        xaxis: {
          title: 'Average Wait Time (Minutes)',
          range: [0, maxVal * 1.15],
          gridcolor: '#F1F5F9',
          zerolinecolor: '#CBD5E1',
          automargin: true,
        },
        yaxis: {
          automargin: true,
          gridcolor: '#FFFFFF',
        },
        shapes: [
          {
            type: 'rect',
            xref: 'x',
            yref: 'paper',
            x0: 0,
            x1: 30,
            y0: 0,
            y1: 1,
            fillcolor: '#DCFCE7',
            opacity: 0.35,
            layer: 'below',
            line: { width: 0 },
          },
          {
            type: 'line',
            xref: 'x',
            yref: 'paper',
            x0: 30,
            x1: 30,
            y0: 0,
            y1: 1,
            line: { color: '#16A34A', width: 2, dash: 'dot' },
          },
        ],
        annotations: [
          {
            x: 30,
            y: 1,
            xref: 'x',
            yref: 'paper',
            text: 'Target SLA Limit (30m)',
            showarrow: false,
            xanchor: 'left',
            yanchor: 'top',
            font: { size: 11, color: '#166534' },
          },
        ],
      });

      Plotly.newPlot(container, [trace], layout, { responsive: true, displayModeBar: false });
    },

    renderHourlyArrivalsChart(containerId, arrivalsData) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const xHours = arrivalsData.map(d => d.hour);
      const yCounts = arrivalsData.map(d => d.arrival_count);

      const trace = {
        type: 'scatter',
        mode: 'lines+markers',
        x: xHours,
        y: yCounts,
        line: { color: '#0284C7', width: 2.5, shape: 'spline' },
        marker: { size: 7, color: '#0284C7', symbol: 'circle' },
        fill: 'tozeroy',
        fillcolor: 'rgba(2, 132, 199, 0.08)',
        hovertemplate: '<b>%{x}</b><br>Customer Arrivals: %{y}<extra></extra>',
      };

      const layout = applyCleanLayout({
        height: 280,
        margin: { l: 45, r: 20, t: 25, b: 35 },
        xaxis: { title: 'Operating Hour', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
        yaxis: { title: 'Arrival Count', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
      });

      Plotly.newPlot(container, [trace], layout, { responsive: true, displayModeBar: false });
    },

    renderPerformanceComparisonChart(containerId, metrics) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const xLabels = metrics.map(d => d.service_name);
      const avgWaits = metrics.map(d => d.avg_wait_minutes);
      const medianWaits = metrics.map(d => d.median_wait_minutes);
      const serviceTimes = metrics.map(d => d.avg_service_minutes);

      const traces = [
        {
          name: 'Average Wait',
          type: 'bar',
          x: xLabels,
          y: avgWaits,
          marker: { color: '#0284C7' },
          text: avgWaits.map(v => `${v.toFixed(1)}m`),
          textposition: 'auto',
          hovertemplate: '<b>%{x}</b><br>Average Wait: %{y:.1f} min<extra></extra>',
        },
        {
          name: 'Median Wait',
          type: 'bar',
          x: xLabels,
          y: medianWaits,
          marker: { color: '#93C5FD' },
          text: medianWaits.map(v => `${v.toFixed(1)}m`),
          textposition: 'auto',
          hovertemplate: '<b>%{x}</b><br>Median Wait: %{y:.1f} min<extra></extra>',
        },
        {
          name: 'Average Service Duration',
          type: 'bar',
          x: xLabels,
          y: serviceTimes,
          marker: { color: '#10B981' },
          text: serviceTimes.map(v => `${v.toFixed(1)}m`),
          textposition: 'auto',
          hovertemplate: '<b>%{x}</b><br>Avg Service: %{y:.1f} min<extra></extra>',
        },
      ];

      const layout = applyCleanLayout({
        barmode: 'group',
        height: 300,
        margin: { l: 45, r: 20, t: 25, b: 35 },
        yaxis: { title: 'Minutes', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
      });

      Plotly.newPlot(container, traces, layout, { responsive: true, displayModeBar: false });
    },

    renderWaitDistributionChart(containerId, curves, thresholdMinutes = 30) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const traces = [];
      for (const [qid, curve] of Object.entries(curves)) {
        traces.push({
          type: 'scatter',
          mode: 'lines',
          name: curve.service_name,
          x: curve.x_wait_minutes,
          y: curve.y_cumulative_pct,
          line: { color: curve.color, width: 2.2 },
          hovertemplate: `<b>${curve.service_name}</b><br>Wait &le; %{x:.1f} min: %{y:.1f}%<extra></extra>`,
        });
      }

      const layout = applyCleanLayout({
        height: 300,
        margin: { l: 45, r: 20, t: 25, b: 35 },
        xaxis: { title: 'Wait Time (Minutes)', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
        yaxis: { title: 'Cumulative Customers (%)', range: [0, 105], gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
        shapes: [
          {
            type: 'line',
            xref: 'x',
            yref: 'paper',
            x0: thresholdMinutes,
            x1: thresholdMinutes,
            y0: 0,
            y1: 1,
            line: { color: '#475569', width: 1.5, dash: 'dash' },
          },
        ],
        annotations: [
          {
            x: thresholdMinutes,
            y: 0.95,
            xref: 'x',
            yref: 'paper',
            text: `${thresholdMinutes}m benchmark`,
            showarrow: false,
            xanchor: 'right',
            font: { size: 11, color: '#475569' },
          },
        ],
      });

      Plotly.newPlot(container, traces, layout, { responsive: true, displayModeBar: false });
    },

    renderCounterUtilizationChart(containerId, utilData) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const counters = utilData.counters || [];
      const xCounters = counters.map(d => d.counter_id);
      const yUtils = counters.map(d => d.utilization_pct);

      function getColor(u) {
        if (u >= 90.0) return '#DC2626';
        if (u >= 75.0) return '#D97706';
        if (u >= 50.0) return '#2563EB';
        return '#64748B';
      }

      const colors = yUtils.map(getColor);
      const textLabels = yUtils.map(v => `${v.toFixed(1)}%`);

      const trace = {
        type: 'bar',
        x: xCounters,
        y: yUtils,
        marker: { color: colors },
        text: textLabels,
        textposition: 'auto',
        hovertemplate: '<b>%{x}</b><br>Utilization: %{y:.1f}%<extra></extra>',
      };

      const layout = applyCleanLayout({
        height: 300,
        margin: { l: 45, r: 20, t: 25, b: 35 },
        xaxis: { title: 'Physical Counter ID', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
        yaxis: { title: 'Utilization (%)', range: [0, 110], gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
        shapes: [
          {
            type: 'line',
            xref: 'paper',
            yref: 'y',
            x0: 0,
            x1: 1,
            y0: 90.0,
            y1: 90.0,
            line: { color: '#DC2626', width: 1.5, dash: 'dash' },
          },
          {
            type: 'line',
            xref: 'paper',
            yref: 'y',
            x0: 0,
            x1: 1,
            y0: 75.0,
            y1: 75.0,
            line: { color: '#D97706', width: 1.5, dash: 'dot' },
          },
        ],
        annotations: [
          {
            x: 1,
            y: 90.0,
            xref: 'paper',
            yref: 'y',
            text: 'Critical Saturation (90%)',
            showarrow: false,
            xanchor: 'right',
            yanchor: 'bottom',
            font: { size: 11, color: '#DC2626' },
          },
          {
            x: 1,
            y: 75.0,
            xref: 'paper',
            yref: 'y',
            text: 'Target Load Limit (75%)',
            showarrow: false,
            xanchor: 'right',
            yanchor: 'bottom',
            font: { size: 11, color: '#D97706' },
          },
        ],
      });

      Plotly.newPlot(container, [trace], layout, { responsive: true, displayModeBar: false });
    },

    renderCapacityScenarioChart(containerId, comparison) {
      const container = document.getElementById(containerId);
      if (!container || !window.Plotly) return;

      const xLabels = comparison.map(d => d.service_name);
      const baseWaits = comparison.map(d => d.base_avg_wait_minutes);
      const simWaits = comparison.map(d => d.sim_avg_wait_minutes);

      const traces = [
        {
          name: 'Baseline (2 Counters)',
          type: 'bar',
          x: xLabels,
          y: baseWaits,
          marker: { color: '#64748B' },
          text: baseWaits.map(v => `${v.toFixed(1)}m`),
          textposition: 'auto',
          hovertemplate: '<b>%{x}</b><br>Baseline Avg Wait: %{y:.1f} min<extra></extra>',
        },
        {
          name: 'Reallocated Scenario',
          type: 'bar',
          x: xLabels,
          y: simWaits,
          marker: { color: '#0284C7' },
          text: simWaits.map(v => `${v.toFixed(1)}m`),
          textposition: 'auto',
          hovertemplate: '<b>%{x}</b><br>Reallocated Avg Wait: %{y:.1f} min<extra></extra>',
        },
      ];

      const layout = applyCleanLayout({
        barmode: 'group',
        height: 300,
        margin: { l: 45, r: 20, t: 25, b: 35 },
        yaxis: { title: 'Average Wait (Minutes)', gridcolor: '#F1F5F9', zerolinecolor: '#CBD5E1', automargin: true },
      });

      Plotly.newPlot(container, traces, layout, { responsive: true, displayModeBar: false });
    },
  };
})();

window.QueueWiseCharts = QueueWiseCharts;
