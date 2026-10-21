/**
 * QueueWise — REST API Client Module
 * Provides robust asynchronous data retrieval from Flask backend endpoints.
 */

const QueueWiseAPI = (() => {
  const API_BASE = '/api';

  async function _get(endpoint, params = null) {
    let url = `${API_BASE}${endpoint}`;
    if (params) {
      const searchParams = new URLSearchParams();
      for (const [key, val] of Object.entries(params)) {
        if (val !== null && val !== undefined && val !== '') {
          searchParams.append(key, val);
        }
      }
      const qs = searchParams.toString();
      if (qs) {
        url += `?${qs}`;
      }
    }

    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
        },
      });

      if (!response.ok) {
        let errMessage = `HTTP error ${response.status}: ${response.statusText}`;
        try {
          const errJson = await response.json();
          if (errJson && errJson.message) {
            errMessage = errJson.message;
          }
        } catch (_) {
          // Fallback to HTTP error
        }
        throw new Error(errMessage);
      }

      const payload = await response.json();
      return payload.data;
    } catch (err) {
      console.error(`[QueueWiseAPI] Error fetching ${url}:`, err);
      throw err;
    }
  }

  return {
    async fetchHealth() {
      return _get('/health');
    },

    async fetchOverview() {
      return _get('/overview');
    },

    async fetchQueues() {
      return _get('/queues');
    },

    async fetchQueuePerformance(queues = null, threshold = 30) {
      const params = {};
      if (queues && queues.length > 0) {
        params.queues = Array.isArray(queues) ? queues.join(',') : queues;
      }
      if (threshold) {
        params.threshold = threshold;
      }
      return _get('/queues/performance', params);
    },

    async fetchCounterUtilization() {
      return _get('/counters/utilization');
    },

    async fetchCapacityBaseline() {
      return _get('/capacity/baseline');
    },

    async fetchCapacityScenario() {
      return _get('/capacity/scenario');
    },

    async fetchDatasets() {
      return _get('/datasets');
    },

    async fetchDatasetPreview(datasetName, limit = 100) {
      return _get(`/datasets/${encodeURIComponent(datasetName)}`, { limit });
    },

    getDatasetDownloadUrl(datasetName) {
      return `${API_BASE}/datasets/${encodeURIComponent(datasetName)}/download`;
    },

    async fetchMLRisk() {
      return _get('/ml/risk');
    },

    async fetchMLMetrics() {
      return _get('/ml/metrics');
    },
  };
})();

// Export globally for browser scripts
window.QueueWiseAPI = QueueWiseAPI;
