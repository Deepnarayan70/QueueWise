/**
 * QueueWise — Application Bootstrap & Navigation Router
 */

document.addEventListener('DOMContentLoaded', () => {
  // Determine current active page
  const pathname = window.location.pathname.toLowerCase();
  let activeTab = 'overview';

  if (pathname.includes('queue-performance')) {
    activeTab = 'queue-performance';
  } else if (pathname.includes('counter-utilization')) {
    activeTab = 'counter-utilization';
  } else if (pathname.includes('capacity-planning')) {
    activeTab = 'capacity-planning';
  } else if (pathname.includes('data-explorer')) {
    activeTab = 'data-explorer';
  }

  // Update navigation link active classes
  const navLinks = document.querySelectorAll('.nav-link');
  navLinks.forEach(link => {
    const pageAttr = link.getAttribute('data-page');
    if (pageAttr === activeTab) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });

  // Initialize active page controller
  switch (activeTab) {
    case 'overview':
      if (window.QueueWisePages && QueueWisePages.initOverviewPage) {
        QueueWisePages.initOverviewPage();
      }
      break;
    case 'queue-performance':
      if (window.QueueWisePages && QueueWisePages.initQueuePerformancePage) {
        QueueWisePages.initQueuePerformancePage();
      }
      break;
    case 'counter-utilization':
      if (window.QueueWisePages && QueueWisePages.initCounterUtilizationPage) {
        QueueWisePages.initCounterUtilizationPage();
      }
      break;
    case 'capacity-planning':
      if (window.QueueWisePages && QueueWisePages.initCapacityPlanningPage) {
        QueueWisePages.initCapacityPlanningPage();
      }
      break;
    case 'data-explorer':
      if (window.QueueWisePages && QueueWisePages.initDataExplorerPage) {
        QueueWisePages.initDataExplorerPage();
      }
      break;
    default:
      console.warn('[QueueWise] Unknown page context:', activeTab);
  }

  // Window resize handler for responsive Plotly charts
  window.addEventListener('resize', () => {
    if (!window.Plotly) return;
    const chartContainers = document.querySelectorAll('.chart-container');
    chartContainers.forEach(container => {
      if (container && container.data) {
        Plotly.Plots.resize(container);
      }
    });
  });
});
