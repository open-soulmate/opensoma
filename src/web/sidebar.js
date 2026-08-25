/**
 * Shared Sidebar Component v1.0 — JS Interaction
 * Shared by OpenMate/OpenSoul/OpenSoma：expand/collapse、active highlight、localStorage persistence
 */
;(function() {
  'use strict';

  function initSharedSidebar() {
    const sidebar = document.querySelector('.shared-sidebar');
    if (!sidebar) return;

    const toggle = sidebar.querySelector('.shared-sidebar-toggle');
    let expanded = localStorage.getItem('sidebar-expanded') === 'true';

    function update() {
      if (expanded) {
        sidebar.classList.add('expanded');
      } else {
        sidebar.classList.remove('expanded');
      }
    }
    update();

    if (toggle) {
      toggle.addEventListener('click', function() {
        expanded = !expanded;
        localStorage.setItem('sidebar-expanded', expanded);
        update();
      });
    }

    // Active highlight
    const items = sidebar.querySelectorAll('.shared-sidebar-item');
    items.forEach(function(item) {
      item.addEventListener('click', function() {
        items.forEach(function(n) { n.classList.remove('active'); });
        item.classList.add('active');
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initSharedSidebar);
  } else {
    initSharedSidebar();
  }
})();
