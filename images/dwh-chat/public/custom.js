(function () {
  "use strict";

  // --- Lightbox modal ---
  function createLightbox() {
    const overlay = document.createElement('div');
    overlay.id = 'img-lightbox';
    overlay.innerHTML = '<img id="img-lightbox-img" />';
    document.body.appendChild(overlay);

    // Close on click anywhere
    overlay.addEventListener('click', function () {
      overlay.classList.remove('active');
    });

    // Close on Escape
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') overlay.classList.remove('active');
    });
  }

  function openLightbox(src) {
    const overlay = document.getElementById('img-lightbox');
    const img = document.getElementById('img-lightbox-img');
    img.src = src;
    overlay.classList.add('active');
  }

  // --- Image click-to-zoom ---
  function initImageZoom(root) {
    root.querySelectorAll('.message-content img:not([data-zoom-init])').forEach(function (img) {
      img.setAttribute('data-zoom-init', '1');
      img.addEventListener('click', function () {
        openLightbox(img.src);
      });
    });
  }

  // --- Boot ---
  function boot() {
    createLightbox();

    const imgObserver = new MutationObserver(function () {
      initImageZoom(document);
    });
    imgObserver.observe(document.body, { childList: true, subtree: true });
    initImageZoom(document);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
