// vmui-tab.js — Injects a "Lakehouse" tab into the VMUI navigation bar.
// When clicked, replaces the VMUI content area with an inline Lakehouse dashboard
// that uses VMUI CSS variables for consistent styling.
(function () {
  "use strict";

  var TAB_ID = "lakehouse-tab";
  var TAB_TEXT = "Lakehouse";
  var ACTIVE_KEY = "lh_vmui_active"; // localStorage flag: Lakehouse tab was last active

  // The render core lives in the shared module lakehouse-ui.js (single source of
  // truth, also used by the standalone /lakehouse/ui/ page). This file is ONLY
  // the VMUI integration: inject a Lakehouse tab and mount the shared UI into it.
  function ensureUI(cb) {
    if (window.LakehouseUI) { cb(); return; }
    var s = document.getElementById("lh-ui-script");
    if (s) { s.addEventListener("load", cb); return; }
    s = document.createElement("script");
    s.id = "lh-ui-script";
    s.src = "/lakehouse/ui/lakehouse-ui.js";
    s.addEventListener("load", cb);
    document.head.appendChild(s);
  }

  // ---- Content area management ----
  // VMUI is a React SPA. When showing Lakehouse content we hide all direct
  // children of the app container EXCEPT the header/nav, then show our own
  // wrapper. This avoids depending on specific VMUI class names which differ
  // between views (Query vs Overview vs Stats).

  var lhWrapper = null;
  var hiddenEls = [];
  var lhActive = false;    // currently showing the Lakehouse view?
  var appObserved = null;  // app container the re-assert observer is attached to

  function isHeaderOrNav(node) {
    if (node.tagName === "HEADER") return true;
    if (node.classList && (node.classList.contains("vm-header") ||
        node.classList.contains("vm-header-nav"))) return true;
    if (node.querySelector && node.querySelector(".vm-header-nav, .vm-header, nav")) return true;
    return false;
  }

  // showLakehouse hides vmui's own views and shows our wrapper. forceRender
  // re-renders the dashboard content (used on an explicit tab click); on a
  // passive re-assert (React re-rendered under us) we only re-render when our
  // wrapper was detached, to avoid a refetch storm.
  function showLakehouse(forceRender) {
    var root = document.getElementById("root");
    if (!root || !root.firstElementChild) return;
    var app = root.firstElementChild;

    // Restore any previously hidden elements first (refs may be stale after a
    // React re-render).
    hiddenEls.forEach(function (e) { e.style.display = e._lhOldDisplay || ""; });
    hiddenEls = [];

    // Hide all direct children except header/nav and our wrapper.
    Array.prototype.forEach.call(app.children, function (child) {
      if (child.id === "lh-wrapper") return;
      if (isHeaderOrNav(child)) return;
      child._lhOldDisplay = child.style.display;
      child.style.display = "none";
      hiddenEls.push(child);
    });

    // (Re)create the wrapper if absent or if React detached it.
    var fresh = false;
    if (!lhWrapper || !lhWrapper.isConnected) {
      lhWrapper = document.createElement("div");
      lhWrapper.id = "lh-wrapper";
      lhWrapper.style.cssText = "flex:1;overflow:auto;min-height:0";
      app.appendChild(lhWrapper);
      fresh = true;
    }
    lhWrapper.style.display = "";
    lhActive = true;
    if (fresh || forceRender) ensureUI(function () { window.LakehouseUI.mount(lhWrapper); });

    startAppObserver(app); // keep the view asserted across React re-renders
  }

  function hideLakehouse() {
    lhActive = false;
    if (lhWrapper) lhWrapper.style.display = "none";
    hiddenEls.forEach(function (e) {
      e.style.display = e._lhOldDisplay || "";
    });
    hiddenEls = [];
  }

  // ---- Re-assert across React re-renders ----
  // After a reload vmui renders its Query view into the app container
  // asynchronously — often AFTER we've restored Lakehouse — which un-hides
  // vmui's content and can detach our wrapper. While the Lakehouse view is
  // active we watch the app container's direct children and re-hide vmui's
  // content. Gated on lhActive so it never fights the user once they navigate
  // to a real vmui tab. showLakehouse(false) makes no childList change when the
  // wrapper is already attached, so this cannot loop.
  var reassertScheduled = false;
  function scheduleReassert() {
    if (reassertScheduled) return;
    reassertScheduled = true;
    setTimeout(function () {
      reassertScheduled = false;
      if (lhActive) showLakehouse(false);
    }, 40);
  }
  function startAppObserver(app) {
    if (appObserved === app) return; // already watching this container
    appObserved = app;
    new MutationObserver(function () {
      if (lhActive) scheduleReassert();
    }).observe(app, { childList: true });
  }

  // ---- Tab injection + click handling ----

  function injectTab() {
    if (document.getElementById(TAB_ID)) return;

    var nav = document.querySelector(".vm-header-nav") ||
              document.querySelector("nav") ||
              document.querySelector("[class*='headerNav']");
    if (!nav) return;

    var items = nav.children;
    if (items.length === 0) return;
    var lastItem = items[items.length - 1];
    var tab = lastItem.cloneNode(true);

    tab.id = TAB_ID;
    tab.textContent = TAB_TEXT;
    if (tab.tagName === "A") tab.href = "#lakehouse";
    else tab.setAttribute("data-href", "#lakehouse");

    function activateLakehouse() {
      Array.prototype.forEach.call(nav.children, function (child) {
        child.classList.remove("active");
      });
      tab.classList.add("active");
      showLakehouse(true);
    }

    tab.addEventListener("click", function (e) {
      e.preventDefault();
      // Remember the Lakehouse tab so a reload returns here instead of snapping
      // back to vmui's Query page (vmui's hash router has no notion of our tab).
      try { localStorage.setItem(ACTIVE_KEY, "1"); } catch (x) { /* ignore */ }
      activateLakehouse();
    });

    // Restore VMUI content (and forget our tab) when other tabs are clicked.
    Array.prototype.forEach.call(nav.children, function (child) {
      if (child.id === TAB_ID) return;
      child.addEventListener("click", function () {
        try { localStorage.removeItem(ACTIVE_KEY); } catch (x) { /* ignore */ }
        tab.classList.remove("active");
        hideLakehouse();
      });
    });

    nav.appendChild(tab);

    // On (re)load, if Lakehouse was the last-active tab, restore it. vmui mounts
    // its React view asynchronously, so retry until the app container exists and
    // the view sticks (lhActive); once shown, startAppObserver keeps re-asserting
    // as React settles. Self-terminates on first success, so a later navigation
    // away is never overridden.
    var wasActive = false;
    try { wasActive = localStorage.getItem(ACTIVE_KEY) === "1"; } catch (x) { /* ignore */ }
    if (wasActive) {
      var rtries = 0;
      (function tryRestore() {
        activateLakehouse();
        if (!lhActive && ++rtries < 40) setTimeout(tryRestore, 75);
      })();
    }
  }

  // Preload the shared UI module so the first tab activation is instant (the CSS
  // is injected by LakehouseUI.mount, not here).
  ensureUI(function () { /* loaded; mount happens on tab activation */ });

  // Observe DOM for VMUI's dynamic nav; disconnect once found.
  var observer = new MutationObserver(function () {
    if (document.getElementById(TAB_ID)) { observer.disconnect(); return; }
    injectTab();
    if (document.getElementById(TAB_ID)) observer.disconnect();
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });
  document.addEventListener("DOMContentLoaded", function () {
    injectTab();
    if (document.getElementById(TAB_ID)) observer.disconnect();
  });
})();
