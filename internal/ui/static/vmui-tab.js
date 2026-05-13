// vmui-tab.js — Injects a "Lakehouse" tab into the VMUI navigation bar.
// Loaded via InjectLakehouseTab middleware into upstream VMUI HTML responses.
(function () {
  "use strict";

  var TAB_ID = "lakehouse-tab";
  var TAB_TEXT = "Lakehouse";
  var TAB_HREF = "#lakehouse";
  var IFRAME_SRC = "/lakehouse/ui/";

  function injectTab() {
    if (document.getElementById(TAB_ID)) return; // already injected

    // Try several selectors to find the VMUI navigation element.
    var nav =
      document.querySelector(".vm-header-nav") ||
      document.querySelector("nav") ||
      document.querySelector("[class*='headerNav']");
    if (!nav) return;

    // Find the last nav item and clone it to inherit existing styles.
    var items = nav.children;
    if (items.length === 0) return;
    var lastItem = items[items.length - 1];
    var tab = lastItem.cloneNode(true);

    tab.id = TAB_ID;
    tab.textContent = TAB_TEXT;
    if (tab.tagName === "A") {
      tab.href = TAB_HREF;
    } else {
      tab.setAttribute("data-href", TAB_HREF);
    }

    tab.addEventListener("click", function (e) {
      e.preventDefault();
      // Remove active class from sibling tabs.
      Array.prototype.forEach.call(nav.children, function (child) {
        child.classList.remove("active");
      });
      tab.classList.add("active");

      // Replace main content area with an iframe pointing to Lakehouse UI.
      var main =
        document.querySelector("main") ||
        document.querySelector("[class*='content']") ||
        document.querySelector(".vm-container");
      if (main) {
        main.innerHTML =
          '<iframe src="' +
          IFRAME_SRC +
          '" style="width:100%;height:calc(100vh - 60px);border:none;"></iframe>';
      }
    });

    nav.appendChild(tab);
  }

  // Primary: observe DOM mutations for dynamically rendered VMUI nav.
  var observer = new MutationObserver(function () {
    injectTab();
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });

  // Fallback: also try on DOMContentLoaded.
  document.addEventListener("DOMContentLoaded", injectTab);
})();
