/*
 * Mermaid 流程图放大查看。
 *
 * Material for MkDocs 会把 pre.mermaid 的源码渲染进一个“关闭的 Shadow DOM”
 * （div.mermaid.attachShadow({mode:"closed"})），普通 querySelector 拿不到
 * 里面的 SVG。因此这里不依赖 shadow 内部：
 *   1) 提前捕获图源码（pre.mermaid 的文本），存在包裹层上；
 *   2) 按钮 / 点图时用 composedPath() 找到 shadow 宿主对应的包裹层；
 *   3) 需要放大时用 mermaid.render() 重新渲染出 SVG 放进浮层。
 * 浮层内支持缩放、拖拽平移、Ctrl/Command + 滚轮缩放，Esc 或点背景关闭。
 */
(() => {
  "use strict";

  const ICONS = {
    expand:
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M8 3H5a2 2 0 0 0-2 2v3"/><path d="M21 8V5a2 2 0 0 0-2-2h-3"/><path d="M3 16v3a2 2 0 0 0 2 2h3"/><path d="M16 21h3a2 2 0 0 0 2-2v-3"/></svg>',
    plus:
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 5v14"/><path d="M5 12h14"/></svg>',
    minus:
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h14"/></svg>',
    fit:
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 9V5a1 1 0 0 1 1-1h4"/><path d="M20 9V5a1 1 0 0 0-1-1h-4"/><path d="M4 15v4a1 1 0 0 0 1 1h4"/><path d="M20 15v4a1 1 0 0 1-1 1h-4"/></svg>',
    close:
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>'
  };

  const DIAGRAM_RE =
    /^(flowchart|flowchart-v2|graph|sequenceDiagram|stateDiagram|stateDiagram-v2|classDiagram|classDiagram-v2|gitGraph|erDiagram|journey|pie|mindmap|timeline|block-beta|quadrantChart|sankey|xychart-beta|gantt|requirementDiagram)\b/;

  let overlay = null;
  let view = null;
  let rendering = false;

  function closeZoom() {
    if (!overlay) return;
    overlay.remove();
    document.body.classList.remove("mmz-lock");
    document.removeEventListener("keydown", onKeydown, true);
    document.removeEventListener("focusin", trapFocus, true);
    overlay = null;
    view = null;
  }

  /*
   * 缩放时保持 viewport 里的 anchor 点（{vx, vy}，相对画布左上角）
   * 对应的内容点不动；不传 anchor 时以画布中心为锚点。
   */
  function setScale(next, anchor) {
    const canvas = view.canvas;
    const a = anchor || { vx: canvas.clientWidth / 2, vy: canvas.clientHeight / 2 };
    const cx = (canvas.scrollLeft + a.vx) / view.scale;
    const cy = (canvas.scrollTop + a.vy) / view.scale;
    view.scale = Math.min(10, Math.max(0.1, next));
    view.svg.style.width = `${view.vb.w * view.scale}px`;
    view.svg.style.height = `${view.vb.h * view.scale}px`;
    canvas.scrollLeft = cx * view.scale - a.vx;
    canvas.scrollTop = cy * view.scale - a.vy;
  }

  function fitZoom() {
    const canvas = view.canvas;
    const availW = Math.max(120, canvas.clientWidth - 24);
    const availH = Math.max(120, canvas.clientHeight - 24);
    view.fitScale = Math.min(availW / view.vb.w, availH / view.vb.h);
    setScale(view.fitScale, null);
    canvas.scrollLeft = 0;
    canvas.scrollTop = 0;
  }

  function onWheel(e) {
    e.preventDefault();
    const canvas = view.canvas;
    const rect = canvas.getBoundingClientRect();
    const vx = Math.max(0, Math.min(rect.width, e.clientX - rect.left));
    const vy = Math.max(0, Math.min(rect.height, e.clientY - rect.top));
    const delta = e.deltaMode === 1 ? e.deltaY * 16 : e.deltaY;
    const factor = Math.min(1.3, Math.max(0.75, Math.exp(-delta * 0.0015)));
    setScale(view.scale * factor, { vx, vy });
  }

  function onKeydown(e) {
    if (e.key === "Escape") closeZoom();
  }

  function trapFocus(e) {
    if (!overlay || e.key !== "Tab") return;
    const focusables = Array.from(overlay.querySelectorAll("button:not([disabled])"));
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  }

  function startDrag(e) {
    if (e.button !== 0 || e.pointerType !== "mouse") return;
    const canvas = view.canvas;
    const startX = e.clientX;
    const startY = e.clientY;
    const startLeft = canvas.scrollLeft;
    const startTop = canvas.scrollTop;
    canvas.classList.add("mmz-dragging");
    canvas.setPointerCapture(e.pointerId);

    const move = (ev) => {
      canvas.scrollLeft = startLeft - (ev.clientX - startX);
      canvas.scrollTop = startTop - (ev.clientY - startY);
    };
    const up = () => {
      canvas.removeEventListener("pointermove", move);
      canvas.removeEventListener("pointerup", up);
      canvas.removeEventListener("pointercancel", up);
      canvas.classList.remove("mmz-dragging");
    };
    canvas.addEventListener("pointermove", move);
    canvas.addEventListener("pointerup", up);
    canvas.addEventListener("pointercancel", up);
  }

  function viewBoxOf(svg) {
    const vb = svg.viewBox && svg.viewBox.baseVal;
    if (vb && vb.width > 0 && vb.height > 0) return { w: vb.width, h: vb.height };
    const rect = svg.getBoundingClientRect();
    return { w: rect.width || 800, h: rect.height || 600 };
  }

  function buildToolbar() {
    const bar = document.createElement("div");
    bar.className = "mmz-toolbar";
    const actions = [
      ["in", ICONS.plus, "放大"],
      ["out", ICONS.minus, "缩小"],
      ["fit", ICONS.fit, "适应窗口"],
      ["close", ICONS.close, "关闭"]
    ];
    for (const [action, icon, label] of actions) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "mmz-btn";
      btn.dataset.mmz = action;
      btn.setAttribute("aria-label", label);
      btn.title = label;
      btn.innerHTML = icon;
      bar.appendChild(btn);
    }
    return bar;
  }

  function buildOverlay(svgEl) {
    closeZoom();

    const vb = viewBoxOf(svgEl);
    const svg = svgEl.cloneNode(true);
    svg.classList.add("mmz-svg");
    svg.style.maxWidth = "none";

    const canvas = document.createElement("div");
    canvas.className = "mmz-canvas";
    canvas.appendChild(svg);

    const panel = document.createElement("div");
    panel.className = "mmz-panel";
    panel.appendChild(canvas);
    panel.appendChild(buildToolbar());
    panel.addEventListener("click", (e) => {
      const btn = e.target.closest(".mmz-btn");
      if (!btn) return;
      const action = btn.dataset.mmz;
      if (action === "in") setScale(view.scale * 1.4, null);
      else if (action === "out") setScale(view.scale / 1.4, null);
      else if (action === "fit") fitZoom();
      else if (action === "close") closeZoom();
    });

    const el = document.createElement("div");
    el.className = "mmz-overlay";
    el.setAttribute("role", "dialog");
    el.setAttribute("aria-modal", "true");
    el.setAttribute("aria-label", "流程图放大");
    el.appendChild(panel);
    el.addEventListener("click", (e) => {
      if (e.target === el) closeZoom();
    });

    document.body.appendChild(el);
    document.body.classList.add("mmz-lock");
    overlay = el;
    view = { svg, canvas, vb, scale: 1, fitScale: 1 };

    canvas.addEventListener("wheel", onWheel, { passive: false });
    canvas.addEventListener("pointerdown", startDrag);
    document.addEventListener("keydown", onKeydown, true);
    document.addEventListener("focusin", trapFocus, true);

    fitZoom();
    const firstBtn = panel.querySelector(".mmz-btn");
    if (firstBtn) firstBtn.focus();
  }

  function openZoom(svgEl) {
    if (svgEl) buildOverlay(svgEl);
  }

  async function openZoomFromSource(src) {
    if (rendering) return;
    if (!window.mermaid || typeof window.mermaid.render !== "function") return;
    rendering = true;
    try {
      const res = await window.mermaid.render(`mmz-zoom-${Date.now()}`, src);
      const box = document.createElement("div");
      box.innerHTML = res.svg;
      const svg = box.querySelector("svg");
      if (svg) buildOverlay(svg);
    } catch (e) {
      /* 渲染失败时保持页面不变 */
    } finally {
      rendering = false;
    }
  }

  function openZoomFor(wrap) {
    if (!wrap) return;
    const host = wrap.querySelector(":scope > .mermaid");
    const svg = host && host.querySelector("svg[aria-roledescription]");
    if (svg) {
      openZoom(svg);
      return;
    }
    if (wrap._mmzSrc) openZoomFromSource(wrap._mmzSrc);
  }

  function captureSource(el) {
    if (!el || !el.isConnected) return null;
    const text = (el.textContent || "").trim();
    if (!text || !DIAGRAM_RE.test(text)) return null;
    return text;
  }

  /*
   * 不动 mermaid 容器本身（源码来自它的文本；渲染后的图在关闭的 shadow DOM），
   * 而是把容器包进 .mmz-wrap，放大按钮挂在包裹层上。
   */
  function prepareDiagram(el) {
    if (el.dataset.mmzReady === "1") return;
    el.dataset.mmzReady = "1";

    if (!el.parentNode) return;
    let wrap =
      el.parentElement && el.parentElement.classList.contains("mmz-wrap")
        ? el.parentElement
        : null;
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "mmz-wrap";
      el.parentNode.insertBefore(wrap, el);
      wrap.appendChild(el);
    }

    const src = captureSource(el);
    if (src) wrap._mmzSrc = src;

    /*
     * Material 会用新 div.mermaid 替换 pre.mermaid（渲染进关闭的 shadow DOM）。
     * 新节点出现时重建按钮，避免按钮指向旧节点；点击时再实时取图。
     */
    const stale = wrap.querySelector(":scope > .mmz-open");
    if (stale) stale.remove();

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "mmz-open";
    btn.setAttribute("aria-label", "放大流程图");
    btn.title = "放大流程图";
    btn.innerHTML = ICONS.expand;
    btn.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      const w = btn.closest(".mmz-wrap");
      if (w) openZoomFor(w);
    });
    wrap.appendChild(btn);
  }

  function scanDiagrams() {
    document.querySelectorAll(".mermaid").forEach(prepareDiagram);
  }

  document.addEventListener("click", (ev) => {
    /*
     * 关闭的 shadow DOM 里点击时，target.closest 跨不过边界；
     * composedPath() 会把 shadow 内部折叠成宿主（div.mermaid），
     * 从这里找回包裹层即可。
     */
    const path = ev.composedPath ? ev.composedPath() : [];
    let wrap = null;
    for (const node of path) {
      if (node && node.classList && node.classList.contains("mmz-wrap")) {
        wrap = node;
        break;
      }
    }
    if (!wrap) return;
    if (ev.target && ev.target.closest && ev.target.closest(".mmz-open")) return;
    openZoomFor(wrap);
  });

  function start() {
    scanDiagrams();
    const observer = new MutationObserver(scanDiagrams);
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
