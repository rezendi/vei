// Studio rendering for the Company Wiki materialized view.

(function () {
  const wikiState = {
    pages: [],
    activePageId: null,
    activePage: null,
    builtAt: "",
    loaded: false,
    loading: false,
    refreshing: false,
    lastError: null,
  };
  studio.wikiState = wikiState;

  function setBuildStatus(message) {
    const node = document.getElementById("wiki-build-status");
    if (node) {
      node.textContent = message;
    }
  }

  function escapeWikiHtml(value) {
    if (typeof escapeHtml === "function") {
      return escapeHtml(value);
    }
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function renderInlineMarkdown(text) {
    // Handle a small subset: **bold**, _italic_, `code`. Newlines become <br>.
    const escaped = escapeWikiHtml(text);
    return escaped
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
      .replace(/`([^`]+)`/g, "<code>$1</code>")
      .replace(/\b_([^_]+)_\b/g, "<em>$1</em>");
  }

  function renderBlockMarkdown(body) {
    if (!body) return "";
    const lines = String(body).split(/\r?\n/);
    const blocks = [];
    let listBuffer = [];
    let listOrdered = false;
    const flushList = () => {
      if (listBuffer.length) {
        const tag = listOrdered ? "ol" : "ul";
        blocks.push(`<${tag}>${listBuffer.map((item) => `<li>${item}</li>`).join("")}</${tag}>`);
        listBuffer = [];
        listOrdered = false;
      }
    };
    for (const rawLine of lines) {
      const line = rawLine.trimEnd();
      if (!line.trim()) {
        flushList();
        continue;
      }
      if (/^- /.test(line)) {
        if (listOrdered && listBuffer.length) flushList();
        listOrdered = false;
        listBuffer.push(renderInlineMarkdown(line.replace(/^- /, "")));
        continue;
      }
      if (/^  - /.test(line)) {
        if (listBuffer.length) {
          listBuffer[listBuffer.length - 1] +=
            `<div class="wiki-li-detail">${renderInlineMarkdown(line.replace(/^  - /, ""))}</div>`;
        } else {
          listBuffer.push(renderInlineMarkdown(line.replace(/^  - /, "")));
        }
        continue;
      }
      if (/^\d+\. /.test(line)) {
        if (!listOrdered && listBuffer.length) flushList();
        listOrdered = true;
        listBuffer.push(renderInlineMarkdown(line.replace(/^\d+\. /, "")));
        continue;
      }
      if (line.startsWith("> ")) {
        flushList();
        blocks.push(`<blockquote>${renderInlineMarkdown(line.slice(2))}</blockquote>`);
        continue;
      }
      flushList();
      blocks.push(`<p>${renderInlineMarkdown(line)}</p>`);
    }
    flushList();
    return blocks.join("");
  }

  function renderCitationChips(citations) {
    if (!Array.isArray(citations) || !citations.length) {
      return "";
    }
    const chips = citations.slice(0, 6).map((citation) => {
      const authority = citation.authority === "curated" ? "curated" : "projected";
      const label = citation.title || citation.ref_id || citation.source;
      const tooltip = `${citation.source} :: ${citation.ref_id}` +
        (citation.snippet ? ` -- ${citation.snippet}` : "");
      return `<span class="wiki-citation-chip wiki-authority-${authority}" title="${escapeWikiHtml(tooltip)}">${escapeWikiHtml(label)}</span>`;
    });
    return `<div class="wiki-citation-row">${chips.join("")}</div>`;
  }

  function renderSection(section) {
    const sectionAuthority = section.metadata && section.metadata.authority;
    const authorityChip = sectionAuthority
      ? `<span class="wiki-authority-pill wiki-authority-${escapeWikiHtml(sectionAuthority)}">${escapeWikiHtml(sectionAuthority)}</span>`
      : "";
    return `
      <div class="wiki-section" id="wiki-section-${escapeWikiHtml(section.section_id)}">
        <h4>${escapeWikiHtml(section.title)}${authorityChip}</h4>
        <div class="wiki-section-body">${renderBlockMarkdown(section.body_md || "")}</div>
        ${renderCitationChips(section.citations)}
      </div>
    `;
  }

  function renderPageContent(page) {
    if (!page) {
      const content = document.getElementById("wiki-page-content");
      if (content) content.innerHTML = "";
      return;
    }
    const titleNode = document.getElementById("wiki-page-title");
    const summaryNode = document.getElementById("wiki-page-summary");
    if (titleNode) titleNode.textContent = page.title || "";
    if (summaryNode) summaryNode.textContent = page.summary || "";
    const content = document.getElementById("wiki-page-content");
    if (content) {
      const sections = Array.isArray(page.sections) ? page.sections : [];
      content.innerHTML = sections.map(renderSection).join("");
    }
    const next = document.getElementById("wiki-next-steps");
    if (next) {
      const steps = Array.isArray(page.next_steps) ? page.next_steps : [];
      if (!steps.length) {
        next.innerHTML = "";
      } else {
        next.innerHTML = `
          <h4>What would make this richer</h4>
          <ul>${steps.map((step) => `<li>${renderInlineMarkdown(step)}</li>`).join("")}</ul>
        `;
      }
    }
  }

  function renderPageNav() {
    const nav = document.getElementById("wiki-page-nav");
    if (!nav) return;
    if (!wikiState.pages.length) {
      nav.innerHTML = `<p class="metric-detail">No wiki pages built yet.</p>`;
      return;
    }
    nav.innerHTML = wikiState.pages
      .map((page) => {
        const cls = page.page_id === wikiState.activePageId ? "wiki-page-nav-button active" : "wiki-page-nav-button";
        const meta = `${page.citation_count || 0} citation${page.citation_count === 1 ? "" : "s"}`;
        return `
          <button type="button" class="${cls}" data-wiki-page-id="${escapeWikiHtml(page.page_id)}">
            <span class="wiki-page-nav-title">${escapeWikiHtml(page.title)}</span>
            <span class="wiki-page-nav-summary">${escapeWikiHtml(page.summary || "")}</span>
            <span class="wiki-page-nav-meta">${escapeWikiHtml(meta)}</span>
          </button>
        `;
      })
      .join("");
    nav.querySelectorAll("[data-wiki-page-id]").forEach((node) => {
      node.addEventListener("click", () => {
        const pageId = node.getAttribute("data-wiki-page-id");
        if (pageId) {
          void selectWikiPage(pageId);
        }
      });
    });
  }

  async function loadWikiPages({ silent = false } = {}) {
    if (!silent) {
      setBuildStatus("Loading wiki\u2026");
    }
    wikiState.loading = true;
    try {
      const payload = await getJson("/api/workspace/wiki/pages");
      wikiState.pages = Array.isArray(payload?.pages) ? payload.pages : [];
      wikiState.builtAt = payload?.built_at || "";
      wikiState.loaded = true;
      wikiState.lastError = null;
      const previousPageId = wikiState.activePageId;
      const stillExists = wikiState.pages.some((p) => p.page_id === previousPageId);
      const nextPageId = stillExists ? previousPageId : (wikiState.pages[0]?.page_id || null);
      renderPageNav();
      if (nextPageId) {
        await selectWikiPage(nextPageId, { force: true });
      } else {
        renderPageContent(null);
      }
      const builtAtSuffix = wikiState.builtAt ? ` :: built ${wikiState.builtAt}` : "";
      setBuildStatus(`${wikiState.pages.length} page${wikiState.pages.length === 1 ? "" : "s"}${builtAtSuffix}`);
    } catch (error) {
      wikiState.lastError = error?.message || String(error);
      setBuildStatus(`Wiki unavailable: ${wikiState.lastError}`);
      const nav = document.getElementById("wiki-page-nav");
      if (nav) nav.innerHTML = "";
      renderPageContent(null);
    } finally {
      wikiState.loading = false;
    }
  }

  async function selectWikiPage(pageId, { force = false } = {}) {
    if (!pageId) return;
    if (!force && wikiState.activePageId === pageId && wikiState.activePage) {
      return;
    }
    wikiState.activePageId = pageId;
    renderPageNav();
    try {
      const page = await getJson(`/api/workspace/wiki/pages/${encodeURIComponent(pageId)}`);
      wikiState.activePage = page;
      renderPageContent(page);
    } catch (error) {
      const detail = error?.message || String(error);
      const content = document.getElementById("wiki-page-content");
      if (content) {
        content.innerHTML = `<p class="metric-detail">Failed to load page: ${escapeWikiHtml(detail)}</p>`;
      }
    }
  }

  async function refreshWiki() {
    if (wikiState.refreshing) return;
    wikiState.refreshing = true;
    setBuildStatus("Rebuilding wiki\u2026");
    try {
      const report = await getJson("/api/workspace/wiki/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      const status = report?.status || "ok";
      const pageCount = report?.page_count ?? 0;
      const citationCount = report?.citation_count ?? 0;
      setBuildStatus(`Refreshed ${pageCount} page${pageCount === 1 ? "" : "s"} (${citationCount} citations) :: status ${status}`);
      await loadWikiPages({ silent: true });
    } catch (error) {
      const detail = error?.message || String(error);
      setBuildStatus(`Refresh failed: ${detail}`);
    } finally {
      wikiState.refreshing = false;
    }
  }

  function bindWikiControls() {
    const button = document.getElementById("wiki-refresh-button");
    if (button && !button.dataset.bound) {
      button.addEventListener("click", () => {
        void refreshWiki();
      });
      button.dataset.bound = "true";
    }
  }

  async function ensureWikiLoaded() {
    bindWikiControls();
    if (wikiState.loaded || wikiState.loading) {
      return;
    }
    await loadWikiPages();
  }

  studio.wiki = {
    ensureWikiLoaded,
    loadWikiPages,
    refreshWiki,
    selectWikiPage,
  };
  if (typeof window !== "undefined") {
    window.ensureWikiLoaded = ensureWikiLoaded;
    window.refreshWiki = refreshWiki;
  }
})();
