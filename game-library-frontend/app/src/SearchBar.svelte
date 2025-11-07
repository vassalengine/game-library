<script>
  import { cleanupSearch, normalizeWhitespace } from './lib/search.js';

  let {
    base_url,
    query = "",
    message = null,
  } = $props();

  function gotoAdvancedSearch(event) {
    const url = new URL(`${base_url}/search`);
    if (query) {
      const q = normalizeWhitespace(query);
      if (q) {
        url.searchParams.append('q', q);
      }
    }
    window.location.assign(url);
  }
</script>

<style>

.search-inner {
  background: none;
  border: 0;
  width: auto;
  flex-grow: 1;
  margin: 0;
  padding: 0.375em 0 0.375em 0.75em;
}

.search-inner:focus-visible {
  outline: none;
}

.search-outer:focus-within {
  /* copied from bootstrap form-control:focus */
  color: var(--bs-body-color);
  background-color: var(--bs-body-bg);
  border-color: #86b7fe;
  outline: 0;
  box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
}

.search-button {
  padding: 0.375em 0.75em;
}

.search-outer {
  padding: 0;
}

</style>

<nav class="d-flex flex-wrap align-items-center my-3">
  <form class="mx-md-2 my-1 flex-grow-1 order-md-1" action="{base_url}/projects" onformdata={cleanupSearch}>
    <div class="d-flex form-control search-outer">
      <input class="search-inner" type="search" name="q" placeholder="Search..." bind:value={query} required>
      <button class="search-button link-secondary border-0 bg-transparent" type="button" aria-label="Open advanced search" title="Open advanced search" onclick={gotoAdvancedSearch}><svg class="svg-icon"><use xlink:href="#sliders"></use></svg></button>
    </div>
  </form>
  <div class="w-100 d-md-none"></div>
{#if message}
  {@render message()}
{:else}
  <div class="d-lg-block d-none w-25 order-md-0"></div>
{/if}
  <div class="ms-auto mx-1 my-1 pe-0 order-md-2">
    <a href="{base_url}/projects">Browse All Projects</a>
  </div>
</nav>

<div class="svg-sprites">
  <div class="fontawesome">
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="sliders" viewBox="0 0 512 512">
        <!--!Font Awesome Free v6.7.2 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2025 Fonticons, Inc.-->        <path d="M0 416c0 17.7 14.3 32 32 32l54.7 0c12.3 28.3 40.5 48 73.3 48s61-19.7 73.3-48L480 448c17.7 0 32-14.3 32-32s-14.3-32-32-32l-246.7 0c-12.3-28.3-40.5-48-73.3-48s-61 19.7-73.3 48L32 384c-17.7 0-32 14.3-32 32zm128 0a32 32 0 1 1 64 0 32 32 0 1 1 -64 0zM320 256a32 32 0 1 1 64 0 32 32 0 1 1 -64 0zm32-80c-32.8 0-61 19.7-73.3 48L32 224c-17.7 0-32 14.3-32 32s14.3 32 32 32l246.7 0c12.3 28.3 40.5 48 73.3 48s61-19.7 73.3-48l54.7 0c17.7 0 32-14.3 32-32s-14.3-32-32-32l-54.7 0c-12.3-28.3-40.5-48-73.3-48zM192 128a32 32 0 1 1 0-64 32 32 0 1 1 0 64zm73.3-64C253 35.7 224.8 16 192 16s-61 19.7-73.3 48L32 64C14.3 64 0 78.3 0 96s14.3 32 32 32l86.7 0c12.3 28.3 40.5 48 73.3 48s61-19.7 73.3-48L480 128c17.7 0 32-14.3 32-32s-14.3-32-32-32L265.3 64z"/>
      </symbol>
    </svg>
  </div>
</div>
