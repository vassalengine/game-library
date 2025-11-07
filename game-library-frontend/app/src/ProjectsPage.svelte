<script>
  import { fetchJSON } from './lib/client.js';
  import { makeRequestURL } from './lib/search.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import SearchBar from './SearchBar.svelte';
  import SearchPageGuts from './SearchPageGuts.svelte';

  let {
    current_version,
    news_link,
    base_url,
    user_info,
    gls_url,
    discourse_url,
    ums_url,
    returnto,
    limit
  } = $props();

  let error = $state(null);
  let meta = $state(null);
  let projects = $state(null);

  function unpackParams(params) {
    const q = params.get('q');

    // default query sort is relevance, otherwise title
    const sort_by = params.get('sort_by') ?? (!!q ? 'r' : 't');

    return [sort_by, q];
  }

  const params = new URLSearchParams(window.location.search);
  const [ sort_by, query ] = unpackParams(params);

  function loadProjects(url) {
     fetchJSON(url)
      .then((result) => ({ projects, meta } = result))
      .catch((err) => error = err);
  }

  loadProjects(makeRequestURL(gls_url, params, limit));
</script>

<style>

/* Alphabetical index */

#index ol {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  padding: 0;
  margin: 0 auto;
  gap: 0.4em;
}

#index li {
  list-style: none;
  white-space: nowrap;
}

@media only screen and (max-width:767px) {
  #index ol {
    max-width: 15em;
  }
}

</style>

<svelte:head>
  {#if query === null}
  <title>Module Library - Vassal</title>
  {:else}
  <title>Search Results for {query} - Module Library - Vassal</title>
  {/if}
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

{#snippet search_msg()}
  {#if projects}
  <div class="small me-auto mx-1 my-1 order-md-0">
<!-- TODO: Is there some way to provide a range for the count? -->
    <span class="text-muted">Displaying</span>
    <b>{projects.length}</b>
    <span class="text-muted">of</span>
    <b>{meta.total}</b>
    <span class="text-muted">{params.has('q') ? "search result" : "module"}{meta.total === 1 ? "" : "s"}</span>
  </div>
  {:else}
  <div class="d-lg-block d-none w-25 order-md-0"></div>
  {/if}
{/snippet}

<SearchBar {base_url} {query} message={search_msg} />

<div class="my-1 p-3 bg-light rounded">
  <h1 class="m-0">
  {#if query === null}
  All Modules
  {:else}
  Search Results <small>for '{query}'</small>
  {/if}
  </h1>
</div>

{#if query === null}
<nav id="index" class="my-2">
  <ol>
    <li><a href="?sort_by=t">0–9</a></li>
    <li><a href="?sort_by=t&from=A">A</a></li>
    <li><a href="?sort_by=t&from=B">B</a></li>
    <li><a href="?sort_by=t&from=C">C</a></li>
    <li><a href="?sort_by=t&from=D">D</a></li>
    <li><a href="?sort_by=t&from=E">E</a></li>
    <li><a href="?sort_by=t&from=F">F</a></li>
    <li><a href="?sort_by=t&from=G">G</a></li>
    <li><a href="?sort_by=t&from=H">H</a></li>
    <li><a href="?sort_by=t&from=I">I</a></li>
    <li><a href="?sort_by=t&from=J">J</a></li>
    <li><a href="?sort_by=t&from=K">K</a></li>
    <li><a href="?sort_by=t&from=L">L</a></li>
    <li><a href="?sort_by=t&from=M">M</a></li>
    <li><a href="?sort_by=t&from=N">N</a></li>
    <li><a href="?sort_by=t&from=O">O</a></li>
    <li><a href="?sort_by=t&from=P">P</a></li>
    <li><a href="?sort_by=t&from=Q">Q</a></li>
    <li><a href="?sort_by=t&from=R">R</a></li>
    <li><a href="?sort_by=t&from=S">S</a></li>
    <li><a href="?sort_by=t&from=T">T</a></li>
    <li><a href="?sort_by=t&from=U">U</a></li>
    <li><a href="?sort_by=t&from=V">V</a></li>
    <li><a href="?sort_by=t&from=W">W</a></li>
    <li><a href="?sort_by=t&from=X">X</a></li>
    <li><a href="?sort_by=t&from=Y">Y</a></li>
    <li><a href="?sort_by=t&from=Z">Z</a></li>
  </ol>
</nav>
{/if}

<SearchPageGuts {base_url} {gls_url} {sort_by} relevance={!!query} {limit} {loadProjects} bind:error={error} bind:meta={meta} bind:projects={projects} />

</main>

<Footer/>
