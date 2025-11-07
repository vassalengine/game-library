<script>
  import { fetchJSON } from './lib/client.js';
  import AutocompleteFetcher, { cleanupSearch, makeRequestURL } from './lib/search.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import SearchPageGuts from './SearchPageGuts.svelte';
  import ChipInput from './ChipInput.svelte';
  import AutocompleteInput from './AutocompleteInput.svelte';

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

  const params = new URLSearchParams(window.location.search);

  const q = params.get('q');
  const publisher = params.get('publisher');
  const year = params.get('year');
  const length_min = params.get('length_min');
  const length_max = params.get('length_max');
  const tags = params.getAll('tag');
  const owners = params.getAll('owner');
  const players = params.getAll('player');

  // default query sort is relevance, otherwise title
  const sort_by = params.get('sort_by') ?? (!!q ? 'r' : 't');

  const players_inc = params.getAll('players_inc');
  const players_min = players_inc.length > 0 ? Math.min(...players_inc) : params.get('players_min');
  const players_max = players_inc.length > 1 ? Math.max(...players_inc) : params.get('players_max');

  let players_range = $state(
    params.has('players_max') || params.has('players_min') ? 'exact' :
    'inclusive'
  );

  // publishers input

  let publishers_cache = null;
  let publisher_select = $state(publisher ? textToPublisher(publisher) : null);

  async function fetchPublishersContaining(s) {
    if (publishers_cache === null) {
      const url = new URL(`${gls_url}/publishers`);
      const result = (await fetchJSON(url)).publishers;

      publishers_cache = result
        .map(textToPublisher)
        .sort((a, b) => a.key.localeCompare(b.key));
    }

    if (!s) {
      return [];
    }

    s = s.toLowerCase();
    // FIXME: slow
    return publishers_cache.filter((t) => t.key.includes(s));
  }

  function publisherToText(p) {
    return p?.publisher ?? '';
  }

  function textToPublisher(p) {
    return { key: p.toLowerCase(), publisher: p };
  }

  const publishers_fetcher = new AutocompleteFetcher(
    (k) => undefined,
    (k, v) => {},
    fetchPublishersContaining
  );

  // tags chip input

  let tags_cache = null;
  let tags_select = $state(tags.map(textToTag));

  async function fetchTagsContaining(s) {
    if (tags_cache === null) {
      const url = new URL(`${gls_url}/tags`);
      const result = (await fetchJSON(url)).tags;

      tags_cache = result
        .map(textToTag)
        .sort((a, b) => a.key.localeCompare(b.key));
    }

    if (!s) {
      return [];
    }

    s = s.toLowerCase();
    // FIXME: slow
    return tags_cache.filter((t) => t.key.includes(s));
  }

  function tagToText(t) {
    return t?.tag;
  }

  function textToTag(t) {
    return { key: t.toLowerCase(), tag: t };
  }

  const tags_fetcher = new AutocompleteFetcher(
    (k) => undefined,
    (k, v) => {},
    fetchTagsContaining
  );

  // owners, players chip inputs

  const users_cache = new Map();
  let owners_select = $state(owners.map(textToUser));
  let players_select = $state(players.map(textToUser));

  async function fetchUsersStartingWith(prefix) {
    const url = new URL(`${ums_url}/users`);
    url.searchParams.append('term', prefix);
    url.searchParams.append('include_groups', false);
    url.searchParams.append('limit', 6);

    return (await fetchJSON(url)).users;
  }

  function userToText(u) {
    return u?.username;
  }

  function textToUser(u) {
    return { username: u };
  }

  const users_fetcher = new AutocompleteFetcher(
    (k) => users_cache.get(k),
    (k, v) => users_cache.set(k, v),
    fetchUsersStartingWith
  );

  function loadProjects(url) {
     fetchJSON(url)
      .then((result) => ({ projects, meta } = result))
      .catch((err) => error = err);
  }

  function fixupData(event) {
    cleanupSearch(event);

    const fdata = event.formData;

    if (publisher_select) {
      fdata.set('publisher', publisherToText(publisher_select));
    }

    for (const t of tags_select) {
      fdata.append('tag', tagToText(t));
    }

    for (const u of owners_select) {
      fdata.append('owner', userToText(u));
    }

    for (const u of players_select) {
      fdata.append('player', userToText(u));
    }

    if (fdata.get('players_min') === "0") {
      // don't send vacuous lower bounds
      fdata.delete('players_min');
    }

    if (fdata.get('length_min') === "0") {
      // don't send vacuous lower bounds
      fdata.delete('length_min');
    }

    if (fdata.get('players_range') === 'inclusive') {
      // inclusive players range uses players_inc for both bounds
      const pmin = fdata.get('players_min');
      if (pmin) {
        fdata.append('players_inc', pmin);
        fdata.delete('players_min');
      }

      const pmax = fdata.get('players_max');
      if (pmax) {
        fdata.append('players_inc', pmax);
        fdata.delete('players_max');
      }
    }

    fdata.delete('players_range');
  }

  function submitSearch(event) {
    event.preventDefault();
    event.stopPropagation();

    const fdata = new FormData(event.target);

    const url = new URL(window.location);
    url.search = '';

    for (const [k, v] of fdata.entries()) {
      if (v !== '') {
        url.searchParams.append(k, v);
      }
    }

    window.location.assign(url);
  }

  if (window.location.search !== '') {
    loadProjects(makeRequestURL(gls_url, params, limit));
  }
</script>

<style>

.advanced_search {
  max-width: 780px;
  margin: 0 auto;
}

</style>

<svelte:head>
  {#if q === null}
  <title>Module Library - Vassal</title>
  {:else}
  <title>Search Results for {q} - Module Library - Vassal</title>
  {/if}
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

<nav class="advanced_search">
  <form action="" onformdata={fixupData} onsubmit={submitSearch}>
    <div class="row">
      <div class="col mb-3">
        <label for="title_input" class="form-label">Full-text search</label>
        <input id="title_input" class="form-control" type="text" name="q" value={q} />
      </div>
    </div>
    <div class="row">
      <div class="col mb-3">
        <label for="publisher_input" class="form-label">Publisher</label>
        <AutocompleteInput fetcher={publishers_fetcher} itemToText={publisherToText} bind:value={publisher_select} />
      </div>
      <div class="col mb-3">
        <label for="year_input" class="form-label">Year</label>
        <input id="year_input" class="form-control" type="text" name="year" value={year} />
      </div>
    </div>
    <div class="row">
      <div class="col-4 mb-3">
        <label for="players_min_input" class="form-label">Number of players</label>
        <div class="row">
          <div class="col-5">
            <input id="players_min_input" class="form-control" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" name="players_min" value={players_min}  />
          </div>
          <div class="col-2 align-self-center">to</div>
          <div class="col-5">
            <input id="players_max_input" class="form-control" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" name="players_max" value={players_max} />
          </div>
        </div>
      </div>
      <div class="col-2 mb-3 d-flex flex-column justify-content-end">
        <div>
          <input id="players_range_radio_inclusive" class="form-check-input" type="radio" name="players_range" value="inclusive" bind:group={players_range} />
          <label class="form-check-label" for="players_range_radio_inclusive">Inclusive</label>
        </div>
        <div>
          <input id="players_range_radio_exact" class="form-check-input" type="radio" name="players_range" value="exact" bind:group={players_range} />
          <label class="form-check-label" for="players_range_radio_exact">Exact</label>
        </div>
      </div>
      <div class="col-4 mb-3">
        <label for="length_min_input" class="form-label">Length</label>
        <div class="row">
          <div class="col-5">
            <input id="length_min_input" class="form-control" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" name="length_min" value={length_min} />
          </div>
          <div class="col-2 align-self-center">to</div>
          <div class="col-5">
            <input id="length_max_input" class="form-control" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" name="length_max" value={length_max} />
          </div>
        </div>
      </div>
    </div>
    <div class="row">
      <div class="col mb-3">
        <label for="tags_input" class="form-label">Tags</label>
        <ChipInput fetcher={tags_fetcher} itemToText={tagToText} bind:items={tags_select} />
      </div>
    </div>
    <div class="row">
      <div class="col mb-3">
        <label for="owners_input" class="form-label">Project owners</label>
        <ChipInput fetcher={users_fetcher} itemToText={userToText} bind:items={owners_select} />
      </div>
    </div>
    <div class="row">
      <div class="col mb-3">
        <label for="players_input" class="form-label">Players</label>
        <ChipInput fetcher={users_fetcher} itemToText={userToText} bind:items={players_select} />
      </div>
    </div>
    <button type="submit" aria-label="Search" class="btn btn-primary"><svg class="svg-icon"><use xlink:href="#magnifying-glass"></use></svg> Search</button>
  </form>
</nav>

<SearchPageGuts {base_url} {gls_url} {sort_by} relevance={!!q} {limit} {loadProjects} bind:error={error} bind:meta={meta} bind:projects={projects} />
</main>

<div class="svg-sprites">
  <div class="fontawesome">
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="magnifying-glass" viewBox="0 0 640 640">
        <!--!Font Awesome Free v7.1.0 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2025 Fonticons, Inc.-->        <path d="M480 272C480 317.9 465.1 360.3 440 394.7L566.6 521.4C579.1 533.9 579.1 554.2 566.6 566.7C554.1 579.2 533.8 579.2 521.3 566.7L394.7 440C360.3 465.1 317.9 480 272 480C157.1 480 64 386.9 64 272C64 157.1 157.1 64 272 64C386.9 64 480 157.1 480 272zM272 416C351.5 416 416 351.5 416 272C416 192.5 351.5 128 272 128C192.5 128 128 192.5 128 272C128 351.5 192.5 416 272 416z"/>
      </symbol>
    </svg>
  </div>
</div>

<Footer/>
