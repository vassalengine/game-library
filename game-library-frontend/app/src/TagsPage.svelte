<script>
  import { fetchJSON } from './lib/client.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import SearchBar from './SearchBar.svelte';
  import ErrorBox from './ErrorBox.svelte';

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
  let tags = $state(null);

  function tagQueryURL(base_url, tag) {
    const req_url = new URL(`${base_url}/projects`);
    req_url.searchParams.append('tag', tag);
    return req_url;
  }

  function loadTags(url) {
     fetchJSON(url)
      .then((result) => ({ tags } = result))
      .catch((err) => error = err);
  }

  loadTags(new URL(`${gls_url}/tags`));

</script>

<svelte:head>
  <title>Tags - Module Library - Vassal</title>
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

<SearchBar {base_url} />

<div class="my-1 p-3 bg-light rounded">
  <h1 class="m-0">Tags</h1>
</div>

{#if error}
<ErrorBox {error} />
{/if}

{#if tags}
<ol id="tags_list" class="list-unstyled m-0 p-0">
  {#each tags as tag}
  <li><a href="{tagQueryURL(base_url, tag)}">{tag}</a></li>
  {/each}
</ol>
{/if}

</main>

<Footer/>
