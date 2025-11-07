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
  let publishers = $state(null);

  function publisherQueryURL(base_url, publisher) {
    const req_url = new URL(`${base_url}/projects`);
    req_url.searchParams.append('publisher', publisher);
    return req_url;
  }

  function loadPublishers(url) {
     fetchJSON(url)
      .then((result) => ({ publishers } = result))
      .catch((err) => error = err);
  }

  loadPublishers(new URL(`${gls_url}/publishers`));

</script>

<svelte:head>
  <title>Publishers - Module Library - Vassal</title>
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

<SearchBar {base_url} />

<div class="my-1 p-3 bg-light rounded">
  <h1 class="m-0">Publishers</h1>
</div>

{#if error}
<ErrorBox {error} />
{/if}

{#if publishers}
<ol id="publishers_list" class="list-unstyled m-0 p-0">
  {#each publishers as publisher}
  <li><a href="{publisherQueryURL(base_url, publisher)}">{publisher}</a></li>
  {/each}
</ol>
{/if}

</main>

<Footer/>
