<script>
  import { fetchJSON } from './lib/client.js';
  import AutocompleteFetcher from './lib/search.js';

  import ChipInput from './ChipInput.svelte';

  let {
    ums_url,
    users,
    items = $bindable(),
    cache = $bindable() 
  } = $props();

  items = users.map(textToUser);

  async function fetchUsersStartingWith(prefix) {
    const url = new URL(`${ums_url}/users`);
    url.searchParams.append('term', prefix);
    url.searchParams.append('include_groups', false);
    url.searchParams.append('limit', 6);

    return (await fetchJSON(url)).users;
  }

  function itemToText(u) {
    return u?.username;
  }

  function textToUser(u) {
    return { username: u };
  }

  const fetcher = new AutocompleteFetcher(
    (k) => cache.get(k),
    (k, v) => cache.set(k, v),
    fetchUsersStartingWith
  );
</script>

<ChipInput {fetcher} {itemToText} bind:items={items} />
