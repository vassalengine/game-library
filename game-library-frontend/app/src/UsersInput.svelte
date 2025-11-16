<script>
  import AutocompleteFetcher from './lib/search.js';

  import ChipInput from './ChipInput.svelte';

  let {
    client, 
    users,
    items = $bindable(),
    cache = $bindable()
  } = $props();

  items = users.map(textToUser);

  async function fetchUsersStartingWith(prefix) {
    return (await client.getUsersStartingWith(prefix)).users;
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
