<script>
  import { fetchJSON } from './lib/client.js';
  import AutocompleteFetcher from './lib/search.js';

  import ChipInput from './ChipInput.svelte';

  let {
    ums_url,
    proj,
    submitEdit,
    cancelEdit
  } = $props();

  const users_cache = new Map();
  let owners_select = $state(proj.owners.map(textToUser));

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

  function fixupData(event) {
    const fdata = event.formData;

    for (const u of owners_select) {
      fdata.append('owner', userToText(u));
    }
  }
</script>

<div class="container">
  <div class="row">
    <form action="" onsubmit={submitEdit} onformdata={fixupData}>
      <label for="owners_input" class="form-label">Owners</label>
      <ChipInput fetcher={users_fetcher} itemToText={userToText} bind:items={owners_select} />
      <button type="submit" aria-label="Submit" class="btn btn-primary"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
      <button type="button" aria-label="Cancel" class="btn btn-primary" onclick={cancelEdit}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
    </form>
  </div>
</div>
