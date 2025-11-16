<script>
  import { fetchJSON } from './lib/client.js';
  import AutocompleteFetcher from './lib/search.js';

  import ChipInput from './ChipInput.svelte';

  let {
    gls_url,
    tags,
    items = $bindable() 
  } = $props();

  let cache = null;
  items = tags.map(textToTag);

  async function fetchTagsContaining(s) {
    if (cache === null) {
      const url = new URL(`${gls_url}/tags`);
      const result = (await fetchJSON(url)).tags;

      cache = result
        .map(textToTag)
        .sort((a, b) => a.key.localeCompare(b.key));
    }

    if (!s) {
      return [];
    }

    s = s.toLowerCase();
    // FIXME: slow
    return cache.filter((t) => t.key.includes(s));
  }

  function itemToText(t) {
    return t?.tag;
  }

  function textToTag(t) {
    return { key: t.toLowerCase(), tag: t };
  }

  const fetcher = new AutocompleteFetcher(
    (k) => undefined,
    (k, v) => {},
    fetchTagsContaining
  );

</script>

<ChipInput {fetcher} {itemToText} bind:items={items} />
