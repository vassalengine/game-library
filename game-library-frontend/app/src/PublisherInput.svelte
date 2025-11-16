<script>
  import AutocompleteFetcher from './lib/search.js';

  import AutocompleteInput from './AutocompleteInput.svelte';

  let {
    client,
    publisher,
    item = $bindable(),
    cache = $bindable()
  } = $props();

  item = publisher ? textToPublisher(publisher) : null;

  async function fetchPublishersContaining(s) {
    if (cache === null) {
      const result = (await client.getPublishers()).publishers;

      cache = result
        .map(textToPublisher)
        .sort((a, b) => a.key.localeCompare(b.key));
    }

    if (!s) {
      return [];
    }

    s = s.toLowerCase();
    // FIXME: slow
    return cache.filter((t) => t.key.includes(s));
  }

  function itemToText(p) {
    return p?.publisher ?? '';
  }

  function textToPublisher(p) {
    return { key: p.toLowerCase(), publisher: p };
  }

  const fetcher = new AutocompleteFetcher(
    (k) => undefined,
    (k, v) => {},
    fetchPublishersContaining
  );
</script>

<AutocompleteInput {fetcher} {itemToText} bind:value={item} />

