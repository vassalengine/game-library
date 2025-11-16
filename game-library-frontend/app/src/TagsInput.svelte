<script>
  import AutocompleteFetcher from './lib/search.js';

  import ChipInput from './ChipInput.svelte';

  let {
    client,
    tags,
    items = $bindable(),
    cache = $bindable()
  } = $props();

  items = tags.map(textToTag);

  async function fetchTagsContaining(s) {
    if (cache === null) {
      const result = (await client.getTags()).tags;

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
