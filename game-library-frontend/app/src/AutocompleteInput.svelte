<script>
  import Autocomplete from '@smui-extra/autocomplete';

  let {
    fetcher,
    itemToText,
    value = $bindable(),
  } = $props();

</script>

<style>

@import 'https://cdn.jsdelivr.net/npm/svelte-material-ui@8.0.3/bare.min.css';

.autocomplete-outer :global(.smui-text-field--standard),
.autocomplete-outer :global(.smui-text-field--standard)::before {
  height: auto;
}

.autocomplete-outer :global(.mdc-line-ripple::before),
.autocomplete-outer :global(.mdc-line-ripple::after) {
  display: none;
}

.autocomplete-outer :global(.smui-autocomplete > div:focus) {
  outline: none;
}

.autocomplete-outer:focus-within {
  /* copied from bootstrap form-control:focus */
  color: var(--bs-body-color);
  background-color: var(--bs-body-bg);
  border-color: #86b7fe;
  outline: 0;
  box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
}

</style>

<div class="form-control autocomplete-outer">
  <Autocomplete
    getOptionLabel={itemToText}
    search={(k) => fetcher.searchItems(k)}
    bind:value
    showMenuWithNoInput={false}
    style="width: 100%;"
    textfield$style="width: 100%;"
  >
    {#snippet loading()}
      <div class="spinner-border" role="status">
<!--      <span class="sr-only">Loading...</span> -->
      </div>
    {/snippet}
  </Autocomplete>
</div>
