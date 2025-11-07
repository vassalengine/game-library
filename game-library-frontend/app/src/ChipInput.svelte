<script>
  import ChipInput from '@smui-extra/chip-input';

  let {
    fetcher,
    itemToText,
    items = $bindable()
  } = $props();

</script>

<style>

@import 'https://cdn.jsdelivr.net/npm/svelte-material-ui@8.0.3/bare.min.css';

.item-chip-outer :global(.smui-text-field--standard),
.item-chip-outer :global(.smui-text-field--standard)::before {
  height: auto;
}

.item-chip-outer :global(.mdc-line-ripple::before),
.item-chip-outer :global(.mdc-line-ripple::after) {
  display: none;
}

.item-chip-outer :global(.smui-chip-input__autocomplete > div:focus) {
  outline: none;
}

.item-chip-outer:focus-within {
  /* copied from bootstrap form-control:focus */
  color: var(--bs-body-color);
  background-color: var(--bs-body-bg);
  border-color: #86b7fe;
  outline: 0;
  box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
}

</style>

<div class="form-control item-chip-outer">
  <ChipInput
    bind:chips={items}
    key={itemToText}
    getChipLabel={itemToText}
    getChipText={itemToText}
    autocomplete$search={(k) => fetcher.searchItems(k)}
    autocomplete$getOptionLabel={itemToText}
    chipTrailingAction$aria-label="Remove element"
  >
    {#snippet loading()}
      <div class="spinner-border" role="status">
  <!--      <span class="sr-only">Loading...</span> -->
      </div>
    {/snippet}
    {#snippet chipTrailingAction()}
      <svg class="svg-icon"><use xlink:href="#circle-xmark"></use></svg>
    {/snippet}
  </ChipInput>
</div>

<!-- TODO: Move this somewhere so it doesn't get included more than once -->
<div class="svg-sprites">
  <div class="fontawesome">
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="circle-xmark" viewBox="0 0 640 640">
        <!--!Font Awesome Free v7.1.0 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2025 Fonticons, Inc.-->
        <path d="M320 576C461.4 576 576 461.4 576 320C576 178.6 461.4 64 320 64C178.6 64 64 178.6 64 320C64 461.4 178.6 576 320 576zM231 231C240.4 221.6 255.6 221.6 264.9 231L319.9 286L374.9 231C384.3 221.6 399.5 221.6 408.8 231C418.1 240.4 418.2 255.6 408.8 264.9L353.8 319.9L408.8 374.9C418.2 384.3 418.2 399.5 408.8 408.8C399.4 418.1 384.2 418.2 374.9 408.8L319.9 353.8L264.9 408.8C255.5 418.2 240.3 418.2 231 408.8C221.7 399.4 221.6 384.2 231 374.9L286 319.9L231 264.9C221.6 255.5 221.6 240.3 231 231z"/>
      </symbol>
    </svg>
  </div>
</div>
