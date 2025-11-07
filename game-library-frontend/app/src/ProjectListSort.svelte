<script>
  let {
    sort_by,
    relevance,
    updateSort
  } = $props();
</script>

<div class="container mx-0 my-2">
  <div class="row">
    <div class="col-auto ms-auto pe-0">
      <label for="sort_selector">Sort by</label>
      <select name="sort_by" id="sort_selector" value={sort_by} onchange={updateSort}>
        {#if relevance}
        <option value="r">Relevance</option>
        {/if}
        <option value="t">Alphabetical</option>
        <option value="m">Recent Updates</option>
        <option value="c">Newly Added</option>
      </select>
    </div>
  </div>
</div>
