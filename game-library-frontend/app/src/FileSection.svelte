<script>
  import { formatDistance, formatSizeWithUnit } from './lib/util.js';
  import UserChip from './UserChip.svelte';


  let { ums_url, file } = $props();

  const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

  function formatRequires(r) {
    return r.replace('>=', '≥').replace('<=', '≤');
  }
</script>

<style>

</style>

<div><a href={file.url}>{file.filename}</a></div>
<div class="d-flex flex-wrap gap-2">
  <div>
    By <UserChip {ums_url} username={file.published_by} size=24 />
  </div>
  <div>
    <svg class="svg-icon"><use xlink:href="#weight-hanging"></use></svg>
    {formatSizeWithUnit(file.size)}
  </div>
{#if file.requires}
  <div>
    <svg class="svg-icon"><use xlink:href="#vassal-bw"></use></svg>
    {formatRequires(file.requires)}
  </div>
{/if}
  <time datetime={file.published_at} title={file.published_at}>
    <svg class="svg-icon"><use xlink:href="#calendar-days"></use></svg>
    {formatDistance(rtf, new Date(file.published_at), new Date())}
  </time>
</div>
