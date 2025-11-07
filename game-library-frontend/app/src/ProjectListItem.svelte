<script>
  import { formatDistance, truncateString } from './lib/util.js';

  let {
    base_url,
    proj,
    rtf,
    now
  } = $props();
</script>

<li class="my-1">
  <div class="p-2 border rounded d-flex flex-wrap">
    <div class="me-auto">
      <div>
        <a class="fs-5 fw-bolder" href="{`${base_url}/projects/${proj.slug}`}">{proj.game.title}</a>
      </div>
      <div>{proj.name}</div>
      <div>{truncateString(proj.description, 256)}</div>
    </div>
    <div class="ms-auto">
<!-- TODO: add tags -->
<!--
      <div>
        <svg class="svg-icon"><use xlink:href="#arrow-down-to-bracket"></use></svg>
        All-Time:
      </div>
      <div>
        <svg class="svg-icon"><use xlink:href="#arrow-down-to-bracket"></use></svg>
        Recent:
      </div>
-->
      <div>
        <svg class="svg-icon"><use xlink:href="#arrows-rotate"></use></svg>
        Updated {formatDistance(rtf, new Date(proj.modified_at), now)}
      </div>
    </div>
  </div>
</li>
