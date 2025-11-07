<script>
  import { formatDistance } from './lib/util.js';

  import ErrorBox from './ErrorBox.svelte';
  import UserChip from './UserChip.svelte';
  import ProjectEditor from './ProjectEditor.svelte';
 
  let {
    ums_url,
    proj = $bindable(),
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

  //
  // edit mode
  //

  let edit = $state(false);
  let error = $state(null);

  function startEdit(event) {
    edit = true;
    editing = true;
  }

  function cancelEdit(event) {
    edit = false;
    editing = false;
    error = null;
  }

  async function submitEdit(event) {
    event.preventDefault();
    event.stopPropagation();

    const fdata = new FormData(event.target);

    const cur_owners = new Set(fdata.getAll('owner'));
    const prev_owners = new Set(proj.owners);
    const to_add = [...[...cur_owners.values()].filter((u) => !prev_owners.has(u))];
    const to_remove = [...[...prev_owners.values()].filter((u) => !cur_owners.has(u))];

    try {
      if (to_add) {
        await client.addOwners(to_add);
      }

      if (to_remove) {
        await client.removeOwners(to_remove);
      }
      error = null;
    }
    catch (err) {
      error = err;
      return;
    }

    // update the project data
    try {
      proj = await client.getProject();
      error = null;
    }
    catch (err) {
      error = err;
      return;
    }

    edit = false;
    editing = false;
  }

</script>

{#if error}
<ErrorBox {error} />
{/if}
<div class="my-2 pb-2">
  <h2>
    <svg class="svg-icon"><use xlink:href="#person-digging"></use></svg>
    Project
    <button class="edit_button" class:is_editable={!editing && user_is_owner()}  type="button" aria-label="Edit" onclick={startEdit}>
      <svg class="svg-icon edit_icon"><use xlink:href="#pencil"></use></svg>
    </button>
  </h2>
{#if edit}
  <ProjectEditor {ums_url} {proj} {submitEdit} {cancelEdit} />
{:else}
  {@const now = new Date()}
  <div>
    <div class="px-3 pt-2 pb-2 border rounded d-flex flex-wrap gap-2">
      <div>
        <div class="d-flex align-items-center gap-1">
          <svg class="svg-icon"><use xlink:href="#id-badge"></use></svg>
          {proj.name}
        </div>
        <div class="d-flex align-items-center gap-1">
          <div>
            <svg class="svg-icon"><use xlink:href="#user"></use></svg>
            Owners
          </div>
          <ul class="d-flex flex-wrap list-unstyled m-0 gap-1">
          {#each proj.owners as owner}
            <li><UserChip {ums_url} username={owner} size=24 /></li>
          {/each}
          </ul>
        </div>
      </div>
      <div class="ms-auto">
        <div>
          <time datetime={proj.modified_at} title={proj.modified_at}>
            <svg class="svg-icon"><use xlink:href="#arrows-rotate"></use></svg>
            Updated {formatDistance(rtf, new Date(proj.modified_at), now)}
          </time>
        </div>
        <div>
          <time datetime={proj.created_at} title={proj.created_at}>
            <svg class="svg-icon"><use xlink:href="#star"></use></svg>
            Created {formatDistance(rtf, new Date(proj.created_at), now)}
          </time>
        </div>
      </div>
    </div>
  </div>
{/if}
</div>
