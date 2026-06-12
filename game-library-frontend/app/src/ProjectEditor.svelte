<script>
  import UsersInput from './UsersInput.svelte';

  let {
    proj,
    client,
    submitEdit,
    cancelEdit
  } = $props();

  let users_cache = $state(new Map());
  let owners_select = $state([]);

  function fixupData(event) {
    const fdata = event.formData;

    for (const u of owners_select) {
      fdata.append('owner', u?.username);
    }
  }
</script>

<div class="container">
  <div class="row">
    <form action="" onsubmit={submitEdit} onformdata={fixupData}>
      <label for="owners_input" class="form-label">
        Owners
        <span title="Add or remove owners of a project.  Project owners can ediit the project, add new packages, releases, and upload files to the project. Only owners may add or remove other owners.">
           <svg class="svg-icon">
            <use xlink:href="#circle-question"></use>
          </svg>
        </span>
      </label>
      <UsersInput {client} users={proj.owners} bind:items={owners_select} bind:cache={users_cache} />
      <button type="submit" aria-label="Submit" class="btn btn-primary"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
      <button type="button" aria-label="Cancel" class="btn btn-primary" onclick={cancelEdit}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
    </form>
  </div>
</div>
