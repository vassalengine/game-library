<script>
  import ErrorBox from './ErrorBox.svelte';
  import PackageSection from './PackageSection.svelte';
  import PackageEditor from './PackageEditor.svelte';

  import { slugFor } from './lib/util.js';


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

    const fd = new FormData(event.target);

    const name = fd.get('package_name')?.trim();
    const slug = slugFor(name);

    const data = {
      'name': name,
      'sort_key': Number(fd.get('sort_key')),
      'description': ''
    };

    try {
      await client.addPackage(slug, data);
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
<div>
  <h2>
    <svg class="svg-icon"><use xlink:href="#cubes"></use></svg>
    Packages
    <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Edit" onclick={startEdit}>
      <svg class="svg-icon edit_icon"><use xlink:href="#plus"></use></svg>
    </button>
  </h2>
  <div>
  {#if edit}
    <div class="package_tmpl_top border rounded p-3 my-2">
      <form action="" onsubmit={submitEdit}>
        <PackageEditor packages={proj.packages} />
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="submit" aria-label="Submit"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="button" aria-label="Cancel" onclick={cancelEdit}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
      </form>
    </div>
  {/if}
  {#each proj.packages as pkg}
    <PackageSection bind:proj={proj} {pkg} {client} {username} {ums_url} bind:editing={editing} />
  {/each}
  </div>
</div>
