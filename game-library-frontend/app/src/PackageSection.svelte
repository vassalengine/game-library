<script>
  import ErrorBox from './ErrorBox.svelte';
  import PackageEditor from './PackageEditor.svelte';
  import ReleaseSection from './ReleaseSection.svelte';


  let {
    ums_url,
    proj = $bindable(),
    pkg,
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  let error = $state(null);

  //
  // package update
  //

  let editPackage = $state(false);

  function startEditPackage(event) {
    editPackage = true;
    editing = true;
  }

  function cancelEditPackage(event) {
    editPackage = false;
    editing = false;
  }

  async function submitEditPackage(event) {
    event.preventDefault();

    const fd = new FormData(event.target);

    const data = {
      'name': fd.get('package_name')?.trim(),
      'sort_key': Number(fd.get('sort_key'))
    };

    try {
      await client.updatePackage(pkg.slug, data);
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

    editPackage = false;
    editing = false;
  }

  //
  // package removal
  //

  async function deletePackage(event) {
    try {
      await client.removePackage(pkg.slug);
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
  }

  //
  // create release
  //

  let editRelease = $state(false);

  let release_versions;

  let validateStrict;

  async function startRelease(event) {
    editRelease = true;
    editing = true;

    release_versions = new Set(pkg.releases.map((r) => r.version));

    const module = await import("https://cdn.jsdelivr.net/npm/compare-versions@6.1.1/+esm");
    validateStrict = module.validateStrict;
  }

  function cancelRelease(event) {
    editRelease = false;
    editing = false;
  }

  function validateReleaseVersion(event) {
    let msg = "";
    if (release_versions.has(event.target.value)) {
      msg = "Release already exists";
    }
    else if (!validateStrict(event.target.value)) {
      msg = "Invalid version";
    }

    event.target.setCustomValidity(msg);
  }

  async function submitRelease(event) {
    event.preventDefault();

    const fdata = new FormData(event.target);

    const release_version = fdata.get('release_version');

    try {
      await client.addRelease(
        pkg.slug,
        release_version
      );
      error = null;
    }
    catch (err) {
// TODO: Release needs different error var from package
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

    editRelease = false;
    editing = false;
  }
</script>

<style>
/* Release accordion */

summary {
  list-style: none;
  display: flex;
  align-items: center;
}

summary::before {
  content: '';
  width: 1em;
  height: 0.5em;
  background: url('assets/chevron-down.svg');
  background-size: cover;
  margin-right: 0.5em;
  transition: 0.2s;
}

details[open] > summary::before {
  transform: rotate(180deg);
}
</style>

{#if error}
<ErrorBox {error} />
{/if}
<div class="border rounded p-3 my-2">
  <h3>
    <svg class="svg-icon"><use xlink:href="#cube"></use></svg>
    {pkg.name}
    <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Add" onclick={startRelease}>
      <svg class="svg-icon edit_icon"><use xlink:href="#plus"></use></svg>
    </button>
    <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Edit" onclick={startEditPackage}>
      <svg class="svg-icon edit_icon"><use xlink:href="#pencil"></use></svg>
    </button>
    <button class="delete_button" class:is_deletable={!editing && user_is_owner() && pkg.releases.length == 0} type="button" aria-label="Delete" onclick={deletePackage}>
      <svg class="svg-icon delete_icon"><use xlink:href="#trash-can"></use></svg>
    </button>
  </h3>
  <div>{pkg.description}</div>
  {#if editPackage}
    <div class="package_tmpl_top border rounded p-3 my-2">
      <form action="" onsubmit={submitEditPackage}>
        <PackageEditor {pkg} packages={proj.packages} />
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="submit" aria-label="Submit"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="button" aria-label="Cancel" onclick={cancelEditPackage}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
      </form>
    </div>
  {/if}
  <ol class="list-unstyled">
    {#if editRelease}
    <li class="release_tmpl_top border rounded p-3 my-2">
      <form action="" onsubmit={submitRelease}>
        <input id="release_version_input" class="release_tmpl_name form-control" type="text" name="release_version" required oninput={validateReleaseVersion}>
<!--
        <input id="release_file_input" class="release_tmpl_name form-control" type="file" name="release_file" required>
-->
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="submit" aria-label="Submit"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="button" aria-label="Cancel" onclick={cancelRelease}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
      </form>
    </li>
    {/if}
    {#if pkg.releases.length > 0}
    <li class="d-flex flex-wrap align-items-start p-1 my-2 gap-2">
      <ReleaseSection bind:proj={proj} {pkg} release={pkg.releases[0]} {client} current {username} {ums_url} bind:editing={editing} />
    </li>
    {/if}
  </ol>
  {#if pkg.releases.length > 1}
  <details>
    <summary>Older releases...</summary>
    <ol class="list-unstyled">
      {#each pkg.releases.slice(1) as release}
      <li class="d-flex flex-wrap align-items-start p-1 my-2 gap-2">
        <ReleaseSection bind:proj={proj} {pkg} {release} {client} {username} {ums_url} bind:editing={editing} />
      </li>
      {/each}
    </ol>
  </details>
  {/if}
</div>
