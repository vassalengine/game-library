<script>
  import Client from './lib/client.js';

  import ErrorBox from './ErrorBox.svelte';
  import FileSection from './FileSection.svelte';

  let {
    ums_url,
    proj = $bindable(),
    pkg,
    release,
    client,
    current = false,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  function extPriority(filename) {
    if (filename.endsWith('.vmod')) {
      return 0;
    }
    else if (filename.endsWith('.vmdx')) {
      return 1;
    }
    else if (filename.endsWith('.vsav')) {
      return 2;
    }
    else {
      return 3;
    }
  }

  function sortFiles(files) {
    return files.toSorted((a, b) => {
      return (extPriority(a.filename) - extPriority(b.filename)) ||
        a.filename.localeCompare(b.filename);
    });
  }

  //
  // release removal
  //

  async function deleteRelease(event) {
    try {
      await client.removeRelease(pkg.slug, release.version);
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
  // edit mode
  //

  let edit = $state(false);
  let error = $state(null);

  let uploading = $state(false);
  let uploadProgress = $state(0);
  let uploadFilename = $state('');

  let extractVersion;
  let validateStrict;

  async function startFile(event) {
    edit = true;
    editing = true;

    const module = await import("./lib/module.js");
    extractVersion = module.extractVersion;

    const compare = await import("https://cdn.jsdelivr.net/npm/compare-versions@6.1.1/+esm");
    validateStrict = compare.validateStrict;
  }

  function cancelFile(event) {
    edit = false;
    editing = false;
    error = null;
  }

  function extension(s, delim) {
    const i = s.lastIndexOf('.');
    return i != -1 ? s.substr(i + 1) : '';
  }

  async function validateFile(event) {
    let msg = "";

    if (event.target.files.length === 1) {
      const file = event.target.files[0];

      const ext = extension(file.name);

      const version = await extractVersion(file);
      if (version !== null) {
        // we found moduledata

        if (version !== release.version) {
          let vtype = null;

          switch (ext) {
          case 'vmod':
            vtype = 'Module';
            break;
          case 'vsav':
            vtype = 'Save';
            break;
          case 'vlog':
            vtype = 'Log';
            break;
          }

          if (vtype !== null) {
            // modules, saves, logs must match the version of their release
            msg = `${vtype} version ${version} does not equal release version ${release.version}.`;
          }
        }
      }
      else {
        // we found no moduledata
        // this is an error for modules, extensions, logs, saves

        let vtype = null;

        switch (ext) {
        case 'vmod':
          vtype = 'module';
          break;
        case 'vmdx':
          vtype = 'extension';
          break;
        case 'vsav':
          vtype = 'save';
          break;
        case 'vlog':
          vtype = 'log';
          break;
        }

        if (vtype !== null) {
          // this is a vassal type, should have moduledata
          msg = `No version found in ${vtype} file.`;
        }
      }
    }

    event.target.setCustomValidity(msg);

    if (msg) {
      error = msg;
    }
  }

  let cancelUpload = $state(() => {});

  async function submitFile(event) {
    event.preventDefault();

    const fdata = new FormData(event.target);
    const file = fdata.get('file');

    uploadFilename = file.name;
    uploadProgress = 0;

    try {
      uploading = true;

      document.documentElement.style.cursor = 'wait';

      const callbacks = {
        progress: (e) => {
          if (e.lengthComputable) {
            uploadProgress = Math.floor((e.loaded / e.total) * 100);
          }
        }
      };

      try {
        const [xhr, promise] = await client.addFile(
          pkg.slug,
          release.version,
          file,
          file.type,
          callbacks
        );

        cancelUpload = () => xhr.abort();

        const result = await promise;
        error = null;

        switch (result) {
          case Client.UPLOAD_OK:
            break;
          case Client.UPLOAD_ABORTED:
            return;
        }
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
    finally {
      uploading = false;
      document.documentElement.style.cursor = 'pointer';
    }

    edit = false;
    editing = false;
  }

</script>

<style>

.upload_progress {
  appearance: none;
  height: 1em;
}

/* Release pills */

.current_release {
  background-color: #008800;
}

.release {
  background-color: #bbbbbb;
}
</style>

<div>
  <div class="badge rounded-pill fs-5" class:current_release={current} class:release={!current}>{release.version}</div>
  <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Add" onclick={startFile}>
    <svg class="svg-icon edit_icon"><use xlink:href="#plus"></use></svg>
  </button>
  <button class="delete_button" class:is_deletable={!editing && user_is_owner() && release.files.length == 0} type="button" aria-label="Delete" onclick={deleteRelease}>
      <svg class="svg-icon delete_icon"><use xlink:href="#trash-can"></use></svg>
  </button>
</div>
{#if release.files.length > 0 || edit}
<ul class="list-unstyled">
  {#if edit}
  <li class="mb-2">
    {#if error}
    <ErrorBox {error} />
    {/if}
    <form action="" onsubmit={submitFile}>
      <input id="file_input" class="form-control" type="file" name="file" required style:display={uploading ? 'none' : 'inline'} onchange={validateFile}>
      {#if !uploading}
      <button class="btn btn-primary p-1 mx-1 rounded-0" aria-label="Submit" type="submit"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
      <button class="btn btn-primary p-1 mx-1 rounded-0" type="button" aria-label="Cancel" onclick={cancelFile}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
      {:else}
      <div>{uploadFilename}</div>
      <div class="d-flex align-items-center">
        <button class="btn btn-primary p-1 mx-1 rounded-0" type="button" aria-label="Cancel" onclick={cancelUpload}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
        <progress class="upload_progress text-primary flex-fill w-auto" value={uploadProgress} max="100"></progress>
      </div>
      {/if}
    </form>
  </li>
  {/if}
  {#each sortFiles(release.files) as file}
  <li class="mb-2">
    <FileSection {file} {ums_url} />
  </li>
  {/each}
</ul>
{/if}
