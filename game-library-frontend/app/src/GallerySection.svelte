<script>
  import markdownIt from 'https://cdn.jsdelivr.net/npm/markdown-it@14.1.0/+esm';

  import Client from './lib/client.js';

  import ErrorBox from './ErrorBox.svelte';
  import GalleryItem from './GalleryItem.svelte';
  import GalleryEditor from './GalleryEditor.svelte';

  let {
    proj = $bindable(),
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  function mdInit() {
    const defaults = {
      html: false,
      xhtmlOut: false,
      breaks: false,
      langPrefix: 'language-',
      linkify: true,
      typographer: true,
  //    highlight: doHighlight
    };

    const md = markdownIt(defaults);

    return md;
  }

  const md = mdInit();

  //
  // edit mode
  //

  let edit = $state(false);
  let error = $state(null);
  let changes = $state([]);

  function startEdit(event) {
    edit = true;
    editing = true;
    changes = [];
  }

  function cancelEdit(event) {
    edit = false;
    editing = false;
    changes = [];
    error = null;
  }

  async function submitEdit(event) {
    event.preventDefault();
    event.stopPropagation();

    /*
      Operations:

      { op: 'update', id: item.id, description: item.description }
      { op: 'delete', id: item.id }
      { op: 'move', id: item.id, next: item.next_id }

    */

    const fdata = new FormData(event.target);

    const ids = fdata.getAll('id').map((i) => parseInt(i));
    const descriptions = fdata.getAll('description').map((d) => d.trim());

    // make an id map for original items
    const orig_pos = proj.gallery.reduce(
      (prev, cur, i) => ({...prev, [cur.id]: i}), {}
    );

    // find all updated items
    for (let i = 0, j = 0; i < ids.length; ++i) {
      // check if the description changed
      const opos = orig_pos[ids[i]];
      if (descriptions[i] !== proj.gallery[opos].description) {
          changes.push({
            op: 'update',
            id: ids[i],
            description: descriptions[i]
          });
      }
    }

    if (changes.length == 0) {
      edit = false;
      editing = false;
      changes = [];
      return;
    }

    // update the gallery
    try {
      await client.updateGallery({ ops: changes });
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
    changes = [];
  }

  function is_single_image(files) {
    return files.length == 1 && files[0].type.startsWith('image/');
  }

  async function submitImage(event) {
    if (!is_single_image(event.target.files)) {
      return;
    }

    const file = event.target.files[0];

    try {
      const callbacks = {
        progress: (e) => {
          if (e.lengthComputable) {
//              uploadProgress = Math.floor((e.loaded / e.total) * 100);
            console.log(Math.floor((e.loaded / e.total) * 100));
          }
        }
      };

      const [_, promise] = await client.addGalleryImage(
        file.name,
        file,
        file.type,
        callbacks
      );

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

</script>

{#if error}
<ErrorBox {error} />
{/if}
<button class="edit_button fs-2" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Edit" onclick={startEdit}>
  <svg class="svg-icon edit_icon"><use xlink:href="#pencil"></use></svg>
</button>

{#if edit}
<GalleryEditor {proj} {client} {md} {submitEdit} {cancelEdit} {submitImage} {changes} />
{:else}
<div class="d-flex flex-wrap align-items-center justify-content-evenly">
{#each proj.gallery as img (img.id)}
  {@const src = client.imageUrl(img.filename)}
  <GalleryItem {img} {src} {md} />
{/each}
</div>
{/if}
