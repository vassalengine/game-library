<script>
  import { SortableList } from '@jhubbardsf/svelte-sortablejs';

  import GalleryEditorItem from './GalleryEditorItem.svelte';

  let {
    proj = $bindable(),
    client,
    md,
    submitEdit,
    cancelEdit,
    submitImage,
    changes = $bindable()
  } = $props();

  let gallery_edit = $derived([...proj.gallery]);

  function deleteItem(event) {
    // items have their id stashed in a hidden input
    const del_el = event.target
      .closest("figure")
      .querySelector("input[name='id']")

    const del_id = parseInt(del_el.value);

    changes.push({ op: 'delete', id: parseInt(del_id) });

    const del_idx = gallery_edit.findIndex((g) => g.id === del_id);
    gallery_edit.splice(del_idx, 1);
    // reassigning forces an update
    gallery_edit = [...gallery_edit];
  }

  function moveItem(event) {
    // get the id of the mover
    const id_input = event.item.querySelector('input[name="id"]');
    const id = parseInt(id_input.value);

    // get the id of the item after the mover
    const next_item = event.item.nextElementSibling;
    const next_id_input = next_item.querySelector('input[name="id"]');
    const next_id = next_id_input ? parseInt(next_id_input.value) : null;

    changes.push({
      op: 'move',
      id: id,
      next: next_id
    });
  }

</script>

<style>

figure:hover #new_image_label {
  transition: filter 0.15s ease-in-out;
  filter: brightness(0.75);
}

#new_image_figure {
  pointer-events: auto;
}

#new_image_input {
  display: none;
}

#new_image_label {
  display: inline-flex;
  position: relative;
  width: 8em;
  height: 8em;
}

#new_image_add_overlay {
  position: absolute;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
}

#new_image_add_overlay svg {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 25%;
  height: auto;
}

</style>

<form action="" onsubmit={submitEdit}>
  <SortableList id="image_list" class="d-flex flex-wrap align-items-center justify-content-evenly" animation={150} draggable=".draggable" onUpdate={moveItem}>
  {#each gallery_edit as img (img.id)}
    {@const src = client.imageUrl(img.filename)}
    <GalleryEditorItem {img} {src} {deleteItem} />
  {/each}
    <figure id="new_image_figure" class="figure col-lg-3 col-md-4 col-6 px-1">
      <label id="new_image_label" for="new_image_input" class="figure-img img-fluid img-thumbnail">
        <div id="new_image_add_overlay">
          <svg><use xlink:href="#plus"></use></svg>
        </div>
      </label>
      <input id="new_image_input" type="file" accept="image/png, image/jpeg, image/svg+xml, image/webp, image/avif" onchange={submitImage}>
    </figure>
  </SortableList>

  <button type="submit" aria-label="Submit" class="btn btn-primary"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
  <button type="button" aria-label="Cancel" class="btn btn-primary" onclick={cancelEdit}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
</form>
