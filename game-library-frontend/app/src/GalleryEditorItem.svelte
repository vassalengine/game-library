<script>
  let {
    img,
    src,
    deleteItem
  } = $props();
</script>

<style>

.draggable {
  cursor: grab;
}

.image_inner {
  display: inline-flex;
  position: relative;
  pointer-events: none;
}

.image_inner:hover img {
  transition: filter 0.15s ease-in-out;
  filter: brightness(0.75);
}

.image_delete_overlay {
  position: absolute;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  pointer-events: auto;
}

.image_delete_overlay button {
  position: absolute;
  transform: translate(-50%, -50%);
  top: 0.5em;
  left: 0.5em;
  width: 1.75em;
  height: 1.75em;
  border: 2px solid black;
  border-radius: 50%;
  z-index: 10;
  background-color: white;
}

.image_delete_overlay button:hover {
  border-color: #0a58ca;
}

.image_delete_overlay button:hover svg {
  fill: #0a58ca;
}

</style>

<figure class="draggable figure col-lg-3 col-md-4 col-6 px-1">
  <div class="image_inner">
    <img class="figure-img img-fluid img-thumbnail" src="{src}" alt="">
    <div class="image_delete_overlay">
      <button type="button" aria-label="Delete" onclick={deleteItem}>
        <svg class="svg-icon"><use xlink:href="#xmark"></use></svg>
      </button>
    </div>
  </div>
  <figcaption class="figure-caption text-center">
    <input type="hidden" name="id" value={img.id}>
    <input type="hidden" name="filename" value={img.filename}>
    <input type="text" name="description" value={img.description}>
  </figcaption>
</figure>
