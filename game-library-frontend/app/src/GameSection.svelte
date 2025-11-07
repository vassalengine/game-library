<script>
  import Client from './lib/client.js';

  import ErrorBox from './ErrorBox.svelte';
  import GameEditor from './GameEditor.svelte';

  let {
    proj = $bindable(),
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  let players_slug = $state();

  function plural(v) {
    return v === 1 ? '' : 's';
  }

  function makePlayersSlug(min, max) {
    return makeRangeSlug(min, 'player', max, 'player');
  }

  let length_slug = $state();

  function roundNearest(v, step) {
    return Math.round(v / step) * step;
  }

  function minutesOrHours(t) {
    return t > 90 ? [roundNearest(t / 60, 0.5), 'hour'] : [t, 'minute'];
  }

  function makeLengthSlug(min, max) {
    return makeRangeSlug(...minutesOrHours(min), ...minutesOrHours(max))
  }

  function makeRangeSlug(lval, lunit, hval, hunit) {
    if (lval !== null) {
      if (hval !== null) {
        if (lval === hval) {
          return `${lval} ${lunit}${plural(lval)}`;
        }
        else if (lunit === hunit) {
          return `${lval}–${hval} ${hunit}s`;
        }
        else {
          return `${lval} ${lunit}${plural(lval)}–${hval} ${hunit}${plural(hval)}`;
        }
      }
      else {
        return `At least ${lval} ${lunit}${plural(lval)}`;
      }
    }
    else if (hval !== null) {
      return `Up to ${hval} ${hunit}${plural(hval)}`;
    }
    else {
      return 'unknown';
    }
  }

  //
  // box image
  //

  let box_img = $state(proj.image ? client.imageUrl(proj.image) : '');

  function init() {
    box_img = proj.image ? client.imageUrl(proj.image) : '';

    players_slug = makePlayersSlug(
      proj.game.players?.min ?? null,
      proj.game.players?.max ?? null
    );

    length_slug = makeLengthSlug(
      proj.game.length?.min ?? null,
      proj.game.length?.max ?? null
    );
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
    init();
  }

  async function submitEdit(event) {
    event.preventDefault();
    event.stopPropagation();

    const fdata = new FormData(event.target);

    const data = {};

    // check for updates to properties which cannot be null

    const description = fdata.get('description').trim();
    if (description !== proj.description) {
      data.description = description;
    }

    for (const k of ['title', 'publisher', 'year']) {
      const fv = fdata.get(`game_${k}`).trim();
      if (fv !== proj.game[k]) {
        (data.game ??= {})[k] = fv;
      }
    }

    // check for updates to min/max players, length, which can be null
    for (const k0 of ['players', 'length']) {
      for (const k1 of ['min', 'max']) {
        let fv = fdata.get(`game_${k0}_${k1}`).trim();
        fv = fv === '' ? null : Number(fv);

        if (fv !== proj.game[k0][k1]) {
          ((data.game ??= {})[k0] ??= {})[k1] = fv;
        }
      }
    }

    // check for updates to image
    const box_image = fdata.get('box_image');
    if (box_image.name !== '') {
      data.image = box_image.name;
    }
    else if (fdata.get('box_image_clear') === 'true') {
      data.image = null;
    }

    // submit changes only if there are some
    if (Object.keys(data).length > 0) {

      if (data.image) {
        data.image = box_image.name;

        try {
          const [_, promise] = await client.addImage(
            box_image.name,
            box_image,
            box_image.type
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
      }

      try {
        await client.updateProject(data);
        error = null;
      }
      catch (err) {
        error = err;
        return;
      }

      // update the project data
      try {
        proj = await client.getProject();
        init();
        error = null;
      }
      catch (err) {
        error = err;
        return;
      }
    }

    edit = false;
    editing = false;
  }

  init();
</script>

<style>

#box_image_none {
  display: none;
  font-style: italic;
  background-color: #dee2e6;
}

.no_image #box_image_none {
  display: flex;
  justify-content: center;
  align-items: center;
  width: 12em;
  height: 12em;
}

.no_image #box_image {
  display: none !important;
}

@media only screen and (min-width: 768px) {
  #box_image {
    max-width: 240px;
  }
}
</style>

<!-- TODO: disable submit buttons when there are no changes -->

{#if error}
<ErrorBox {error} />
{/if}
<div class="my-2 p-2 border rounded clearfix bg-light">
{#if edit}
  <GameEditor {proj} {box_img} {submitEdit} {cancelEdit} />
{:else}
  <div class="m-3">
    <div class="row mb-3">
      <div class="col-md-auto text-center mb-2" class:no_image={!box_img}>
        <img id="box_image" class="rounded border" src={box_img} alt="box cover">
        <div id="box_image_none" class="rounded border">
          <div>no image</div>
        </div>
      </div>
      <div class="col">
        <h1 class="">
          {proj.game.title}
          <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Edit" onclick={startEdit}>
            <svg class="svg-icon edit_icon"><use xlink:href="#pencil"></use></svg>
          </button>
        </h1>
        <div class="d-flex flex-wrap gap-3 h5 fw-normal">
          <div>
            <svg class="svg-icon"><use xlink:href="#industry"></use></svg>
            {#if proj.game.publisher}
            <a href=".">{proj.game.publisher}</a>{#if proj.game.publisher}, {/if}
            {/if}
            {#if proj.game.year}
            <a href=".">{proj.game.year}</a>
            {/if}
          </div>
          <div><svg class="svg-icon"><use xlink:href="#user-group"></use></svg> <a href=".">{players_slug}</a></div>
          <div><svg class="svg-icon"><use xlink:href="#clock"></use></svg> <a href=".">{length_slug}</a></div>
        </div>
        <div class="d-flex flex-wrap gap-2">
          <svg class="align-self-center svg-icon"><use xlink:href="#tags"></use></svg>
          <ul class="d-flex flex-wrap list-unstyled gap-3 mb-0">
            {#each proj.tags as tag}
            <li><a href=".">{tag.replace(':', ': ')}</a></li>
            {/each}
          </ul>
        </div>
      </div>
    </div>
    <p class="mb-0">{proj.description}</p>
  </div>
{/if}
</div>
