<script>
  import markdownIt from 'https://cdn.jsdelivr.net/npm/markdown-it@14.1.0/+esm';
  import DOMPurify from 'https://cdn.jsdelivr.net/npm/dompurify@3.2.6/+esm';

  import ErrorBox from './ErrorBox.svelte';

  let {
    proj = $bindable(),
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  function renderImage(tokens, idx, options, env, self) {
    // Get token of a specific image
    const tok = tokens[idx];

    // Use token to get src and alt attributes
    const src = client.imageUrl(tok.attrGet("src"));
    const alt = tok.content;

    // return image HTML
    return `<img src="${src}" alt="${alt}" loading="lazy" />`;
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

  //  mdHtml.renderer.rules.paragraph_open = mdHtml.renderer.rules.heading_open = injectLineNumbers;

    md.renderer.rules['image'] = renderImage;

    return md;
  }

  const md = mdInit();

  let source = $state(proj.readme);

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

    const data = { readme: source };

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

<style>
.full-height {
  height: 100%;
}

.source {
  width: 100%;
  font-family: monospace;
  font-size: 13px;
  padding: 2px;
}

.result {
  padding: 2px 10px;
  overflow: auto;
  background-color: #fff;
  border: 1px solid #ccc;
  border-radius: 4px;
}
</style>

<!-- TODO: Add formatting bar like Discourse -->

{#if error}
<ErrorBox {error} />
{/if}
<div>
  <h2>
    <svg class="svg-icon"><use xlink:href="#info-circle"></use></svg>
    Readme
    <button class="edit_button" class:is_editable={!editing && user_is_owner()} type="button" aria-label="Edit" onclick={startEdit}>
      <svg class="svg-icon edit_icon"><use xlink:href="#pencil"></use></svg>
    </button>
  </h2>
{#if edit}
  <div class="container">
    <div class="row">
      <div class="col-6">
        <textarea id="readme_source" class="source full-height" bind:value={source}></textarea>
      </div>
      <section class="col-6">
<!-- TODO: do we need to sanitize here? -->
        <div id="readme_result" class="result full-height">{@html md.render(source)}</div>
      </section>
    </div>
    <div class="row">
      <form id="readme_form" action="" onsubmit={submitEdit}>
        <button type="submit" aria-label="Submit" class="btn btn-primary"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
        <button id="cancel" aria-label="Cancel" type="button" class="btn btn-primary" onclick={cancelEdit}><svg class="svg-icon"><use xlink:href="#xmark"></use></svg></button>
      </form>
    </div>
  </div>
{:else}
  <div>{@html DOMPurify.sanitize(md.render(proj.readme))}</div>
{/if}
</div>
