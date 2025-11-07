<script>
  import CountingTextArea from './CountingTextArea.svelte';
  import ErrorBox from './ErrorBox.svelte';

  let { client } = $props();

  let flag_dialog = $state();

  let flag_selected = $state(null);

  let explanation = $state('');
  let explanation_valid = $state(false);
  let explanation_area = $state();

  let illegal_checked = $state(false);

  function isValid(flag_selected, explanation_valid, illegal_checked) {
    switch (flag_selected) {
    case 'inappropriate':
    case 'spam':
      return true;
    case 'illegal':
      return explanation_valid && illegal_checked;
    case 'other':
      return explanation_valid;
    default:
      return false;
    }
  }

  function selectFlagType(event) {
    explanation_area?.revalidate();

    // uncheck confirmation box on selection change
    illegal_checked = false;
  }

  function resetFlagDialog() {
    flag_dialog.returnValue = '';
    flag_dialog.querySelector('form').reset();
    flag_selected = null;
    explanation = '';
  }

  export function openDialog(event) {
    resetFlagDialog();
    flag_dialog.showModal();
  }

  function maybeCloseDialog(event) {
    // The backdrop is part of the dialog element, but the dialog contents
    // completely fill the portion that is "inside" the dialog, so any clicks
    // that register as being directly on the dialog are actually "outside"
    // it, which means we should close the dialog.
    //
    // We listen on mousedown to prevent the situation in which the user
    // selected text inside the dialog but released the mouse button outside
    // the dialog.
    if (event.target === event.currentTarget) {
      event.currentTarget.close();
    }
  }

  let error = $state(null);

  async function submitFlag(event) {
    event.preventDefault();
    const fdata = new FormData(event.target);

    const flag = fdata.get('flag');
    const message = fdata.get('message')?.trim();

    try {
      await client.addFlag(flag, message);
      error = null;
      flag_dialog.close();
    }
    catch (err) {
      error = err;
      return;
    }
  }

  function closeDialog(event) {
    flag_dialog.close();
  }

// TODO: fix guidelines link

</script>

<style>

#flag_dialog {
  max-width: 38em;
}

#flag_dialog::backdrop {
  background-color: #000000;
/*  animation: fade 0.3s forwards; */
  opacity: 0.6;
}

/*
@keyframes fade {
  from {
    opacity: 0;
  }

  to {
    opacity: 0.6;
  }
}
*/

.flag_item {
  display: grid;
  grid-template-columns: auto 1fr;
  grid-column-gap: 0.5em;
  grid-row-gap: 0px;
}

.flag_item .flag_type_radio {
  grid-area: 1 / 1 / 2 / 2;
}

.flag_item .flag_type {
  grid-area: 1 / 2 / 2 / 3;
}

.flag_item .flag_type_description {
  grid-area: 2 / 2 / 3 / 3;
}

.flag_item .flag_explanation {
  grid-area: 3 / 2 / 3 / 3;
}

.flag_item .flag_explanation_checkbox {
  grid-area: 4 / 2 / 4 / 3;
}

#flag_dialog .close_button {
  background: none;
  border: none;
  padding: 0;
}

#flag_dialog .close_icon {
  fill: #bbbbbb;
}

#flag_dialog .close_icon:hover {
  fill: var(--bs-link-hover-color);
}

</style>

<dialog id="flag_dialog" class="container p-0 border-0 shadow-lg " bind:this={flag_dialog} onclose={closeDialog} onmousedown={maybeCloseDialog}>
  {#if error}
  <ErrorBox {error} />
  {/if}
  <form id="flag_form" action="" onsubmit={submitFlag}>
    <header class="d-flex align-items-center border-bottom ps-4 pe-3 py-3">
      <h1 class="fs-4 m-0">Thanks for keeping our community civil!</h1>
      <button id="flag_dialog_close" class="close_button ms-auto fs-4" type="button" aria-label="Close" onclick={closeDialog}><svg class="svg-icon close_icon"><use xlink:href="#xmark"></use></svg></button>
    </header>
    <div class="px-4 pt-4 pb-3">
      <p>All flags are received by moderators and will be reviewed as soon as possible.</p>
      <label class="flag_item pt-1 mb-1">
        <input class="flag_type_radio" type="radio" name="flag" bind:group={flag_selected} value="inappropriate" />
        <strong class="flag_type">It's Inappropriate</strong>
        <div class="flag_type_description mb-1">This page contains content that a reasonable person would consider offensive, abusive, to be hateful conduct or a violation of <a href="/guidelines">our community guidelines</a>.</div>
      </label>
      <label class="flag_item pt-1 mb-1">
        <input class="flag_type_radio" type="radio" name="flag" bind:group={flag_selected} value="spam" onchange={selectFlagType} />
        <strong class="flag_type">It's Spam</strong>
        <div class="flag_type_description mb-1">This page contains an advertisement, or vandalism. It is not useful or relevant to the current topic.</div>
      </label>
      <label class="flag_item pt-1 mb-1">
        <input class="flag_type_radio" type="radio" name="flag" bind:group={flag_selected} value="illegal" onchange={selectFlagType} />
        <strong class="flag_type">It's Illegal</strong>
        <div class="flag_type_description mb-1">This page requires staff attention because I believe it contains content that is illegal.</div>
        {#if flag_selected === 'illegal'}
        <div class="flag_explanation mb-1">
          <CountingTextArea name="message" placeholder={"Let us know specifically why you believe this content is illegal, and provide relevant links and examples where possible."} bind:value={explanation} bind:valid={explanation_valid} bind:this={explanation_area}  />
        </div>
        <label class="flag_explanation_checkbox d-flex gap-2">
          <input type="checkbox" bind:checked={illegal_checked} /><span>What I've written above is accurate and complete.</span>
        </label>
        {/if}
      </label>
      <label class="flag_item pt-1 mb-1">
        <input class="flag_type_radio" type="radio" name="flag" bind:group={flag_selected} value="other" onchange={selectFlagType} />
        <strong class="flag_type">Something Else</strong>
        <div class="flag_type_description mb-1">This post requires staff attention for another reason not listed above.</div>
        {#if flag_selected === 'other'}
        <div class="flag_explanation mb-1">
          <CountingTextArea name="message" placeholder={"Let us know specifically what you are concerned about, and provide relevant links and examples where possible."} bind:value={explanation} bind:valid={explanation_valid} bind:this={explanation_area} />
        </div>
        {/if}
      </label>
    </div>
    <footer class="d-flex border-top px-4 py-3">
      <menu class="m-0 p-0">
        <button id="submit_flag_message" class="btn btn-primary p-2 rounded-0" type="submit" value="submit" disabled={!isValid(flag_selected, explanation_valid, illegal_checked)}><svg class="svg-icon"><use xlink:href="#flag"></use></svg> Message</button>
      </menu>
    </footer>
  </form>
</dialog>
