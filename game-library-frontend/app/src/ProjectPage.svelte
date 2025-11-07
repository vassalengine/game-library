<script>
  import { getCookie } from './lib/util.js';
  import Client from './lib/client.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import SearchBar from './SearchBar.svelte';
  import ErrorBox from './ErrorBox.svelte';
  import FlagDialog from './FlagDialog.svelte';
  import GameSection from './GameSection.svelte';
  import ProjectSection from './ProjectSection.svelte';
  import PackagesSection from './PackagesSection.svelte';
  import ReadmeSection from './ReadmeSection.svelte';
  import GallerySection from './GallerySection.svelte';
  import PlayersSection from './PlayersSection.svelte';

  let {
    current_version,
    news_link,
    base_url,
    user_info,
    project,
    gls_url,
    discourse_url,
    ums_url,
    returnto,
    proj = $bindable(),
    proj_error,
    players,
    players_error
  } = $props();

  const client = new Client(
    gls_url,
    ums_url,
    project,
    getCookie('token'),
    getCookie('refresh')
  );

  let editing = $state(false);

  let flag_dialog = $state(null);
</script>

<style>

/* Edit icons */

#project_content :global(.is_editable) {
  visibility: visible !important;
}

#project_content :global(.edit_button) {
  background: none;
  border: none;
  padding: 0;
  visibility: hidden;
}

#project_content :global(.edit_icon) {
  width: 0.75em;
  height: 0.75em;
  fill: #bbbbbb;
  vertical-align: 0;
}

#project_content :global(.edit_icon:hover) {
  fill: var(--bs-link-hover-color);
}

/* Delete icons */

#project_content :global(.is_deletable) {
  visibility: visible !important;
}

#project_content :global(.delete_button) {
  background: none;
  border: none;
  padding: 0;
  visibility: hidden;
}

#project_content :global(.delete_icon) {
  width: 0.75em;
  height: 0.75em;
  fill: #bbbbbb;
  vertical-align: 0;
}

#project_content :global(.delete_icon:hover) {
  fill: var(--bs-link-hover-color);
}

</style>

<svelte:head>
  {#if proj}
  <title>{proj.game.title} - Module Library - Vassal</title>
  {:else}
  <title>Module Library - Vassal</title>
  {/if}
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

<SearchBar {base_url} />

<div id="project_content">
  {#if proj_error}
  <ErrorBox error={proj_error} />
  {/if}
  {#if proj}
  <GameSection bind:proj={proj} {client} username={user_info?.username} bind:editing={editing} />
  <ProjectSection bind:proj={proj} {client} username={user_info?.username} {ums_url} bind:editing={editing} />
  <PackagesSection bind:proj={proj} {client} username={user_info?.username} {ums_url} bind:editing={editing} />
  <ReadmeSection bind:proj={proj} {client} username={user_info?.username} bind:editing={editing} />
  <GallerySection bind:proj={proj} {client} username={user_info?.username} bind:editing={editing} />
  {/if}
</div>

<!-- TODO: limit text field length -->

{#if players_error}
<ErrorBox error={players_error} />
{/if}
{#if players !== null}
<PlayersSection players={players} {client} username={user_info?.username} ums_url={ums_url} bind:editing={editing} />
{/if}

{#if user_info}
<button id="flag_button" class="btn bg-secondary-subtle p-1 mx-1 rounded-0" type="button" onclick={() => flag_dialog.openDialog()}><svg class="svg-icon"><use xlink:href="#flag"></use></svg> Flag</button>
{/if}
</main>

{#if user_info}
<FlagDialog bind:this={flag_dialog} {client} />
{/if}

<Footer/>
