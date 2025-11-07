<script>
  import { getCookie, isValidProjectName, slugFor } from './lib/util.js';

  import Client from './lib/client.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import SearchBar from './SearchBar.svelte';
  import ErrorBox from './ErrorBox.svelte';

  let {
    current_version,
    news_link,
    base_url,
    user_info,
    gls_url,
    discourse_url,
    ums_url,
    returnto
  } = $props();

  let error = $state(null);

  const client = new Client(
    gls_url,
    ums_url,
    null,
    getCookie('token'),
    getCookie('refresh')
  );

  async function submitEdit(event) {
    event.preventDefault();

    error = null;

    const fdata = new FormData(event.target);

    const pname = fdata.get('project_name')?.trim();
    const gtitle = fdata.get('game_title')?.trim();

    if (pname) {
      const data = {
        name: pname,
        description: "",
        tags: [],
        game: {
          title: gtitle,
          publisher: "",
          year: "",
          players: {
            min: null,
            max: null
          },
          length: {
            min: null,
            max: null
          }
        },
        readme: ""
      };

      const slug = slugFor(pname);
      client.project = slug;

      try {
        await client.createProject(data);
        error = null;
      }
      catch (err) {
        error = err;
        return;
      }

      // redirect to the new project page
      window.location.replace(`${base_url}/projects/${slug}`);
    }
  }

  function validateProjectName(event) {
    const msg = isValidProjectName(event.target.value) ?
      '' :
      'Project names must be 5 to 64 characters long without leading, trailing, or consecutive whitespace and contain only letters, numbers, punctuation, separators, and marks.';
    event.target.setCustomValidity(msg);
  }

</script>

<svelte:head>
  <title>Module Library - Vassal</title>
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">

<SearchBar {base_url} />

<div id="new_project_content">

{#if error}
  <ErrorBox {error} />
{/if}

  <div class="my-2 pb-2">
    <h2>
      <svg class="svg-icon"><use xlink:href="#person-digging"></use></svg>
      Create Project
    </h2>

{#if user_info}
    <div class="container">
      <div class="row">
        <form action="" onsubmit={submitEdit}>
          <label for="project_name_input" class="form-label">Project name</label>
          <input id="project_name_input" type="text" name="project_name" class="form-control" required oninput={validateProjectName}>
          <label for="game_title_input" class="form-label">Game title</label>
          <input id="game_title_input" type="text" name="game_title" class="form-control" required>
          <button class="btn btn-primary p-1 mx-1 rounded-0" type="submit" aria-label="Submit"><svg class="svg-icon"><use xlink:href="#check"></use></svg></button>
        </form>
      </div>
    </div>
{:else}
<ErrorBox error={"Log in to create a project."} />
{/if}

  </div>
</div>

</main>

<Footer/>
