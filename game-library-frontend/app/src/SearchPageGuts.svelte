<script>
  import { fetchJSON } from './lib/client.js';
  import { makeRequestURL } from './lib/search.js';

  import ErrorBox from './ErrorBox.svelte';
  import ProjectList from './ProjectList.svelte';
  import ProjectListSort from './ProjectListSort.svelte';

  let {
    base_url,
    gls_url,
    sort_by,
    relevance,
    limit,
    loadProjects,
    error = $bindable(),
    meta = $bindable(),
    projects = $bindable(),
  } = $props();
 
  function updateSort(event) {
    const url = new URL(window.location);

    url.searchParams.delete('from');
    url.searchParams.set('sort_by', event.target.value);

    // update page instead of reloading
    loadProjects(makeRequestURL(gls_url, url.searchParams, limit));
    window.history.replaceState(null, '', url.toString());
  }

  function handleIntersect(entries) {
    if (!entries[0].isIntersecting) {
      return;
    }

    if (!meta.next_page) {
      return;
    }

    const target = entries[0].target;

    observer.unobserve(target);

    const page = meta.next_page;
    const projects_list = document.getElementById('projects_list');

    const s_query = page.substring(1);
    const s_params = new URLSearchParams(s_query);
    const s_url = makeRequestURL(gls_url, s_params, limit);

    fetchJSON(s_url)
        .then((result) => {
          meta.next_page = result.meta.next_page;
          // must reassign to projects so it works reactively
          projects = projects.concat(result.projects);
        })
        .then(() => observer.observe(target))
        .catch((err) => error = err);
  }

  const observer = new IntersectionObserver(handleIntersect);
  const watchScroll = (el) => observer.observe(el);

</script>

<ProjectListSort {sort_by} {relevance} {updateSort} />

{#if error}
<ErrorBox {error} />
{/if}

{#if projects}
<ProjectList {base_url} {projects} />
<div {@attach watchScroll} id="scroll_forward" class="infinite-scroll"></div>
{/if}


