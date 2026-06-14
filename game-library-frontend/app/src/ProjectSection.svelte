<script>
  import { formatDistance } from './lib/util.js';

  import ErrorBox from './ErrorBox.svelte';
  import UserChip from './UserChip.svelte';
  import ProjectEditor from './ProjectEditor.svelte';
 
  let {
    ums_url,
    proj = $bindable(),
    client,
    username,
    editing = $bindable()
  } = $props();

  function user_is_owner() {
     return username && proj.owners.includes(username);
  }

  const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

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
    event.stopPropagation();

    const fdata = new FormData(event.target);

    const cur_owners = new Set(fdata.getAll('owner'));
    const prev_owners = new Set(proj.owners);
    const to_add = [...[...cur_owners.values()].filter((u) => !prev_owners.has(u))];
    const to_remove = [...[...prev_owners.values()].filter((u) => !cur_owners.has(u))];

    try {
      if (to_add) {
        await client.addOwners(to_add);
      }

      if (to_remove) {
        await client.removeOwners(to_remove);
      }
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

  //
  // Feedback and forum search
  //
  const forumURL = "https://forum.vassalengine.org/";
  const projectsURL = "https://vassalengine.org/library/projects/"

  function gameEncoded() {
    return encodeURIComponent( "[" + proj.game.title + "]" );
  }

  function projectURL() {
    return projectsURL+proj.name;
  }

  function projectQueryComponent(forum) {
    return encodeURIComponent(proj.name+" "+proj.game.title+" #"+forum);
  }

  function projectQuery(forum) {
    const url = forumURL + "search?q=" + projectQueryComponent(forum);
    window.open(url, '_blank').focus();
  }

  function searchDiscussion(event) {
    projectQuery("module-discussion");
  }

  function searchSupport(event) {
    projectQuery("module-support");
  }

  function projectLink() {
    return encodeURIComponent("Regarding the project [" + proj.game.title + "](" + projectURL() + ").");
  }

  function forumPost(forum) {
    const url = forumURL + "new-topic?title=" + gameEncoded() + "&body=" + projectLink() + "&category=" + forum;
    window.open(url, '_blank').focus();
  }

  function postDiscussion(event) {
    forumPost("module-discussion");
  }

  function postSupport(event) {
    forumPost("module-support");
  }


  function messageOwners(event) {
    const owners = proj.owners.join(",");
    const url    = forumURL+"new-message?username="+owners+"&title="+gameEncoded()+"&body="+projectLink();
    window.open(url, '_blank').focus();
  }
  /*
    Search: https://forum.vassalengine.org/search?q=anzio%2Bbeachhead%20anzio_beachhead_cholmcc%20%23module-discussion
    Post: https://forum.vassalengine.org/new-topic?title=https://vassalengine.org/library/projects/anzio_beachhead_cholmcc&category=module-discussion
  */
</script>
<style>
  .forum-category::before {
    content: "";
    display: inline-block;
    margin-right: 0.125em;
    width: 0.75em;
    height: 0.75em;
    border-radius: 2pt;
    background-color: var(--forum-category-color);
    flex: 0 0 auto;
  }
  .module-discussion {
    --forum-category-color: #3AB54A;
  }
  .module-support {
    --forum-category-color: #25AAE2;
  }
  .message-button {
    padding-left: .2em !important;
    padding-right: .3em !important;
  }
  .m-r-1 {
    margin-right: .3em !important;
  }
</style>
{#if error}
<ErrorBox {error} />
{/if}
<div class="my-2 pb-2">
  <h2>
    <svg class="svg-icon"><use xlink:href="#person-digging"></use></svg>
    Project
    <button class="edit_button"
            class:is_editable={!editing && user_is_owner()}
            type="button"
            aria-label="Edit"
            title="Edit this project"
            onclick={startEdit}>
      <svg class="svg-icon edit_icon">
        <use xlink:href="#pencil"></use>
      </svg>
    </button>
  </h2>
{#if edit}
  <ProjectEditor {client} {proj} {submitEdit} {cancelEdit} />
{:else}
  {@const now = new Date()}
  <div>
    <div class="px-3 pt-2 pb-2 border rounded d-flex flex-wrap gap-2">
      <div>
        <div class="d-flex align-items-center gap-1">
          <svg class="svg-icon"><use xlink:href="#id-badge"></use></svg>
          {proj.name}
        </div>
        <div class="d-flex align-items-center gap-1">
          <div>
            <svg class="svg-icon"><use xlink:href="#user"></use></svg>
            Owners
          </div>
          <ul class="d-flex flex-wrap list-unstyled m-0 gap-1">
          {#each proj.owners as owner}
            <li><UserChip {ums_url} username={owner} size=24 /></li>
          {/each}
          </ul>
          <button class="btn btn-primary p-0 message-button"
                  type="button"
                  title="Send direct message to all project owners"
                  onclick={messageOwners}>
            <svg class="svg-icon">
              <use xlink:href="#envelope"></use>
            </svg>
            <span class="d-button-label">{"Message"+(proj.owners.length>1?" all":"")}</span>
          </button>
        </div>
        <div>
          <div class="d-inline-block">
            <svg class="svg-icon">
              <use xlink:href="#landmark"></use>
            </svg>
          </div>
          <div class="d-inline-flex align-middle">
            <div class="d-flex flex-column m-r-1">
              <span class="forum-category"
                    style="--forum-category-color: #3AB54A;">
                Module Discussion:
              </span>
              <span class="forum-category"
                    style="--forum-category-color: #25AAE2;">
                Module Support:
              </span>
            </div>
            <div class="d-flex flex-column m-r-1">
              <button class="btn p-0 mx-1"
                      type="button"
                      title="Search for posts on this project in the Module Discussion forum"
                      onclick={searchDiscussion}>
                <svg class="svg-icon">
                  <use xlink:href="#search"></use>
                </svg>
                <span class="d-button-label">Search</span>
              </button>
              <button class="btn p-0 mx-1"
                      type="button"
                      title="Search for posts on this project in the Module Support forum"
                      onclick={searchSupport}>
                <svg class="svg-icon">
                  <use xlink:href="#search"></use>
                </svg>
                <span class="d-button-label">Search</span>
              </button>
            </div>
            <div class="d-flex flex-column">              
              <button class="btn btn-light p-0 rounded-0"
                      type="button"
                      title="Start new topic on this project in the Module Discussion forum"
                      onclick={postDiscussion}>
                  <svg class="svg-icon">
                    <use xlink:href="#pen-to-square"></use>
                  </svg>
                  <span class="d-button-label">New topic</span>
              </button>
              <button class="btn btn-light p-0 rounded-0"
                      type="button"
                      title="Start new topic on this project in the Module Support forum"
                      onclick={postSupport}>
                <svg class="svg-icon">
                  <use xlink:href="#pen-to-square"></use>
                </svg>
                <span class="d-button-label">New topic</span>
              </button>
            </div>
          </div>
        </div>
      </div>
      <div class="ms-auto">
        <div>
          <time datetime={proj.modified_at} title={proj.modified_at}>
            <svg class="svg-icon"><use xlink:href="#arrows-rotate"></use></svg>
            Updated {formatDistance(rtf, new Date(proj.modified_at), now)}
          </time>
        </div>
        <div>
          <time datetime={proj.created_at} title={proj.created_at}>
            <svg class="svg-icon"><use xlink:href="#star"></use></svg>
            Created {formatDistance(rtf, new Date(proj.created_at), now)}
          </time>
        </div>
      </div>
    </div>
  </div>
{/if}
</div>
