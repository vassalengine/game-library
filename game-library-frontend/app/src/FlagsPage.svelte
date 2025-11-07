<script>
  import markdownIt from 'https://cdn.jsdelivr.net/npm/markdown-it@14.1.0/+esm';
  import DOMPurify from 'https://cdn.jsdelivr.net/npm/dompurify@3.2.6/+esm';
  import { getCookie } from './lib/util.js';

  import Client from './lib/client.js';

  import Header from './Header.svelte';
  import Footer from './Footer.svelte';
  import ErrorBox from './ErrorBox.svelte';
  import UserChip from './UserChip.svelte';

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

  const client = new Client(
    gls_url,
    ums_url,
    null,
    getCookie('token'),
    getCookie('refresh')
  );

  let flags = $state(null);
  let error = $state(null);

  client.getFlags()
    .then((f) => flags = f.flags)
    .catch((err) => error = err);

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

  function replyTitle(flag) {
    return encodeURIComponent(`Flag for project ${flag.project}`);
  }

  function replyBody(flag) {
    let flag_type;
    let message;

    switch (flag.flag) {
    case "inappropriate":
      flag_type = `an ${flag.flag}`;
      break;
    case "spam":
      flag_type = `a ${flag.flag}`;
      break;
    case "illegal":
      flag_type = `an ${flag.flag}`;
      break;
    case "other":
      flag_type = `a`;
      break;
    }

    const proj_url = `${window.location.origin}${base_url}/projects/${flag.slug}`;

    switch (flag.flag) {
    case "inappropriate":
    case "spam":
      message = `You raised ${flag_type} flag on project [${flag.project}](${proj_url}). Thanks for your report.`
      break;
    case "illegal":
    case "other":
      message = `You raised ${flag_type} flag on project [${flag.project}](${proj_url}):

> ${flag.message.replaceAll("\n", "\n> ")}

Thanks for your report.`
      break;
    }

    return encodeURIComponent(message);
  }

  async function closeFlag(flag_id) {
    // close the selected flag
    try {
      await client.closeFlag(flag_id);
      error = null;
    }
    catch (err) {
      error = err;
      return;
    }

    // remove rows for the now-closed flag
    for (const el of document.querySelectorAll(`.flag-${flag_id}`)) {
      el.remove();
    }
  }

</script>

<style>

tr:nth-child(even) {
  border-bottom: 1px solid black;
}

</style>

<svelte:head>
  <title>Admin - Flags - Module Library - Vassal</title>
</svelte:head>

<Header {base_url} {user_info} {discourse_url} {ums_url} {returnto} {current_version} {news_link} />

<main class="container px-5 mb-5">
  {#if error}
  <ErrorBox error={error} />
  {/if}

  <table>
    <thead>
      <tr>
        <th></th>
        <th>Project</th>
        <th>Reporter</th>
        <th>Type</th>
        <th>Date</th>
      </tr>
    </thead>
    <tbody>
    {#if flags !== null}
    {#each flags as flag}
      <tr class="flag-{flag.flag_id}">
        <td><button class="btn p-1 mx-1 rounded-0" type="button" title="Close" aria-label="Close" onclick={() => closeFlag(flag.flag_id)}><svg class="svg-icon close_icon"><use xlink:href="#xmark"></use></svg></button></td>
        <td><a href="{base_url}/projects/{flag.slug}">{flag.project}</a></td>
        <td>
          <a href="{discourse_url}/new-message?username={flag.flagged_by}&title={replyTitle(flag)}&body={replyBody(flag)}" aria-label="Reply" title="Reply"><svg class="svg-icon"><use xlink:href="#reply"></use></svg></a>
          <UserChip {ums_url} username={flag.flagged_by} size=24 />
        </td>
        <td>{flag.flag}</td>
        <td>{flag.flagged_at.replace(/\.[0-9]+Z/, "").replace("T", " ")}</td>
      </tr>
      <tr class="flag-{flag.flag_id}">
        <td colspan="4">{@html flag.message ? DOMPurify.sanitize(md.render(flag.message)) : ""}</td>
      </tr>
    {/each}
    {/if}
    </tbody>
  </table>
</main>

<Footer/>
