<script>
  let { ums_url, username, size } = $props();
</script>

<style>

.avatar {
  border-radius: 50%;
  margin-right: 0.1em;
}

</style>

<a href={`${ums_url}/users/${username}`}><img class="avatar" width={size} height={size} loading="lazy" src={`${ums_url}/users/${username}/avatar/${size}`} alt="">{username}</a>
