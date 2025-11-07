<script>
  let {
    base_url,
    user_info,
    discourse_url,
    ums_url,
    returnto,
    current_version,
    news_link
  } = $props();
</script>

<style>

#current_user button img.avatar {
  width: 2em;
  height: 2em;
  margin: 0.2em;
}

.avatar {
  border-radius: 50%;
}

</style>

<!-- nav -->

<header class="d-flex flex-wrap align-items-center justify-content-center justify-content-md-between px-2 py-2">

  <script src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js" integrity="sha384-I7E8VVD/ismYTF4hNIPjVp/Zjvgyol6VFvRkX/vR+Vc4jQkC+hVqc2pM8ODewa9r" crossorigin="anonymous"></script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/js/bootstrap.min.js" integrity="sha384-7qAoOXltbVP82dhxHAUje59V5r2YsVfBafyUDxEdApLPmcdhBPg1DKg1ERo0BZlK" crossorigin="anonymous"></script>

  <a href="/" class="d-flex align-items-center col-md-3 mb-2 mb-md-0 text-dark text-decoration-none">
    <img class="wordmark" src="/images/wordmark-path.svg" width="529" height="180" alt="Vassal">
  </a>

  <ul class="nav col-12 col-md-auto mb-2 justify-content-center align-items-center mb-md-0">
    {#if !user_info}
    <li id="sign_up_user"><a class="btn btn-primary p-1 mx-1 rounded-0" href="{discourse_url}/signup" role="button">Sign Up</a></li>
    <li id="login_user"><a class="btn btn-primary p-1 mx-1 rounded-0" href="{ums_url}/sso/login?returnto={returnto}" role="button"><svg class="svg-icon"><use xlink:href="#user"></use></svg> Log In</a></li>
    {/if}
    <li><a class="nav-link px-2 link-dark" href="/about.html"><svg class="svg-icon"><use xlink:href="#info-circle"></use></svg> About</a></li>
    <li><a class="nav-link px-2 link-dark" href="https://discord.gg/GDdfq9AkfM"><svg class="svg-icon"><use xlink:href="#fab-discord"></use></svg> Chat</a></li>
    <li><a class="nav-link px-2 link-dark" href="https://forum.vassalengine.org"><svg class="svg-icon"><use xlink:href="#landmark"></use></svg> Forum</a></li>
    <li><a class="nav-link px-2 link-dark" href="{base_url}/projects"><svg class="svg-icon"><use xlink:href="#chess-knight"></use></svg> Modules</a></li>
<!-- TODO: update doc link -->
    <li><a class="nav-link px-2 link-dark" href="https://forum.vassalengine.org/c/documentation/18"><svg class="svg-icon"><use xlink:href="#book-open"></use></svg> Documentation</a></li>
    <li><a class="nav-link px-2 link-dark" href="https://forum.vassalengine.org/c/news/17"><svg class="svg-icon"><use xlink:href="#bullhorn"></use></svg> News</a></li>
    {#if user_info}
    <li id="current_user" class="nav-item dropdown">
      <button id="userDropdown" class="nav-link p-0 link-dark" data-bs-toggle="dropdown" aria-expanded="false">
        <img id="userAvatar" class="avatar" title="{user_info.name}" src="{user_info.avatar_url}" alt="">
      </button>
      <ul class="dropdown-menu" aria-labelledby="userDropdown">
        <li><a id="userProfile" class="dropdown-item" href="{discourse_url}/u/{user_info.username}"><svg class="svg-icon"><use xlink:href="#user"></use></svg> Profile</a></li>
        <li><a id="userLogout" class="dropdown-item" href="{ums_url}/sso/logout?returnto={returnto}"><svg class="svg-icon"><use xlink:href="#right-from-bracket"></use></svg> Log Out</a></li>
      </ul>
    </li>
    {/if}
  </ul>
</header>

<div id="alert-bar" class="text-center">
  <h5 class="py-3">We've moved to the new module library. See <a class="link-light" href="{news_link}">here</a> for details.</h5>
</div>

<div class="svg-sprites">
  <div class="fontawesome">
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="book-open" viewBox="0 0 576 512">
        <path d="M542.22 32.05c-54.8 3.11-163.72 14.43-230.96 55.59-4.64 2.84-7.27 7.89-7.27 13.17v363.87c0 11.55 12.63 18.85 23.28 13.49 69.18-34.82 169.23-44.32 218.7-46.92 16.89-.89 30.02-14.43 30.02-30.66V62.75c.01-17.71-15.35-31.74-33.77-30.7zM264.73 87.64C197.5 46.48 88.58 35.17 33.78 32.05 15.36 31.01 0 45.04 0 62.75V400.6c0 16.24 13.13 29.78 30.02 30.66 49.49 2.6 149.59 12.11 218.77 46.95 10.62 5.35 23.21-1.94 23.21-13.46V100.63c0-5.29-2.62-10.14-7.27-12.99z"></path>
      </symbol>
      <symbol id="bullhorn" viewBox="0 0 576 512">
        <path d="M576 240c0-23.63-12.95-44.04-32-55.12V32.01C544 23.26 537.02 0 512 0c-7.12 0-14.19 2.38-19.98 7.02l-85.03 68.03C364.28 109.19 310.66 128 256 128H64c-35.35 0-64 28.65-64 64v96c0 35.35 28.65 64 64 64h33.7c-1.39 10.48-2.18 21.14-2.18 32 0 39.77 9.26 77.35 25.56 110.94 5.19 10.69 16.52 17.06 28.4 17.06h74.28c26.05 0 41.69-29.84 25.9-50.56-16.4-21.52-26.15-48.36-26.15-77.44 0-11.11 1.62-21.79 4.41-32H256c54.66 0 108.28 18.81 150.98 52.95l85.03 68.03a32.023 32.023 0 0 0 19.98 7.02c24.92 0 32-22.78 32-32V295.13C563.05 284.04 576 263.63 576 240zm-96 141.42l-33.05-26.44C392.95 311.78 325.12 288 256 288v-96c69.12 0 136.95-23.78 190.95-66.98L480 98.58v282.84z"></path>
      </symbol>
      <symbol id="chess-knight" viewBox="0 0 384 512">
        <path d="M19 272.47l40.63 18.06a32 32 0 0 0 24.88.47l12.78-5.12a32 32 0 0 0 18.76-20.5l9.22-30.65a24 24 0 0 1 12.55-15.65L159.94 208v50.33a48 48 0 0 1-26.53 42.94l-57.22 28.65A80 80 0 0 0 32 401.48V416h319.86V224c0-106-85.92-192-191.92-192H12A12 12 0 0 0 0 44a16.9 16.9 0 0 0 1.79 7.58L16 80l-9 9a24 24 0 0 0-7 17v137.21a32 32 0 0 0 19 29.26zM52 128a20 20 0 1 1-20 20 20 20 0 0 1 20-20zm316 320H16a16 16 0 0 0-16 16v32a16 16 0 0 0 16 16h352a16 16 0 0 0 16-16v-32a16 16 0 0 0-16-16z"></path>
      </symbol>
      <symbol id="info-circle" viewBox="0 0 512 512">
        <path d="M256 8C119.043 8 8 119.083 8 256c0 136.997 111.043 248 248 248s248-111.003 248-248C504 119.083 392.957 8 256 8zm0 110c23.196 0 42 18.804 42 42s-18.804 42-42 42-42-18.804-42-42 18.804-42 42-42zm56 254c0 6.627-5.373 12-12 12h-88c-6.627 0-12-5.373-12-12v-24c0-6.627 5.373-12 12-12h12v-64h-12c-6.627 0-12-5.373-12-12v-24c0-6.627 5.373-12 12-12h64c6.627 0 12 5.373 12 12v100h12c6.627 0 12 5.373 12 12v24z"></path>
      </symbol>
      <symbol id="landmark" viewBox="0 0 512 512">
        <path d="M501.62 92.11L267.24 2.04a31.958 31.958 0 0 0-22.47 0L10.38 92.11A16.001 16.001 0 0 0 0 107.09V144c0 8.84 7.16 16 16 16h480c8.84 0 16-7.16 16-16v-36.91c0-6.67-4.14-12.64-10.38-14.98zM64 192v160H48c-8.84 0-16 7.16-16 16v48h448v-48c0-8.84-7.16-16-16-16h-16V192h-64v160h-96V192h-64v160h-96V192H64zm432 256H16c-8.84 0-16 7.16-16 16v32c0 8.84 7.16 16 16 16h480c8.84 0 16-7.16 16-16v-32c0-8.84-7.16-16-16-16z"></path>
      </symbol>
      <symbol id="fab-discord" viewBox="0 0 448 512">
        <path d="M297.216 243.2c0 15.616-11.52 28.416-26.112 28.416-14.336 0-26.112-12.8-26.112-28.416s11.52-28.416 26.112-28.416c14.592 0 26.112 12.8 26.112 28.416zm-119.552-28.416c-14.592 0-26.112 12.8-26.112 28.416s11.776 28.416 26.112 28.416c14.592 0 26.112-12.8 26.112-28.416.256-15.616-11.52-28.416-26.112-28.416zM448 52.736V512c-64.494-56.994-43.868-38.128-118.784-107.776l13.568 47.36H52.48C23.552 451.584 0 428.032 0 398.848V52.736C0 23.552 23.552 0 52.48 0h343.04C424.448 0 448 23.552 448 52.736zm-72.96 242.688c0-82.432-36.864-149.248-36.864-149.248-36.864-27.648-71.936-26.88-71.936-26.88l-3.584 4.096c43.52 13.312 63.744 32.512 63.744 32.512-60.811-33.329-132.244-33.335-191.232-7.424-9.472 4.352-15.104 7.424-15.104 7.424s21.248-20.224 67.328-33.536l-2.56-3.072s-35.072-.768-71.936 26.88c0 0-36.864 66.816-36.864 149.248 0 0 21.504 37.12 78.08 38.912 0 0 9.472-11.52 17.152-21.248-32.512-9.728-44.8-30.208-44.8-30.208 3.766 2.636 9.976 6.053 10.496 6.4 43.21 24.198 104.588 32.126 159.744 8.96 8.96-3.328 18.944-8.192 29.44-15.104 0 0-12.8 20.992-46.336 30.464 7.68 9.728 16.896 20.736 16.896 20.736 56.576-1.792 78.336-38.912 78.336-38.912z"></path>
      </symbol>
    </svg>
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="user" viewBox="0 0 448 512">
        <!--!Font Awesome Free 6.5.1 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2024 Fonticons, Inc.-->
        <path d="M224 256A128 128 0 1 0 224 0a128 128 0 1 0 0 256zm-45.7 48C79.8 304 0 383.8 0 482.3C0 498.7 13.3 512 29.7 512H418.3c16.4 0 29.7-13.3 29.7-29.7C448 383.8 368.2 304 269.7 304H178.3z"/>
      </symbol>
    </svg>
    <svg xmlns="http://www.w3.org/2000/svg">
      <symbol id="right-from-bracket" viewBox="0 0 512 512">
        <!--!Font Awesome Free 6.5.1 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2024 Fonticons, Inc.-->
        <path d="M377.9 105.9L500.7 228.7c7.2 7.2 11.3 17.1 11.3 27.3s-4.1 20.1-11.3 27.3L377.9 406.1c-6.4 6.4-15 9.9-24 9.9c-18.7 0-33.9-15.2-33.9-33.9l0-62.1-128 0c-17.7 0-32-14.3-32-32l0-64c0-17.7 14.3-32 32-32l128 0 0-62.1c0-18.7 15.2-33.9 33.9-33.9c9 0 17.6 3.6 24 9.9zM160 96L96 96c-17.7 0-32 14.3-32 32l0 256c0 17.7 14.3 32 32 32l64 0c17.7 0 32 14.3 32 32s-14.3 32-32 32l-64 0c-53 0-96-43-96-96L0 128C0 75 43 32 96 32l64 0c17.7 0 32 14.3 32 32s-14.3 32-32 32z"/>
      </symbol>
    </svg>
  </div>
</div>
