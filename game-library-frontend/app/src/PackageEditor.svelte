<script>
  import { onMount } from 'svelte';

  let { pkg, packages } = $props();

  let pkg_names;
  let pkg_sort_keys;

  function startEdit() {
    pkg_names = new Set(packages.map((p) => p.name));
    pkg_sort_keys = new Set(packages.map((p) => p.sort_key));
  }

  onMount(startEdit);

  function validatePackageName(event) {
    event.target.setCustomValidity(
      pkg_names.has(event.target.value) ?
        "Package already exists" : ""
    );
  }

  function validatePackageSortKey(event) {
    let message = "";

    const sk = Number(event.target.value);
    if (!Number.isInteger(sk)) {
      message = "Sort key is not an integer";
    }

    if (pkg_sort_keys.has(sk)) {
      message = "Sort key already exists";
    }

    event.target.setCustomValidity(message);
  }
</script>

<svg class="svg-icon"><use xlink:href="#cube"></use></svg>
<label for="package_name_input" class="form-label">
  Package name
  <span title="A package is a way to structure the project into different parts.  A package name should say what this part contains, e.g., &quot;Module&quot;.  Package names are not release names&emdash;releases are contained within packages.">
    <svg class="svg-icon">
      <use xlink:href="#circle-question"></use>
    </svg>
  </span>
</label>
<input id="package_name_input"
       type="text"
       name="package_name"
       class="package_tmpl_name form-control"
       required
       title="Input the package name, for example &quot;Module&quot;"
       oninput={validatePackageName} value={pkg?.name ?? ""}>
<label for="sort_key_input"
       class="form-label">Package sort key
  <span title="The package sort key is an integer used to sort this package with respect to other packages. E.g., a package with a sort key of 0 will sort before a package with a sort key of 1.">
    <svg class="svg-icon">
      <use xlink:href="#circle-question"></use>
    </svg>
  </span>
</label>
<input id="sort_key_input"
       type="number"
       min={Number.MIN_SAFE_INTEGER}
       max={Number.MAX_SAFE_INTEGER}
       step="1"
       name="sort_key"
       class="package_tmpl_name form-control"
       required
       oninput={validatePackageSortKey} value={pkg?.sort_key ?? ""}>
