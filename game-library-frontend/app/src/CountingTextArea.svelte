<script>
  let {
    name,
    placeholder,
    valid = $bindable(false),
    value = $bindable('')
  } = $props();

  const min_len = 10;
  const max_len = 500;

  function textLengthMessage(len) {
    if (len === 0) {
      return `enter at least ${min_len} characters`;
    }
    else if (len < min_len) {
      return `${min_len - len} more to go…`;
    }
    else {
      return `${max_len - len} remaining`;
    }
  }

  function updateValidFromEvent(event) {
    // check event.target.value.length instead of value.length because
    // value in textarea is not updated until after the input event
    updateValid(event.target.value);
  } 

  function updateValid(val) {
    valid = val.length >= min_len && val.length <= max_len;
  }

  export function revalidate() {
    updateValid(value);
  }

</script>

<style>

textarea {
  height: 5em;
}

</style>

<div>
  <textarea name={name} class="w-100 p-2" placeholder={placeholder} oninput={updateValidFromEvent} bind:value></textarea>
  <div class="text-secondary"><small>{textLengthMessage(value.length)}</small></div>
</div>
