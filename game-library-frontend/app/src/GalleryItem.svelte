<script>
  import DOMPurify from 'https://cdn.jsdelivr.net/npm/dompurify@3.2.6/+esm';

  let {
    img,
    src,
    md
  } = $props();

</script>

<style>

figure:hover img {
  transition: filter 0.15s ease-in-out;
  filter: brightness(0.75);
}

</style>

<figure class="figure col-lg-3 col-md-4 col-6 px-1">
  <a href="{src}"><img class="figure-img img-fluid img-thumbnail" src="{src}" alt=""></a>
  <figcaption class="figure-caption text-center">
    {@html DOMPurify.sanitize(md.renderInline(img.description))}
  </figcaption>
</figure>
