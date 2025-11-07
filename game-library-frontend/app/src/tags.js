import TagsPage from './TagsPage.svelte';
import { CONFIG } from './lib/setup.js';
import { mount } from "svelte";

const limit = 50;

const path = window.location.pathname;

// strip last component from path to get base path
const base_path = path.substring(0, path.lastIndexOf('/'));
const base_url = window.location.origin + base_path;

const app = mount(TagsPage, {
  target: document.body,
  anchor: document.body.firstChild,
  props: {
    ...CONFIG,
    base_url,
    limit
  }
});

export default app;
