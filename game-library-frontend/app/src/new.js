import NewProjectPage from './NewProjectPage.svelte';
import { CONFIG } from './lib/setup.js';
import { mount } from "svelte";

const path = window.location.pathname;

// strip last component from path to get base path
const base_path = path.substring(0, path.lastIndexOf('/'));
const base_url = window.location.origin + base_path;

const app = mount(NewProjectPage, {
  target: document.body,
  anchor: document.body.firstChild,
  props: {
    ...CONFIG,
    base_url
  }
});

export default app;
