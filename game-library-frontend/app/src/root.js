import RootPage from './RootPage.svelte';
import { CONFIG } from './lib/setup.js';
import { mount } from "svelte";

const base_url = window.location.origin + window.location.pathname;

const app = mount(RootPage, {
  target: document.body,
  anchor: document.body.firstChild,
  props: {
    ...CONFIG,
    base_url
  }
});

export default app;
