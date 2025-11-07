import FlagsPage from './FlagsPage.svelte';
import { CONFIG } from './lib/setup.js';
import { mount } from "svelte";

// strip last two component from path to get base path
const base_url = window.location.pathname.split('/').slice(0, -2).join('/');

const app = mount(FlagsPage, {
  target: document.body,
  anchor: document.body.firstChild,
  props: {
    ...CONFIG,
    base_url
  }
});

export default app;
