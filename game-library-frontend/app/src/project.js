import ProjectPage from './ProjectPage.svelte';
import { CONFIG } from './lib/setup.js';
import { mount } from "svelte";

// determine what project to display
const path = window.location.pathname;
const i = path.lastIndexOf('/');
const project = path.substring(i + 1);

// strip last component from path to get base path
const base_path = path.substring(0, path.lastIndexOf('/', i - 1));
const base_url = window.location.origin + base_path;

let proj = JSON.parse(document.getElementById("project-data")?.content ?? null);
let proj_error = document.getElementById("project-error")?.content ?? null;

let players = JSON.parse(document.getElementById("players-data")?.content ?? null);
let players_error = document.getElementById("players-error")?.content ?? null;

const app = mount(ProjectPage, {
  target: document.body,
  anchor: document.body.firstChild,
  props: {
    ...CONFIG,
    base_url,
    project,
    proj,
    proj_error,
    players,
    players_error
  }
});

export default app;
