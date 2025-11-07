export function makeRequestURL(api_url, params, limit) {
  // Construct API request
  const req_url = new URL(`${api_url}/projects`);

  for (const [k, v] of params.entries()) {
    req_url.searchParams.append(k, v);
  }

  if (!req_url.searchParams.has('limit')) {
    req_url.searchParams.set('limit', limit);
  }

  return req_url;
}

export function normalizeWhitespace(s) {
  return s.trim().split(/\s+/).filter(s => s.length > 0).join(' ');
}

export function cleanupSearch(event) {
  const fdata = event.formData;
  for (const k of fdata.keys()) {
    fdata.set(k, normalizeWhitespace(fdata.get(k)));
  }
}

export class AutocompleteFetcher {
  constructor(getCache, putCache, fetchItemsFor) {
    this.getCache = getCache;
    this.putCache = putCache;
    this.fetchItemsFor = fetchItemsFor;

    this.interval = 200;
    this.current_search_counter = 0;
    this.prev_call = Promise.resolve();
  }

  async searchItems(key) {
    if (key === '') {
      return [];
    }

    // check the cache
    let search_items = this.getCache(key);
    if (search_items !== undefined) {
      return search_items;
    }

    const my_counter = ++this.current_search_counter;

    // wait interval ms after the previous request
    await this.prev_call;
    this.prev_call = new Promise((resolve) => setTimeout(resolve, this.interval));

    if (my_counter !== this.current_search_counter) {
      // there is a newer search; don't update using this one
      return false;
    }

    // do the search
    search_items = await this.fetchItemsFor(key);
    this.putCache(key, search_items);

    if (my_counter !== this.current_search_counter) {
      // there is a newer search; don't update using this one
      return false;
    }

    return search_items;
  }
}

export default AutocompleteFetcher;
