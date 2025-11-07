import { parseJWT, setCookie } from './util.js';

class APIError extends Error {
  constructor(status, statusText, message) {
    super(message);
    this.status = status;
    this.statusText = statusText;
  }

  name = this.constructor.name;
}

APIError.prototype.toString = function () {
  return `APIError: ${this.status} ${this.statusText}: ${this.message}`;
};

class NetworkError extends Error {
  constructor(cause) {
    if (cause) {
      super("", { cause: cause });
    }
    else {
      super();
    }
  }

  name = this.constructor.name;
}

NetworkError.prototype.toString = function () {
  return 'NetworkError: Request failed' + (this.cause ? `: ${this.cause}` : '');
};

class TimeoutError extends Error {
  name = this.constructor.name;
}

TimeoutError.prototype.toString = function () {
  return 'TimeoutError: Request timed out';
};

const UPLOAD_OK = 'UploadOk';

const UPLOAD_ABORTED = 'UploadAborted';

function extractError(response) {
  try {
    return JSON.parse(response)?.error ?? response;
  }
  catch {
    // it's not JSON, just return it verbatim
    return response;
  }
}

async function extractFetchError(response) {
  // The response body is a stream, so we can't call json() and then
  // subsequently call text() on it if json() fails---the stream will
  // already be consumed in that case.
  try {
    response = await response.text();
  }
  catch {
    return "Failed to extract response text";
  }

  return extractError(response);
}

async function checkError(response) {
  if (!response.ok) {
    throw new APIError(
      response.status,
      response.statusText,
      await extractFetchError(response)
    );
  }
}

async function doFetch(url, options={}) {
  let response;
  try {
    response = await fetch(url, options);
  }
  catch (err) {
    if (err.name === 'TimeoutError') {
      throw new TimeoutError;
    }
    else {
      throw new NetworkError(err);
    }
  }

  await checkError(response);
  return response;
}

async function fetchOk(url, options={}) {
  await doFetch(url, options);
}

export async function fetchJSON(url, options={}) {
  return (await doFetch(url, options)).json();
}

function doUpload(file, type, url, token, callbacks) {
  const xhr = new XMLHttpRequest();

  const promise = new Promise((resolve, reject) => {
    // We listen on the load event on the xhr instead of xhr.upload because
    // the upload load event fires when all the data is sent, not when the
    // request completes.
    xhr.addEventListener('load', () => {
      if (xhr.status === 200) {
        resolve(UPLOAD_OK);
      }
      else {
        reject(new APIError(
          xhr.status,
          xhr.statusText,
          extractError(xhr.response)
        ));
      }
    });

    xhr.upload.addEventListener('abort', () => resolve(UPLOAD_ABORTED));
    xhr.upload.addEventListener('error', () => reject(new NetworkError));
    xhr.upload.addEventListener('timeout', () => reject(new TimeoutError));

    for (const [evt, cb] of Object.entries(callbacks)) {
      xhr.upload.addEventListener(evt, cb);
    }

    xhr.open('POST', url);
    xhr.setRequestHeader('Authorization', `Bearer ${token}`);
    if (type) {
      xhr.setRequestHeader('Content-Type', type);
    }
    xhr.send(file);
  });

  return [xhr, promise];
}

function isTokenExpired(token) {
  // Date.now() is milliseconds, token exp is seconds
  return Date.now() / 1000 >= parseJWT(token).exp
}

class Client {
  constructor(gls_api, ums_api, project, token, refresh) {
    this.gls_api = gls_api;
    this.ums_api = ums_api;
    this.project = project;
    this.token = token;
    this.refresh = refresh;
    this.fetch_timeout = 10000; // ms
  }

  async fetchJSON(url, options={}) {
    return await fetchJSON(
      url,
      {
        signal: AbortSignal.timeout(this.fetch_timeout),
        ...options
      }
    );
  }

  async fetchOk(url, options={}) {
    await fetchOk(
      url,
      {
        signal: AbortSignal.timeout(this.fetch_timeout),
        ...options
      }
    );
  }

  authHeaders() {
    return { 'Authorization': `Bearer ${this.token}` }
  }

  authJSONHeaders() {
    return {
      'Authorization': `Bearer ${this.token}`,
      'Content-Type': 'application/json'
    }
  }

  async refreshAccessToken() {
    return this.fetchJSON(
      `${this.ums_api}/refresh`,
      {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${this.refresh}` }
      }
    );
  }

  async refreshTokenIfExpired() {
    if (!this.token || isTokenExpired(this.token)) {
      this.token = (await this.refreshAccessToken()).token;
      const parsed_token = parseJWT(this.token);
      // Date() wants milliseconds, token exp is in seconds
      const expires = new Date(parsed_token.exp * 1000);
      setCookie('token', this.token, { expires: expires, secure: true });
    }
  }

  async getProject() {
    return this.fetchJSON(`${this.gls_api}/projects/${this.project}`);
  }

  async createProject(data) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}`,
      {
        method: 'POST',
        headers: this.authJSONHeaders(),
        body: JSON.stringify(data)
      }
    );
  }

  async updateProject(data) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}`,
      {
        method: 'PATCH',
        headers: this.authJSONHeaders(),
        body: JSON.stringify(data)
      }
    );
  }

  async getPlayers() {
    return this.fetchJSON(`${this.gls_api}/projects/${this.project}/players`);
  }

  async addPlayer() {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/players`,
      {
        method: 'PUT',
        headers: this.authHeaders()
      }
    );
  }

  async removePlayer() {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/players`,
      {
        method: 'DELETE',
        headers: this.authHeaders()
      }
    );
  }

  async addOwners(owners) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/owners`,
      {
        method: 'PUT',
        headers: this.authJSONHeaders(),
        body: JSON.stringify({ 'users': owners })
      }
    );
  }

  async removeOwners(owners) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/owners`,
      {
        method: 'DELETE',
        headers: this.authJSONHeaders(),
        body: JSON.stringify({ 'users': owners })
      }
    );
  }

  async addPackage(pkg, data) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/packages/${pkg}`,
      {
        method: 'POST',
        headers: this.authJSONHeaders(),
        body: JSON.stringify(data)
      }
    );
  }

  async updatePackage(pkg, data) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/packages/${pkg}`,
      {
        method: 'PATCH',
        headers: this.authJSONHeaders(),
        body: JSON.stringify(data)
      }
    );
  }

  async removePackage(pkg) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/packages/${pkg}`,
      {
        method: 'DELETE',
        headers: this.authHeaders()
      }
    );
  }

  async addRelease(pkg, version) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/packages/${pkg}/${version}`,
      {
        method: 'POST',
        headers: this.authHeaders()
      }
    );
  }

  async removeRelease(pkg, version) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/packages/${pkg}/${version}`,
      {
        method: 'DELETE',
        headers: this.authHeaders()
      }
    );
  }

  static UPLOAD_OK = UPLOAD_OK;

  static UPLOAD_ABORTED = UPLOAD_ABORTED;

  async addFile(pkg, version, file, type, callbacks={}) {
    await this.refreshTokenIfExpired();
    return doUpload(
      file,
      type,
      `${this.gls_api}/projects/${this.project}/packages/${pkg}/${version}/${file.name}`,
      this.token,
      callbacks
    );
  }

  async addImage(imgname, file, type, callbacks={}) {
    await this.refreshTokenIfExpired();
    return doUpload(
      file,
      type,
      `${this.gls_api}/projects/${this.project}/images/${imgname}`,
      this.token,
      callbacks
    );
  }

  imageUrl(filename) {
    const enc_filename = encodeURIComponent(filename);
    return `${this.gls_api}/projects/${this.project}/images/${enc_filename}`;
  }

  async addGalleryImage(imgname, file, type, callbacks={}) {
    await this.refreshTokenIfExpired();
    return doUpload(
      file,
      type,
      `${this.gls_api}/projects/${this.project}/gallery/${imgname}`,
      this.token,
      callbacks
    );
  }

  async updateGallery(data) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/gallery`,
      {
        method: 'PATCH',
        headers: this.authJSONHeaders(),
        body: JSON.stringify(data)
      }
    );
  }

  async addFlag(flag, message) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/projects/${this.project}/flag`,
      {
        method: 'POST',
        headers: this.authJSONHeaders(),
        body: JSON.stringify({ 'flag': flag, 'message': message })
      }
    );
  }

  async closeFlag(flag) {
    await this.refreshTokenIfExpired();
    return this.fetchOk(
      `${this.gls_api}/admin/flags/${flag}`,
      {
        method: 'PATCH',
        headers: this.authHeaders()
      }
    );
  }

  async getFlags() {
    await this.refreshTokenIfExpired();
    return this.fetchJSON(
      `${this.gls_api}/admin/flags`,
      {
        method: 'GET',
        headers: this.authJSONHeaders()
      }
    );
  }
}

export default Client;
