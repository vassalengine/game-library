import { getCookie } from './util.js';

const gls_url = 'http://localhost:3000/api/v1';
const ums_url = 'http://localhost:4000/api/v1';
const discourse_url = 'https://forum.vassalengine.org';

const current_version = '3.7.16';
const news_link = 'https://vassalengine.org/library'

function getUserInfo(ums) {
  const username = getCookie('username');
  if (!username) {
    return null;
  }

  let name = getCookie('name');
  if (name !== null) {
    name = decodeURI(name);
  }

  return {
    username,
    name,
    avatar_url: `${ums}/users/${username}/avatar/48`
  };
}

function returnToFor(logged_in, ret, ums) {
  // construct the login/logout returnto
  const here = encodeURIComponent(ret);
  const action = logged_in ? 'Logout' : 'Login';
  return encodeURIComponent(`${ums}/sso/complete${action}?returnto=${here}`);
}

const user_info = getUserInfo(ums_url);

export const CONFIG = {
  discourse_url,
  gls_url,
  ums_url,
  current_version,
  news_link,
  user_info,
  returnto: returnToFor(user_info, window.location.href, ums_url)
};
