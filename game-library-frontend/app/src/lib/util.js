export function getCookie(name) {
  return document.cookie.split('; ')
    .find((c) => c.startsWith(`${name}=`))
    ?.split('=')[1];
}

export function setCookie(name, value, attributes = {}) {
  attributes = {
    path: '/',
    ...attributes
  };

  if (attributes.expires instanceof Date) {
    attributes.expires = attributes.expires.toUTCString();
  }

  let updatedCookie = encodeURIComponent(name) + "=" + encodeURIComponent(value);

  // NB: this is supposed to be 'in': attributes is not iterable with 'of'
  for (const attributeKey in attributes) {
    updatedCookie += "; " + attributeKey;
    let attributeValue = attributes[attributeKey];
    if (attributeValue !== true) {
      updatedCookie += "=" + attributeValue;
    }
  }

  document.cookie = updatedCookie;
}

export function parseJWT(token) {
  // https://stackoverflow.com/questions/38552003/how-to-decode-jwt-token-in-javascript-without-using-a-library
  const base64Url = token.split('.')[1];
  const base64 = base64Url.replaceAll('-', '+').replaceAll('_', '/');
  const jsonPayload = decodeURIComponent(
    window.atob(base64).split('').map(
      c => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2)
    ).join('')
  );
  return JSON.parse(jsonPayload);
}

export function truncateString(s, limit) {
  return s.length <= limit ? s : s.slice(0, limit-1) + '…';
}

export function formatSizeWithUnit(n) {
  const k = n > 0 ? Math.floor((Math.log2(n) / 10)) : 0;
  const unit = (k > 0 ? 'KMGT'[k - 1] + 'i' : '') + 'B';
  const count = Math.floor(n / 1024**k);
  return `${count} ${unit}`;
}

export function formatDistance(rtf, date, base) {
  // Based on https://www.builder.io/blog/relative-time,
  // modified for better rounding.

  // Get the amount of seconds between the date and base 
  const dsec = (date.getTime() - base.getTime()) / 1000;

  // One minute, hour, day, week, month, year in seconds
  const cutoffs = [
    60,
    3600,
    86400,
    86400 * 7,
    86400 * 30,
    86400 * 365,
    Infinity
  ];

  // Words for units, parallel to above
  const unit = [
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "year"
  ];

  // Find the ideal cutoff unit
  const unitIndex = cutoffs.findIndex(cutoff => cutoff > Math.abs(dsec));

  // Get the divisor to divide from the seconds. E.g., if our unit is "day"
  // our divisor is one day in seconds, so we can divide our seconds by
  // this to get the number of days
  const divisor = unitIndex ? cutoffs[unitIndex - 1] : 1;

  // Do the formatting, rounding toward zero
  return rtf.format(Math.trunc(dsec / divisor), unit[unitIndex]);
}

export function slugFor(s) {
  // percent-encode non-ascii
  return encodeURIComponent(
    // replace whitespace with hyphens
    s.replace(/\s/g, '-')
      // remove all special characters
      .replace(/[:\/?#\[\]@!$&'()*+,;=%"<>\\^`{}|]/g, '')
      // coalesce consecutive hyphens
      .replace(/-+/g, '-')
  )
  // remove leading and trailing hyphens
  .replace(/^-+|-+$/g, '');
}

export function isValidProjectName(name) {
  return !(
    name.length < 5 ||
    name.length > 64 ||
    name != name.trim() ||
    /[^\p{L}\p{M}\p{N}\p{P}\p{Z}]/u.test(name) ||
    /\s\s/.test(name)
  );
}
