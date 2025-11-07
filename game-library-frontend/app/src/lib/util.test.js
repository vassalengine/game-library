import {
  formatDistance,
  formatSizeWithUnit,
  isValidProjectName,
  slugFor,
  truncateString
} from './util.js';

import { expect, test } from 'vitest';

test.each([
  ['abc', 4, 'abc'],
  ['abcd', 4, 'abcd'],
  ['abcde', 4, 'abc…'],
  ['abcdef', 4, 'abc…']
])(
  'truncateString %s %i',
  (s, limit, exp) => expect(truncateString(s, limit)).toEqual(exp)
);

test.each([
  [0, '0 B'],
  [1, '1 B'],
  [1023, '1023 B'],
  [1024, '1 KiB'],
  [1048575, '1023 KiB'],
  [1048576, '1 MiB'],
  [1073741823, '1023 MiB'],
  [1073741824, '1 GiB'],
  [1099511627775, '1023 GiB'],
  [1099511627776, '1 TiB']
])(
  'formatSizeWithUnit %i %s',
  (n, exp) => expect(formatSizeWithUnit(n)).toEqual(exp)
);

const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });
const base = new Date(0);

test.each([
  [0, 'now'],
  [999, 'now'],
  [1000, '1 second ago'],
  [1999, '1 second ago'],
  [2000, '2 seconds ago'],
  [60 * 1000 - 1, '59 seconds ago'],
  [60 * 1000, '1 minute ago'],
  [2 * 60 * 1000 - 1, '1 minute ago'],
  [2 * 60 * 1000, '2 minutes ago'],
  [60 * 60 * 1000 - 1, '59 minutes ago'],
  [60 * 60 * 1000, '1 hour ago'],
  [2 * 60 * 60 * 1000 - 1, '1 hour ago'],
  [2 * 60 * 60 * 1000, '2 hours ago'],
  [24 * 60 * 60 * 1000 - 1, '23 hours ago'],
  [24 * 60 * 60 * 1000, 'yesterday'],
  [2 * 24 * 60 * 60 * 1000 - 1, 'yesterday'],
  [2 * 24 * 60 * 60 * 1000, '2 days ago'],
  [7 * 24 * 60 * 60 * 1000 - 1, '6 days ago'],
  [7 * 24 * 60 * 60 * 1000, 'last week'],
  [2 * 7 * 24 * 60 * 60 * 1000 - 1, 'last week'],
  [2 * 7 * 24 * 60 * 60 * 1000, '2 weeks ago'],
  [30 * 24 * 60 * 60 * 1000 - 1, '4 weeks ago'],
  [30 * 24 * 60 * 60 * 1000, 'last month'],
  [2 * 30 * 24 * 60 * 60 * 1000 - 1, 'last month'],
  [2 * 30 * 24 * 60 * 60 * 1000, '2 months ago'],
  [365 * 24 * 60 * 60 * 1000 - 1, '12 months ago'], // uniform 30-day months!
  [365 * 24 * 60 * 60 * 1000, 'last year'],
  [2 * 365 * 24 * 60 * 60 * 1000 - 1, 'last year'],
  [2 * 365 * 24 * 60 * 60 * 1000, '2 years ago']
])(
  'formatDistance %s %s',
  (d, exp) => expect(formatDistance(rtf, base, new Date(d))).toEqual(exp)
);

test.each([
  ['abcd' , 'abcd'],
  ['x      x', 'x-x'],
  ['x----x---x', 'x-x-x'],
  ['-x-', 'x'],
  ['x/#?*x', 'xx'],
  ['x💩x', 'x%F0%9F%92%A9x']
])(
  'slugFor "%s"',
  (name, exp) => expect(slugFor(name)).toEqual(exp)
);

test.each([
  ['', false],
  ['abcd', false],
  ['abcde', true],
  ['ab de', true],
  ['x'.repeat(65), false],
  [' x ', false],
  ['x  x', false],
  ['x💩x', false]
])(
  'isValidProjectName "%s"',
  (name, exp) => expect(isValidProjectName(name)).toEqual(exp)
);
