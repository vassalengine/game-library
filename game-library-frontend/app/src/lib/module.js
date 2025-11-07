import { BlobReader, TextWriter, ZipReader } from 'https://cdn.jsdelivr.net/npm/@zip.js/zip.js@2.7.62/+esm';

async function getFileEntry(zipreader, filename) {
  try {
    for await (const e of zipreader.getEntriesGenerator()) {
      if (!e.directory && e.filename === filename) {
        return e;
      }
    }
  }
  catch {
  }

  return null;
}

async function dumpFile(zipfile, filename) {
  const reader = new BlobReader(zipfile);
  const zipreader = new ZipReader(reader);
  const entry = await getFileEntry(zipreader, filename);
  return await entry?.getData(new TextWriter());
}

function versionInModuledata(md) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(md, 'application/xml');

  // This is the entirely crazy way XML parsing errors are reported
  if (doc.querySelector('parsererror')) {
    return null;
  }

  return doc.evaluate(
    '/data/version',
    doc,
    null,
    XPathResult.STRING_TYPE,
    null
  ).stringValue.trim();
}

export async function extractVersion(zipfile) {
  const md = await dumpFile(zipfile, 'moduledata');
  return md ? versionInModuledata(md) : null;
}
