import { exec } from 'child_process';
import { createReadStream, createWriteStream } from 'fs';
import { access } from 'fs/promises';
import { Level } from 'level';
import minimist from 'minimist';
import path from 'path';
import { createInterface } from 'readline';
import { Readable } from 'stream';
import { finished } from 'stream/promises';
import { promisify } from 'util';
import { constants, gunzip, gzip } from 'zlib';

import { AsyncPool, CSVGzipStream } from './util.js';
import { S3, famcvgToFamily, phycvgToPhylum, summaryToObject, vircvgToSequence } from './util.serratus.js';

const ARGV = minimist(process.argv.slice(2));

if(ARGV.help) {
  console.log('** rindex.csv.js **');
  console.log();
  console.log('Reads the most recent RdRp index file from S3, downloads the alignment');
  console.log('summaries into a local cache and creates csv files for the rfamily,');
  console.log('rphylum and rsequence tables.');
  console.log();
  console.log('Usage:');
  console.log('  node rindex.csv.js [...]');
  console.log();
  console.log('Parameters:');
  console.log('  data-dir (required):');
  console.log('    directory to use for caching and file output');
  console.log('    (to start from scratch just delete this directory)');
  console.log('  serratus-s3-endpoint (optional):');
  console.log('    hostname for serratus public data on S3');
  console.log('    default: https://lovelywater2.s3.amazonaws.com');
  console.log();
  console.log('  cache (optional):');
  console.log('    downloads all summary files from S3 to the local cache');
  console.log('  csv (optional):');
  console.log('    creates rfamily, rphylum and rsequence files from the local cache');
  console.log('  help (optional):');
  console.log('    shows this message');
  console.log('  phy (debug, optional):');
  console.log('    parses the summary files in the local cache and returns a unique');
  console.log('    list of all the phy entries found');
  console.log();
  console.log('Example:');
  console.log('  node rindex.csv.js --data-dir=data --cache --csv');

  process.exit(0);
}

if(!ARGV['data-dir']) {
  console.error('You must supply a valid data-dir argument');
  console.log('  see node rindex.csv.js --help');

  process.exit(1);
}

const SERRATUS_S3_ENDPOINT = ARGV['serratus-s3-endpoint'] || 'https://lovelywater2.s3.amazonaws.com';

console.log('/data/rindex.tsv');
try { await access(path.join(ARGV['data-dir'], 'rindex.tsv')); }
catch(e) {
  console.time('rindex.tsv (fetch)');

  const res = await fetch(SERRATUS_S3_ENDPOINT + '/rindex.tsv');
  const readableFromWeb = Readable.fromWeb(res.body);
  
  await finished(readableFromWeb.pipe(createWriteStream(path.join(ARGV['data-dir'], 'rindex.tsv'))));
  console.timeEnd('rindex.tsv (fetch)');
}

console.log('rindex.tsv ' + await new Promise(resolve => exec('wc ' + path.join(ARGV['data-dir'], 'rindex.tsv'), (e, so, se) => resolve(so))));

const rIndexLevel = new Level(path.join(ARGV['data-dir'], 'level/rindex'), { valueEncoding:'binary' });

if(ARGV.cache) {
  var [N, M] = [0, 0];
  console.time('rindex.tsv (cache)');
  const asyncPool = new AsyncPool({ n:128 });
  for await (const line of createInterface({ input:createReadStream(path.join(ARGV['data-dir'], 'rindex.tsv')) })) {
    var m = undefined;

    if((m = line.match(/(\d+)\s+(\S+?)\.psummary$/)) && m[1] > 140) {
      asyncPool.push((m_2 => async () => {
        try { await rIndexLevel.get(m_2); }
        catch(e) {
          const rSummary = await S3.fetchRSummary(m_2);

          if(rSummary) {
            const rSummaryObject = summaryToObject(rSummary);

            const rSummaryObjectGzip = await promisify(gzip)(JSON.stringify(rSummaryObject), { level:constants.Z_MAX_LEVEL });

            await rIndexLevel.put(m_2, rSummaryObjectGzip);
          }
        };

        if(++M%1000 === 0)
          console.log('M', M.toLocaleString(), '/', (asyncPool.push._||[]).length.toLocaleString());
      })(m[2]));

      if(++N > 1024*1024*8)
        break;
    }
  }
  await asyncPool.flush(true);
  console.timeEnd('rindex.tsv (cache)');
}

if(ARGV.csv) {
  var N = 0;
  console.time('rindex.tsv (csv)');
  const rFamilyCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], '/data/rfamily.csv.gz') });
  const rPhylumCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], '/data/rphylum.csv.gz') });
  const rSequenceCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], '/data/rsequence.csv.gz') });
  for await (let [key, value] of rIndexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.famcvg).forEach(v => rFamilyCSV.write(famcvgToFamily(v, true)));
    value.filter(v => v.phycvg).forEach(v => rPhylumCSV.write(phycvgToPhylum(v, true)));
    value.filter(v => v.vircvg).forEach(v => rSequenceCSV.write(vircvgToSequence(v, true)));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(N > 1024*1024*8)
      break;
  }
  rFamilyCSV.end();
  rPhylumCSV.end();
  rSequenceCSV.end();
  console.timeEnd('rindex.tsv (csv)');
}

if(ARGV.phy) {
  var N = 0;
  console.time('rindex.tsv (phy)');
  var PHY = new Set();
  for await (let [key, value] of rIndexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.phycvg).forEach(v => PHY.add(v.phy));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(N > 1024*1024*8)
      break;
  }
  console.log(Array.from(PHY).sort().join('\n'));
  console.timeEnd('rindex.tsv (phy)');
}
