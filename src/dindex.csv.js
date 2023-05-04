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
  console.log('** dindex.csv.js **');
  console.log();
  console.log('Reads the most recent dark RdRp index file from S3, downloads the');
  console.log('alignment summaries into a local cache and creates csv files for');
  console.log('the dfamily, dphylum and dsequence tables.');
  console.log();
  console.log('Usage:');
  console.log('  node dindex.csv.js [...]');
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
  console.log('    creates dfamily, dphylum and dsequence files from the local cache');
  console.log('  help (optional):');
  console.log('    shows this message');
  console.log('  phy (debug, optional):');
  console.log('    parses the summary files in the local cache and returns a unique');
  console.log('    list of all the phy entries found');
  console.log();
  console.log('Example:');
  console.log('  node dindex.csv.js --data-dir=data --cache --csv');

  process.exit(0);
}

if(!ARGV['data-dir']) {
  console.error('You must supply a valid data-dir argument');
  console.log('  see node dindex.csv.js --help');

  process.exit(1);
}

const SERRATUS_S3_ENDPOINT = ARGV['serratus-s3-endpoint'] || 'https://lovelywater2.s3.amazonaws.com';

console.log('/data/dindex.tsv');
try { await access(path.join(ARGV['data-dir'], 'dindex.tsv')); }
catch(e) {
  console.time('dindex.csv (fetch)');

  const res = await fetch(SERRATUS_S3_ENDPOINT + '/dindex.tsv');
  const readableFromWeb = Readable.fromWeb(res.body);
  
  await finished(readableFromWeb.pipe(createWriteStream(path.join(ARGV['data-dir'], 'dindex.tsv'))));
  console.timeEnd('dindex.csv (fetch)');
}

console.log('dindex.tsv ' + await new Promise(resolve => exec('wc ' + path.join(ARGV['data-dir'], 'dindex.tsv'), (e, so, se) => resolve(so))));

const dIndexLevel = new Level(path.join(ARGV['data-dir'], 'level/dindex'), { valueEncoding:'binary' });

if(ARGV.cache) {
  var [N, M] = [0, 0];
  console.time('dindex.csv (cache)');
  const asyncPool = new AsyncPool({ n:128 });
  for await (const line of createInterface({ input:createReadStream(path.join(ARGV['data-dir'], 'dindex.tsv')) })) {
    var m = undefined;

    if((m = line.match(/(\d+)\s+(\S+?)\.psummary$/)) && m[1] > 140) {
      asyncPool.push((m_2 => async () => {

        try { await dIndexLevel.get(m_2); }
        catch(e) {
          const dSummary = await S3.fetchDSummary(m_2);

          if(dSummary) {
            const dSummaryObject = summaryToObject(dSummary);

            const dSummaryObjectGzip = await promisify(gzip)(JSON.stringify(dSummaryObject), { level:constants.Z_MAX_LEVEL });

            await dIndexLevel.put(m_2, dSummaryObjectGzip);
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
  console.timeEnd('dindex.csv (cache)');
}

if(ARGV.csv) {
  var N = 0;
  console.time('dindex.csv (csv)');
  const dFamilyCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'dfamily.csv.gz') });
  const dPhylumCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'dphylum.csv.gz') });
  const dSequenceCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'dsequence.csv.gz') });
  for await (let [key, value] of dIndexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.famcvg).forEach(v => dFamilyCSV.write(famcvgToFamily(v)));
    value.filter(v => v.phycvg).forEach(v => dPhylumCSV.write(phycvgToPhylum(v)));
    value.filter(v => v.vircvg).forEach(v => dSequenceCSV.write(vircvgToSequence(v)));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(N > 1024*1024*8)
      break;
  }
  dFamilyCSV.end();
  dPhylumCSV.end();
  dSequenceCSV.end();
  console.timeEnd('dindex.csv (csv)');
}

if(ARGV.phy) {
  var N = 0;
  console.time('dindex.csv (phy)');
  var PHY = new Set();
  for await (let [key, value] of dIndexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.phycvg).forEach(v => PHY.add(v.phy));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(N > 1024*1024*8)
      break;
  }
  console.log(Array.from(PHY).sort().join('\n'));
  console.timeEnd('dindex.csv (phy)');
}
