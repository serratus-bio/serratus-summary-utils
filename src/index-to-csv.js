import { exec } from 'child_process';
import { createReadStream, createWriteStream } from 'fs';
import { access, mkdir, readFile } from 'fs/promises';
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
import { createHash } from 'crypto';

const ARGV = minimist(process.argv.slice(2));

if(ARGV.help) {
  console.log('** index-to-csv.js **');
  console.log();
  console.log('Reads the most recent index file from S3, downloads the alignment');
  console.log('summaries into a local cache and creates csv files for');
  console.log('the family, phylum and sequence tables.');
  console.log();
  console.log('Usage:');
  console.log('  node index-to-csv.js [...]');
  console.log();
  console.log('Parameters:');
  console.log('  data-dir (required):');
  console.log('    directory to use for caching and file output');
  console.log('    (to start from scratch just delete this directory)');
  console.log('  index-path (required):');
  console.log('    path on S3 where the index file is stored');
  console.log('  n (optional):');
  console.log('    number of records to process');
  console.log('    if not set, it goes through all of them');
  console.log('  phylum-name-dictionary-path (optional):');
  console.log('    path for a JSON file used to resolve phylum names');
  console.log('  serratus-s3-endpoint (optional):');
  console.log('    hostname for serratus public data on S3');
  console.log('    default: https://lovelywater2.s3.amazonaws.com');
  console.log('  summary-path (required):');
  console.log('    path on S3 where the summary files are stored');
  console.log('    variables: $ID');
  console.log();
  console.log('  cache (optional):');
  console.log('    downloads all summary files from S3 to the local cache');
  console.log('  csv (optional):');
  console.log('    creates family, phylum and sequence files from the local cache');
  console.log('  help (optional):');
  console.log('    shows this message');
  console.log('  phy (debug, optional):');
  console.log('    parses the summary files in the local cache and returns a unique');
  console.log('    list of all the phy entries found');
  console.log();
  console.log('Examples:');
  console.log('  (dsummmary)');
  console.log('  node index-to-csv.js --data-dir=data --index-path=dindex.tsv --summary-path=dsummary/\\\$ID.psummary --cache --csv');
  console.log('  (rsummmary)');
  console.log('  node index-to-csv.js --data-dir=data --index-path=rindex.tsv --summary-path=rsummary/\\\$ID.psummary --phylum-name-dictionary-path=src/rsummary.dictionary.json --cache --csv');

  process.exit(0);
}

if(!ARGV['data-dir']) {
  console.error('You must supply a \'data-dir\' argument');
  console.log('  see node index-to-csv.js --help');

  process.exit(1);
}

if(!ARGV['index-path']) {
  console.error('You must supply an \'index-path\' argument');
  console.log('  see node index-to-csv.js --help');

  process.exit(1);
}

if(ARGV['phylum-name-dictionary-path']) {
  try { await access(ARGV['phylum-name-dictionary-path']); }
  catch(e) {
    console.error('\'phylum-name-dictionary-path\' is either not there or inaccesible');
    console.log('  see node index-to-csv.js --help');

    process.exit(1);
  }
}

if(!ARGV['summary-path']) {
  console.error('You must supply an \'summary-path\' argument');
  console.log('  see node index-to-csv.js --help');

  process.exit(1);
}

const INDEX_PATH_MD5 = createHash('md5').update(ARGV['index-path']).digest('hex');
const SERRATUS_S3_ENDPOINT = ARGV['serratus-s3-endpoint'] || 'https://lovelywater2.s3.amazonaws.com';
let PHYLUM_NAME_DICTIONARY = undefined;
if(ARGV['phylum-name-dictionary-path']) {
  try {
    PHYLUM_NAME_DICTIONARY = JSON.parse(await readFile(ARGV['phylum-name-dictionary-path']));
  } catch(e) {}
}

try { await mkdir(path.join(ARGV['data-dir'])); }
catch(e) {}

try { await mkdir(path.join(ARGV['data-dir'], 'cache')); }
catch(e) {}

try { await mkdir(path.join(ARGV['data-dir'], 'out')); }
catch(e) {}

try { await access(path.join(ARGV['data-dir'], 'cache', ARGV['index-path'])); }
catch(e) {
  console.log('Local \'' + ARGV['index-path'] + '\' not found, downloading ...');
  console.time(ARGV['index-path'] + ' (fetch)');

  const res = await fetch(SERRATUS_S3_ENDPOINT + '/' + ARGV['index-path']);
  const readableFromWeb = Readable.fromWeb(res.body);
  
  await finished(readableFromWeb.pipe(createWriteStream(path.join(ARGV['data-dir'], 'cache', ARGV['index-path']))));
  console.timeEnd(ARGV['index-path'] + ' (fetch)');
}

console.log(ARGV['index-path'] + ' ' + await new Promise(resolve => exec('wc ' + path.join(ARGV['data-dir'], 'cache', ARGV['index-path']), (e, so, se) => resolve(so))));

const indexLevel = new Level(path.join(ARGV['data-dir'], 'level', INDEX_PATH_MD5), { valueEncoding:'binary' });

if(ARGV.cache) {
  var [N, M] = [0, 0];
  console.time(ARGV['index-path'] + ' (cache)');
  const asyncPool = new AsyncPool({ n:128 });
  for await (const line of createInterface({ input:createReadStream(path.join(ARGV['data-dir'], 'cache', ARGV['index-path'])) })) {
    var m = undefined;

    if((m = line.match(/(\d+)\s+(\S+?)\.psummary$/)) && m[1] > 140) {
      asyncPool.push((m_2 => async () => {
        const summaryPath = ARGV['summary-path'].replace(/\$ID/g, m_2);

        try { await indexLevel.get(summaryPath); }
        catch(e) {
          const summary = await S3.fetchSummary(summaryPath);

          if(summary) {
            const summaryObject = summaryToObject(summary);

            const summaryObjectGzip = await promisify(gzip)(JSON.stringify(summaryObject), { level:constants.Z_MAX_LEVEL });

            await indexLevel.put(summaryPath, summaryObjectGzip);
          }
        };

        if(++M%1000 === 0)
          console.log('M', M.toLocaleString(), '/', (asyncPool.push._||[]).length.toLocaleString());
      })(m[2]));

      if(ARGV.n && ++N > ARGV.n)
        break;
    }
  }
  await asyncPool.flush(true);
  console.timeEnd(ARGV['index-path'] + ' (cache)');
}

if(ARGV.csv) {
  var N = 0;
  console.time(ARGV['index-path'] + ' (csv)');
  const familyCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'out', 'family.csv.gz') });
  const phylumCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'out', 'phylum.csv.gz') });
  const sequenceCSV = new CSVGzipStream({ path:path.join(ARGV['data-dir'], 'out', 'sequence.csv.gz') });
  for await (let [key, value] of indexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.famcvg).forEach(v => familyCSV.write(famcvgToFamily(v, { phylumNameDictionary:PHYLUM_NAME_DICTIONARY })));
    value.filter(v => v.phycvg).forEach(v => phylumCSV.write(phycvgToPhylum(v, { phylumNameDictionary:PHYLUM_NAME_DICTIONARY })));
    value.filter(v => v.vircvg).forEach(v => sequenceCSV.write(vircvgToSequence(v, { phylumNameDictionary:PHYLUM_NAME_DICTIONARY })));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(ARGV.n && ++N > ARGV.n)
      break;
  }
  familyCSV.end();
  phylumCSV.end();
  sequenceCSV.end();
  console.timeEnd(ARGV['index-path'] + ' (csv)');
}

if(ARGV.phy) {
  var N = 0;
  console.time(ARGV['index-path'] + ' (phy)');
  var PHY = new Set();
  for await (let [key, value] of indexLevel.iterator()) {
    try { value = JSON.parse(await promisify(gunzip)(value)); }
    catch(e) { value = null; }

    value.filter(v => v.phycvg).forEach(v => PHY.add(v.phy));

    if(++N%1000 === 0)
      console.log('N', N.toLocaleString());

    if(ARGV.n && ++N > ARGV.n)
      break;
  }
  console.log(Array.from(PHY).sort().join('\n'));
  console.timeEnd(ARGV['index-path'] + ' (phy)');
}
