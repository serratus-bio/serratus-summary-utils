'use strict';

import { createInterface } from 'readline';

let flush
let n = 0;

// Dictionary used to filter lines according to their phylumn_name
// if phylum_name matches any regex in tr it goes through, otherwise it doesn't
// if the second parameter is a string, the phylum_name gets replaced with it
//
// header line always goes through
//
// only family, phylum and sequence files need to be filtered through this
const tr = [
  [/^PH*/, true],
  [/^OBLI$/, 'obli2'],
  [/^obli$/, 'obli1']
];

createInterface({
  input:process.stdin,
  terminal:false
})
  .on('line', line => {
    if(n == 0)
      process.stdout.write(line + '\n');
    else {
      const a = line.split(/,/);

      const match = tr.filter(v => v[0].test(a[1]))[0];

      if(match) {
        if(match[1] !== true)
          a[1] = match[1];
        
        // obli2 has family_name, family_group and virus_name in uppercase, should be lowercase
        if(a[1] === 'obli2') {
          // why like this?
          //   because family, phylum and sequence files may or may not have these columns in them
          //   ugly, but it works without having to recur to a csv parser in here
          if(a[2] === 'ORF2')
            a[2] = 'orf2';
          if(a[3] === 'ORF2')
            a[3] = 'orf2';
          if(a[4] === 'ORF2')
            a[4] = 'orf2';
        }

        process.stdout.write(a.join(',') + '\n');
      }
    }

    ++n;

    // This is needed since stdin >>> stdout
    if(n%(1024*1024*32) === 0) {
      process.stdin.pause();

      setTimeout(() => process.stdin.resume(), 800);
    }
  })
  .on('close', () => {
    console.log('n', n);

    console.log('DONE');
  });

process.stdout.on('error', e => {
  if(e.code === 'EPIPE')
    process.exit(0);
});
