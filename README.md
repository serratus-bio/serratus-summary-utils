# serratus-summary-utils

A collection of utilities used to parse the alignment summary files and create the SQL tables for the serratus data on Amazon Aurora.

# nvm

To run these script you need an up-to-date version of [node.js](https://nodejs.org),
We recommend to use nvm, see [how to install nvm](https://github.com/nvm-sh/nvm#installing-and-updating "how to install nvm").

Once installed, you should run:

`nvm install`, `nvm use` and `npm install` on the root path and you're ready to run the scripts.

# src/dindex.csv.js

Builds dfamily, dphylum and dsequence CSV file tables for all the dark RdRp alignment summaries found in the public serratus S3 bucket.

For usage information, run `node src/dindex.csv.js --help`.

# src/rindex.csv.js

Builds rfamily, rphylum and rsequence CSV file tables for all the RdRp alignment summaries found in the public serratus S3 bucket.

For usage information, run `node src/rindex.csv.js --help`.
