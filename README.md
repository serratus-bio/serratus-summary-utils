# serratus-summary-utils

A collection of utilities used to parse the alignment summary files and create the SQL tables for the serratus data on Amazon Aurora.

# nvm

To run these script you need an up-to-date version of node.js,
We recommend to use nvm, see [how to install nvm](https://github.com/nvm-sh/nvm#installing-and-updating "how to install nvm").

Once installed, you should run :
`nvm install` and `nvm use`
on the root path and you're ready to run the scripts.

# src/dindex.csv.js

Builds dfamily, dphylum and dsequence CSV files for all the dark RdRp alignment summaries found on the public serratus S3 bucket.

For usage information, run `node src/dindex.csv.js --help`.

# src/rindex.csv.js

...
