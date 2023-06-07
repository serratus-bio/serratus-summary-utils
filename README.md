# serratus-summary-utils

A collection of utilities used to parse the alignment summary files and create the SQL tables for the serratus data on Amazon Aurora.

# nvm

To run these scripts you need an up-to-date version of [node.js](https://nodejs.org),
We recommend using nvm, see [how to install nvm](https://github.com/nvm-sh/nvm#installing-and-updating "how to install nvm").

Once nvm is installed, you should run:

`nvm install`, `nvm use` and `npm install` on the root path and you're ready to run the scripts.

# src/index.to.csv.js

Builds family, phylumm, sequence and sra CSV file tables for all the records found in the specified index file inside the public serratus S3 bucket.

For usage information, run `node src/index.to.csv.js --help`.
