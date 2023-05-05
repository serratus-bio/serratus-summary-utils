import { stringify } from 'csv-stringify';
import { createWriteStream } from 'fs';
import { createGzip } from 'zlib';

export const AsyncPool = function(args) { this.args = args||{}; };
Object.assign(AsyncPool.prototype, {
  flush:async function(flush) {
    if(!this.flush.promise && (this.push._||[]).length >= (this.args.n||8) || flush)
      this.flush.promise = new Promise(async resolve => {
        await Promise.all(this.push._.splice(0, (this.args.n||8)).map(v => v()));

        delete this.flush.promise;

        await this.flush();

        resolve();
      });
    
    return this.flush.promise;
  },
  push:function(f) {
    (this.push._ = this.push._||[]).push(f);

    this.flush();
  }
});

export const CSVGzipStream = function(args) {
  this.args = args||{};

  this.csv = stringify({ header:true })
    .on('readable', () => {
      let row = undefined;

      while(row = this.csv.read())
        this.gzip.write(row);
    })
    .on('finish', () => {
      this.gzip.end();
    });
    this.gzip = createGzip();
    this.writeStream = createWriteStream(this.args.path);

    this.gzip.pipe(this.writeStream);
};

Object.assign(CSVGzipStream.prototype, {
  end:function() { this.csv.end.apply(this.csv, arguments); },
  write:function() { this.csv.write.apply(this.csv, arguments); }
});
