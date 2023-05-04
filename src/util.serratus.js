export const S3 = {
  fetchDSummary:async id => {
    return new Promise(resolve => fetch('https://lovelywater2.s3.amazonaws.com/dsummary/' + id + '.psummary')
      .then(res => res.status === 200
        ? res.text().then(resolve)
        : resolve())
      .catch(() => resolve()));
  },
  fetchRSummary:async id => {
    return new Promise(resolve => fetch('https://lovelywater2.s3.amazonaws.com/rsummary/' + id + '.psummary')
      .then(res => res.status === 200
        ? res.text().then(resolve)
        : resolve())
      .catch(() => resolve()));
  }
};

export const famcvgToFamily = (famcvg, full_phylum_name) => {
  const fam = famcvg.fam.split(/\./);

  return {
    run_id:famcvg.sra,
    phylum_name:
      full_phylum_name
        ? {
          dupl:'Duplornaviricota',
          kiti:'Kitrinoviricota',
          levi:'Lenarviricota',
          nega:'Negarnaviricota',
          pisu:'Pisuviricota',
          rdrp:'Unclassified',
          var:'Deltavirus'
        }[fam[0]]
        : fam[0],
    family_name:fam[1].startsWith('Unc')
      ? fam[1].replace(/^Unc/, 'Unclassified-')
      : fam[1].split(/-/)[0],
    family_group:fam[1],
    coverage_bins:famcvg.famcvg,
    score:famcvg.score,
    percent_identity:famcvg.pctid,
    depth:famcvg.depth,
    n_reads:famcvg.alns,
    aligned_length:famcvg.avgcols
  };
};
export const phycvgToPhylum = (phycvg, full_phylum_name) => ({
  run_id:phycvg.sra,
  phylum_name:
    full_phylum_name
      ? {
        dupl:'Duplornaviricota',
        kiti:'Kitrinoviricota',
        levi:'Lenarviricota',
        nega:'Negarnaviricota',
        pisu:'Pisuviricota',
        rdrp:'Unclassified',
        var:'Deltavirus'
      }[phycvg.phy]
      : phycvg.phy,
  coverage_bins:phycvg.phycvg,
  score:phycvg.score,
  percent_identity:phycvg.pctid,
  depth:phycvg.depth,
  n_reads:phycvg.alns,
  aligned_length:phycvg.avgcols
});
export const summaryToObject = summary => summary.split(/\n/).filter(v => !!v).map(v => Object.fromEntries(v.split(/;/).filter(_v => !!_v).map(_v => _v.split(/=(.+)/, 2))));
export const vircvgToSequence = (vircvg, full_phylum_name) => {
  const vir = vircvg.vir.split(/\./);

  return {
    run_id:vircvg.sra,
    phylum_name:
      full_phylum_name
        ? {
          dupl:'Duplornaviricota',
          kiti:'Kitrinoviricota',
          levi:'Lenarviricota',
          nega:'Negarnaviricota',
          pisu:'Pisuviricota',
          rdrp:'Unclassified',
          var:'Deltavirus'
        }[vir[0]]
        : vir[0],
    family_name:vir[1].startsWith('Unc')
      ? vir[1].replace(/^Unc/, 'Unclassified-')
      : vir[1].split(/-/)[0],
    family_group:vir[1],
    virus_name:vir[2].split(/:/)[0],
    sequence_accession:vir[2].split(/:/)[1],
    coverage_bins:vircvg.vircvg,
    score:vircvg.score,
    percent_identity:vircvg.pctid,
    depth:vircvg.depth,
    n_reads:vircvg.alns,
    aligned_length:vircvg.avgcols
  };
};
