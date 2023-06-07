export const S3 = {
  fetchSummary:async path => new Promise(resolve => fetch('https://lovelywater2.s3.amazonaws.com/' + path)
    .then(res => res.status === 200
      ? res.text().then(resolve)
      : resolve())
    .catch(() => resolve()))
};

export const famcvgToFamily = (famcvg, args) => {
  const fam = famcvg.fam.split(/\./);

  return {
    run_id:famcvg.sra,
    phylum_name:
      args.phylumNameDictionary
        ? args.phylumNameDictionary[fam[0]]
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

export const phycvgToPhylum = (phycvg, args) => ({
  run_id:phycvg.sra,
  phylum_name:
    args.phylumNameDictionary
      ? args.phylumNameDictionary[phycvg.phy]
      : phycvg.phy,
  coverage_bins:phycvg.phycvg,
  score:phycvg.score,
  percent_identity:phycvg.pctid,
  depth:phycvg.depth,
  n_reads:phycvg.alns,
  aligned_length:phycvg.avgcols
});

export const summaryToObject = summary => summary.split(/\n/).filter(v => !!v).map(v => Object.fromEntries(v.split(/;/).filter(_v => !!_v).map(_v => _v.split(/=(.+)/, 2))));

export const sumzerCommentToSRA = sumzerComment => {
  const SUMZER_COMMENT = summaryToObject(sumzerComment.SUMZER_COMMENT.replace(/,/g, ';'));

  return {
    run_id:sumzerComment.sra,
    read_length:sumzerComment.readlength,
    genome:SUMZER_COMMENT[0].genome,
    aligned_reads:sumzerComment.totalalns,
    date:SUMZER_COMMENT[0].date,
    truncated:sumzerComment.truncated
  };
};

export const vircvgToSequence = (vircvg, args) => {
  const vir = vircvg.vir.split(/\./);

  return {
    run_id:vircvg.sra,
    phylum_name:
      args.phylumNameDictionary
        ? args.phylumNameDictionary[vir[0]]
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
