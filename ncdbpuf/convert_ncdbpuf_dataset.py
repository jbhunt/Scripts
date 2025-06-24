import pandas as pd
import pathlib as pl
import argparse

SCHEMA_2022 = [
    ('PUF_CASE_ID', 37),
    ('PUF_FACILITY_ID', 10),
    ('FACILITY_TYPE_CD', 1),
    ('FACILITY_LOCATION_CD', 1),
    ('AGE', 3),
    ('SEX', 1),
    ('RACE', 2),
    ('SPANISH_HISPANIC_ORIGIN', 1),
    ('INSURANCE_STATUS', 2),
    ('MED_INC_QUAR_00', 1),
    ('NO_HSD_QUAR_00', 1),
    ('UR_CD_03', 1),
    ('MED_INC_QUAR_12', 1),
    ('NO_HSD_QUAR_12', 1),
    ('UR_CD_13', 1),
    ('CROWFLY', 8),
    ('CDCC_TOTAL_BEST', 2),
    ('SEQUENCE_NUMBER', 2),
    ('CLASS_OF_CASE', 2),
    ('YEAR_OF_DIAGNOSIS', 4),
    ('PRIMARY_SITE', 4),
    ('LATERALITY', 1),
    ('GRADE', 1),
    ('REGIONAL_NODES_EXAMINED', 2),
    ('CS_TUMOR_SIZE', 3),
    ('CS_EXT', 4),
    ('CS_METS_AT_DX', 2),
    ('CS_METS_EVAL', 1),
    ('CS_SURVIVAL_TIME_MONTHS', 3),
    ('CS_CSS_DAYS', 4),
    ('PRIM_SITE_LATERALITY', 4),
    ('CLASS_OF_CASE_INTERNAL_RECODE', 2),
    ('LVI', 1),
    ('NUMBER_SURGICAL_PROCS', 1),
    ('REGIONAL_NODES_POSITIVE', 2),
    ('RX_SUMM_TREATMENT_STATUS', 1),
    ('RX_SUMM_SURG_PRIM_SITE', 2),
    ('SCOPE_REG_LN_SUR_PROC', 2),
    ('SURG_PRIMARY_SITE', 2),
    ('RX_SUMM_SURGICAL_MARGINS', 1),
    ('RX_SUMM_SCOPE_REG_LN_SUR', 2),
    ('RX_HOSP_SURG_APPR_2020', 3),
    ('RX_SUMM_SURG_APPR_2020', 3),
    ('RX_HOSP_CHEMO', 1),
    ('CHEMOTHERAPY_RECODE', 1),
    ('RX_HOSP_IMMUNO', 1),
    ('IMMUNOTHERAPY_RECODE', 1),
    ('RX_HOSP_HORMONE', 1),
    ('HORMONE_THERAPY_RECODE', 1),
    ('RX_HOSP_OTHER', 1),
    ('RX_HOSP_RECON_BREAST_PROC', 1),
    ('RX_SUMM_CHEMO', 1),
    ('RX_SUMM_IMMUNOTHERAPY', 1),
    ('RX_SUMM_HORMONE', 1),
    ('RX_SUMM_OTHER', 1),
    ('RX_SUMM_TRANSPLNT_ENDOC', 1),
    ('RX_SUMM_SYSTEMIC_SUR_SEQ', 1),
    ('RX_SUMM_THERAPY', 1),
    ('CAREGIVER_INFO', 1),
    ('PHASE_I_IV', 1),
    ('PHASE_II_III', 1),
    ('CLINICAL_TRIAL_END', 1),
    ('TUMOR_SIZE_SUMM', 3),
    ('REGIONAL_NODES_EXAMINED_SUMM', 3),
    ('METS_AT_DX_BRAIN', 1),
    ('METS_AT_DX_BONE', 1),
    ('METS_AT_DX_LIVER', 1),
    ('METS_AT_DX_LUNG', 1),
    ('GRADE_CLIN', 1),
    ('GRADE_PATH', 1),
    ('GRADE_POST_THERAPY', 1),
    ('GRADE_CLIN_PRETREAT', 1),
    ('GRADE_PATH_PATTERNS', 1),
    ('GRADE_PATH_RESIDUAL', 1),
    ('HJURP', 1),
    ('HRAS', 1),
    ('KRAS', 1),
    ('PIK3CA', 1),
    ('TP53', 1),
    ('TMB', 2),
    ('MSI', 1),
    ('CIMP', 1),
    ('TIL', 1),
    ('PD_L1', 1),
    ('MOLECULAR_STATUS', 2),
    ('EBV', 1),
    ('HPV', 1),
    ('HER2', 1),
    ('ER', 1),
    ('PR', 1),
    ('AR', 1),
    ('BRCA1', 1),
    ('BRCA2', 1),
    ('CHEK2', 1),
    ('PALB2', 1),
    ('HBOC', 1),
    ('LYNCH', 1),
    ('FAP', 1),
    ('MEN1', 1),
    ('RET', 1),
    ('SDH', 1),
    ('VHL', 1),
    ('NF1', 1),
    ('PTEN', 1),
    ('TSC1', 1),
    ('TSC2', 1),
    ('NIPBL', 1),
    ('SMAD4', 1),
    ('BMPR1A', 1),
    ('ENG', 1),
    ('DICER1', 1),
    ('KIT', 1),
    ('CCND1', 1),
    ('NTRK', 1),
    ('TSH', 1),
    ('THYROGLOBULIN', 1),
    ('CALCITONIN', 1),
    ('ACTH', 1),
    ('GH', 1),
    ('IGF1', 1),
    ('GONADOTROPIN', 1),
    ('INSULIN', 1),
    ('RENIN', 1),
    ('ALDOSTERONE', 1),
    ('CATECHOLAMINE', 1),
    ('CORTISOL', 1),
    ('CA_19_9', 1),
    ('CA125', 1),
    ('CA15_3', 1),
    ('CFDNA', 1),
    ('CEA', 1),
    ('AFP', 1),
    ('BETA_HCG', 1),
    ('LDH', 1),
    ('PSA', 1),
    ('SYNAPTOPHYSIN', 1),
    ('CHROMOGRANIN', 1),
    ('NSE', 1),
    ('CALRETININ', 1),
    ('S100', 1),
    ('GATA3', 1),
    ('SOX10', 1),
    ('MELAN_A', 1),
    ('HMB45', 1),
    ('MITF', 1),
    ('P53', 1),
    ('KI67', 1),
    ('ALK_REARRANGEMENT', 1),
    ('EGFR_MUTATIONAL_ANALYSIS', 1),
    ('BRAF_MUTATIONAL_ANALYSIS', 1),
    ('NRAS_MUTATIONAL_ANALYSIS', 1),
    ('CA_19_9_PRETX_LAB_VALUE', 6)
]


def read_ncdb_puf_fixed(
    puf_txt_path: str | pl.Path,
    schema: list[tuple[str, int]] = SCHEMA_2022,
    max_rows: int | None = None
    ) -> pd.DataFrame:
    """
    Load an NCDB Participant-User-File (PUF) into a pandas DataFrame us
    ing a hard-coded fixed-width schema.

    Parameters
    ----------
    puf_txt_path : str | Path
    Path to the raw *.txt* file.
    schema : list[(name, width)]
        Sequence of (column-name, width-in-bytes) pairs.  Defaults to the
        2022 layout included above.
    max_rows : int, optional
        Handy for test loads; None ⇒ read the entire file.

    Returns
    -------
    pandas.DataFrame
        Every field is read as *str* so that leading zeros are preserved.
    """

    names, widths = zip(*schema)
    colspecs = []
    start = 0
    for w in widths:
        colspecs.append((start, start + w))
        start += w

    df = pd.read_fwf(
        puf_txt_path,
        colspecs   = colspecs,
        names      = names,
        dtype      = str,      # keep all values exactly as stored
        header     = None,
        encoding   = "latin-1",  # NCDB distributes in Windows-1252; latin-1 is safest
        nrows      = max_rows
    )
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dat', help='NCDB PUF dataset to convert to csv', type=str)
    args =parser.parse_args()
    src = pl.Path(args.dat)
    df = read_ncdb_puf_fixed(src)
    dst = src.parent.joinpath(f'{src.stem}.csv')
    df.to_csv(dst)
