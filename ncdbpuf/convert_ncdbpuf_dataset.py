import pandas as pd
import pathlib as pl
import argparse

SCHEMA_2022 = (
    ('PUF_CASE_ID', 37), ('PUF_FACILITY_ID', 10), ('FACILITY_TYPE_CD', 1),
    ('FACILITY_LOCATION_CD', 1), ('AGE', 3), ('SEX', 1), ('RACE', 2),
    ('SPANISH_HISPANIC_ORIGIN', 1), ('INSURANCE_STATUS', 2),
    ('MED_INC_QUAR_00', 1), ('NO_HSD_QUAR_00', 1), ('UR_CD_03', 1),
    ('MED_INC_QUAR_12', 1), ('NO_HSD_QUAR_12', 1), ('UR_CD_13', 1),
    ('CROWFLY', 8), ('CDCC_TOTAL_BEST', 2), ('SEQUENCE_NUMBER', 2),
    ('CLASS_OF_CASE', 2), ('YEAR_OF_DIAGNOSIS', 4), ('PRIMARY_SITE', 4),
    ('LATERALITY', 1), ('HISTOLOGY', 4), ('BEHAVIOR', 1), ('GRADE', 1),
    ('DIAGNOSTIC_CONFIRMATION', 1), ('TUMOR_SIZE', 3),
    ('REGIONAL_NODES_POSITIVE', 2), ('REGIONAL_NODES_EXAMINED', 2),
    ('DX_STAGING_PROC_DAYS', 8), ('RX_SUMM_DXSTG_PROC', 2),
    ('TNM_CLIN_T', 5), ('TNM_CLIN_N', 5), ('TNM_CLIN_M', 5),
    ('TNM_CLIN_STAGE_GROUP', 4), ('TNM_PATH_T', 5), ('TNM_PATH_N', 5),
    ('TNM_PATH_M', 5), ('TNM_PATH_STAGE_GROUP', 4),
    ('TNM_EDITION_NUMBER', 2), ('ANALYTIC_STAGE_GROUP', 1),
    ('CS_METS_AT_DX', 2), ('CS_METS_EVAL', 1), ('CS_EXTENSION', 3),
    ('CS_TUMOR_SIZEEXT_EVAL', 1), ('CS_METS_DX_BONE', 1),
    ('CS_METS_DX_BRAIN', 1), ('CS_METS_DX_LIVER', 1),
    ('CS_METS_DX_LUNG', 1), ('LYMPH_VASCULAR_INVASION', 1),
    ('CS_SITESPECIFIC_FACTOR_1', 3), ('CS_SITESPECIFIC_FACTOR_2', 3),
    ('CS_SITESPECIFIC_FACTOR_3', 3), ('CS_SITESPECIFIC_FACTOR_4', 3),
    ('CS_SITESPECIFIC_FACTOR_5', 3), ('CS_SITESPECIFIC_FACTOR_6', 3),
    ('CS_SITESPECIFIC_FACTOR_7', 3), ('CS_SITESPECIFIC_FACTOR_8', 3),
    ('CS_SITESPECIFIC_FACTOR_9', 3), ('CS_SITESPECIFIC_FACTOR_10', 3),
    ('CS_SITESPECIFIC_FACTOR_11', 3), ('CS_SITESPECIFIC_FACTOR_12', 3),
    ('CS_SITESPECIFIC_FACTOR_13', 3), ('CS_SITESPECIFIC_FACTOR_14', 3),
    ('CS_SITESPECIFIC_FACTOR_15', 3), ('CS_SITESPECIFIC_FACTOR_16', 3),
    ('CS_SITESPECIFIC_FACTOR_17', 3), ('CS_SITESPECIFIC_FACTOR_18', 3),
    ('CS_SITESPECIFIC_FACTOR_19', 3), ('CS_SITESPECIFIC_FACTOR_20', 3),
    ('CS_SITESPECIFIC_FACTOR_21', 3), ('CS_SITESPECIFIC_FACTOR_22', 3),
    ('CS_SITESPECIFIC_FACTOR_23', 3), ('CS_SITESPECIFIC_FACTOR_24', 3),
    ('CS_SITESPECIFIC_FACTOR_25', 3), ('CS_VERSION_LATEST', 6),
    ('DX_RX_STARTED_DAYS', 8), ('DX_SURG_STARTED_DAYS', 8),
    ('DX_DEFSURG_STARTED_DAYS', 8), ('RX_SUMM_SURG_PRIM_SITE', 2),
    ('RX_HOSP_SURG_APPR_2010', 1), ('RX_SUMM_SURGICAL_MARGINS', 1),
    ('RX_SUMM_SCOPE_REG_LN_SUR', 1), ('RX_SUMM_SURG_OTH_REGDIS', 1),
    ('SURG_DISCHARGE_DAYS', 8), ('READM_HOSP_30_DAYS', 1),
    ('REASON_FOR_NO_SURGERY', 1), ('DX_RAD_STARTED_DAYS', 8),
    ('RAD_LOCATION_OF_RX', 1), ('RX_SUMM_SURGRAD_SEQ', 1),
    ('RAD_ELAPSED_RX_DAYS', 3), ('REASON_FOR_NO_RADIATION', 1),
    ('DX_SYSTEMIC_STARTED_DAYS', 8), ('DX_CHEMO_STARTED_DAYS', 8),
    ('RX_SUMM_CHEMO', 2), ('DX_HORMONE_STARTED_DAYS', 8),
    ('RX_SUMM_HORMONE', 2), ('DX_IMMUNO_STARTED_DAYS', 8),
    ('RX_SUMM_IMMUNOTHERAPY', 2), ('RX_SUMM_TRNSPLNT_ENDO', 2),
    ('RX_SUMM_SYSTEMIC_SUR_SEQ', 1), ('DX_OTHER_STARTED_DAYS', 8),
    ('RX_SUMM_OTHER', 1), ('PALLIATIVE_CARE', 1),
    ('RX_SUMM_TREATMENT_STATUS', 1), ('PUF_30_DAY_MORT_CD', 1),
    ('PUF_90_DAY_MORT_CD', 1), ('DX_LASTCONTACT_DEATH_MONTHS', 8),
    ('PUF_VITAL_STATUS', 1), ('RX_HOSP_SURG_PRIM_SITE', 2),
    ('RX_HOSP_CHEMO', 2), ('RX_HOSP_IMMUNOTHERAPY', 2),
    ('RX_HOSP_HORMONE', 2), ('RX_HOSP_OTHER', 2), ('PUF_MULT_SOURCE', 1),
    ('PUF_REFERENCE_DATE_FLAG', 1), ('RX_SUMM_SCOPE_REG_LN_2012', 1),
    ('RX_HOSP_DXSTG_PROC', 2), ('PALLIATIVE_CARE_HOSP', 1),
    ('TUMOR_SIZE_SUMMARY_2016', 3), ('METS_AT_DX_OTHER', 1),
    ('METS_AT_DX_DISTANT_LN', 1), ('METS_AT_DX_BONE', 1),
    ('METS_AT_DX_BRAIN', 1), ('METS_AT_DX_LIVER', 1),
    ('METS_AT_DX_LUNG', 1), ('NO_HSD_QUAR_2016', 1),
    ('MED_INC_QUAR_2016', 1), ('PUF_MEDICAID_EXPN_CODE', 1),
    ('PHASE_I_RT_VOLUME', 2), ('PHASE_I_RT_TO_LN', 2),
    ('PHASE_I_DOSE_FRACT', 5), ('PHASE_I_NUM_FRACT', 3),
    ('PHASE_I_BEAM_TECH', 2), ('PHASE_I_TOTAL_DOSE', 6),
    ('PHASE_I_RT_MODALITY', 2), ('PHASE_II_RT_VOLUME', 2),
    ('PHASE_II_RT_TO_LN', 2), ('PHASE_II_DOSE_FRACT', 5),
    ('PHASE_II_NUM_FRACT', 3), ('PHASE_II_BEAM_TECH', 2),
    ('PHASE_II_TOTAL_DOSE', 6), ('PHASE_II_RT_MODALITY', 2),
    ('PHASE_III_RT_VOLUME', 2), ('PHASE_III_RT_TO_LN', 2),
    ('PHASE_III_DOSE_FRACT', 5), ('PHASE_III_NUM_FRACT', 3),
    ('PHASE_III_BEAM_TECH', 2), ('PHASE_III_TOTAL_DOSE', 6),
    ('PHASE_III_RT_MODALITY', 2), ('NUMBER_PHASES_RAD_RX', 2),
    ('RAD_RX_DISC_EARLY', 2), ('TOTAL_DOSE', 6), ('ADENOID_CYSTIC_BSLD', 5),
    ('ADENOPATHY', 1), ('AFP_POST_ORCH_RANGE', 1),
    ('AFP_POST_ORCH_VALUE', 7), ('AFP_PRE_INTERP', 1),
    ('AFP_PRE_ORCH_RANGE', 1), ('AFP_PRE_ORCH_VALUE', 7),
    ('AFP_PRE_VALUE', 6), ('AJCC_ID', 4), ('AJCC_TNM_CLIN_M', 15),
    ('AJCC_TNM_CLIN_N', 15), ('AJCC_TNM_CLIN_N_SFX', 4),
    ('AJCC_TNM_CLIN_STG_GRP', 15), ('AJCC_TNM_CLIN_T', 15),
    ('AJCC_TNM_CLIN_T_SFX', 4), ('AJCC_TNM_PATH_M', 15),
    ('AJCC_TNM_PATH_N', 15), ('AJCC_TNM_PATH_N_SFX', 4),
    ('AJCC_TNM_PATH_STG_GRP', 15), ('AJCC_TNM_PATH_T', 15),
    ('AJCC_TNM_PATH_T_SFX', 4), ('AJCC_TNM_POST_PATH_M', 15),
    ('AJCC_TNM_POST_PATH_N', 15), ('AJCC_TNM_POST_PATH_N_SFX', 4),
    ('AJCC_TNM_POST_PATH_STG_GRP', 15), ('AJCC_TNM_POST_PATH_T', 15),
    ('AJCC_TNM_POST_PATH_T_SFX', 4), ('ALBUMIN_PRE_TX_LEVL', 1),
    ('ANEMIA', 1), ('B_SYMPTOMS', 1), ('BASAL_DIAMETER', 4),
    ('BETA2MG_PRE_TX_LVL', 1), ('BEYOND_CAPSULE', 1),
    ('BILIRUBIN_PRE_UNIT', 1), ('BILIRUBIN_PRE_VALUE', 5),
    ('BONE_INVASION', 1), ('BRAIN_MOL_MARKERS', 2),
    ('BRESLOW_THICKNESS', 4), ('CA125_PRE_INTERP', 1),
    ('CEA_PRE_INTERP', 1), ('CEA_PRE_VALUE', 6),
    ('CHROMOSOME_19QLOH', 1), ('CHROMOSOME_1PLOH', 1),
    ('CHROMOSOME_3_STATUS', 1), ('CHROMOSOME_8Q_STAT', 1),
    ('CREATININE_PRE_UNIT', 1), ('CREATININE_PRE_VALU', 4),
    ('CRM', 4), ('ENE_CLIN_HN', 1), ('ENE_CLIN_NOT_HN', 1),
    ('ENE_PATH_HN', 3), ('ENE_PATH_NOT_HN', 1),
    ('ER_PERCENT_POS_OR_RNG', 3), ('ER_SUMMARY', 1),
    ('ER_TOTAL_ALLRED', 2), ('ESOPH_EPICENTER', 1),
    ('EXTRAVASC_MATRIX', 1), ('FIBROSIS_SCORE', 1),
    ('FIGO_STAGE', 2), ('GEST_PROGNOST_INDEX', 2),
    ('GLEASON_PAT_CLIN', 2), ('GLEASON_PAT_PATH', 2),
    ('GLEASON_SCORE_CLIN', 2), ('GLEASON_SCORE_PATH', 2),
    ('GLEASON_SCORE_TERTIARY_PT', 2), ('GRADE_CLIN', 1),
    ('GRADE_PATH', 1), ('GRADE_PATH_POST', 1),
    ('HCG_POST_ORCH_RANGE', 1), ('HCG_POST_ORCH_VALUE', 7),
    ('HCG_PRE_ORCH_RANGE', 1), ('HCG_PRE_ORCH_VALUE', 7),
    ('HER2_IHC_SUMMARY', 1), ('HER2_ISH_DUAL_NUM', 4),
    ('HER2_ISH_DUAL_RATIO', 4), ('HER2_ISH_SINGLE_NUM', 4),
    ('HER2_ISH_SUMMARY', 1), ('HER2_OVERALL_SUMM', 1),
    ('HERITABLE_TRAIT', 1), ('HIGH_RISK_CYTOGENET', 1),
    ('HIGH_RISK_HIST_FEAT', 1), ('HIV_STATUS', 1),
    ('IMMUNE_SUPP', 1), ('INR_PRO_TIME', 3),
    ('IPSI_ADRENAL_INVOL', 1), ('JAK2', 1), ('KI67', 5),
    ('KIT_GENE_IHC', 1), ('KRAS', 1), ('LDH_POST_ORCH_RANGE', 1),
    ('LDH_PRE_ORCH_RANGE', 1), ('LDH_PRE_TX_LEVEL', 1),
    ('LDH_PRE_TX_VALUE', 7), ('LDH_UPPER_NORMAL', 3),
    ('LN_DIST_MEDIAS_SCALN', 1), ('LN_DISTANT_METHOD', 1),
    ('LN_FEM_ING_PARA_APELV', 1), ('LN_ITC', 1),
    ('LN_LATERALITY', 1), ('LN_METHOD_FEMING', 1),
    ('LN_METHOD_PARA_AORT', 1), ('LN_METHOD_PELVIC', 1),
    ('LN_POS_AX_LEVELS_I_II', 2), ('LN_SIZE', 4),
    ('LNHN_LEVELS_I_III', 1), ('LNHN_LEVELS_IV_V', 1),
    ('LNHN_LEVELS_OTHER', 1), ('LNHN_LEVELS_VI_VII', 1),
    ('LYMPHOCYTOSIS', 1), ('MAJOR_VEIN_INVOLV', 1),
    ('METHYLATION_O6MGMT', 1), ('MICROVASC_DENSITY', 2),
    ('MITOTIC_COUNT_UVEA', 4), ('MITOTIC_RATE_MELANO', 2),
    ('MSI', 1), ('MULTIGENE_METHOD', 1), ('MULTIGENE_RESULTS', 2),
    ('NCCN_IPI', 2), ('NUM_CORES_EXAM', 2), ('NUM_CORES_POS', 2),
    ('NUM_NODES_EXAM_PARA_A', 2), ('NUM_NODES_POS_PARA_A', 2),
    ('NUM_PELV_NODES_EXAM', 2), ('NUM_PELV_NODES_POS', 2),
    ('ONCOTYPE_RISK_DCIS', 1), ('ONCOTYPE_RISK_INVAS', 1),
    ('ONCOTYPE_SCORE_DCIS', 3), ('ONCOTYPE_SCORE_INV', 3),
    ('ORGANOMEGALY', 1), ('P_SCLER_CHOLANGITIS', 1),
    ('PERCNT_NECROS_POST', 5), ('PERINEURAL_INV', 1),
    ('PERIPH_BLOOD_INV', 1), ('PERITONEAL_CYTOL', 1),
    ('PLEURAL_EFFUSION', 1), ('PLEURAL_INV', 1),
    ('PR_PERCENT_POS_OR_RNG', 3), ('PR_SUMMARY', 1),
    ('PR_TOTAL_ALLRED', 2), ('PROSTATE_PATH_EXT', 3), ('PSA', 5),
    ('RSPNS_TO_NEOADJUVT', 1), ('S_CAT_CLIN', 1), ('S_CAT_PATH', 1),
    ('SARCOMATOID', 3), ('SCHEMA_DISC_1', 1), ('SCHEMA_DISC_2', 1),
    ('SCHEMA_DISC_3', 1), ('SCHEMA_ID', 5), ('SEP_NODULES', 1),
    ('SLN_EXAM', 2), ('SLN_POS', 2), ('THICKNESS', 4),
    ('THROMBOCYTOPENIA', 1), ('TUMOR_DEPOSITS', 2),
    ('TUMOR_GROWTH_PAT', 1), ('ULCERATION', 1),
    ('SENTINEL_LNBX_STARTED_DAY', 8),
    ('REG_LN_DISS_STARTED_DAY', 8), ('RESID_POST_CYTOREDU', 2),
    ('NO_HSD_QUAR_2020', 1), ('MED_INC_QUAR_2020', 1),
    ('SARSCOV2_POS', 1), ('SARSCOV2_POS_DAYS', 8),
    ('SARSCOV2_TEST', 1), ('GRADE_POST_THERAPY_CLIN_YC', 1),
    ('AJCC_TNM_POST_CLIN_YC_M', 15), ('AJCC_TNM_POST_CLIN_YC_N', 15),
    ('AJCC_TNM_POST_CLIN_YC_N_SUF', 4),
    ('AJCC_TNM_POST_CLIN_YC_T', 15),
    ('AJCC_TNM_POST_CLIN_YC_T_SUF', 4),
    ('AJCC_TNM_POST_THER_CLIN_YC_GRP', 15),
    ('MACROSCOPIC_EVAL_TMESORECTUM', 2), ('DERIVED_RAI_STAGE', 1),
    ('P16', 1), ('LN_STATUS_PELVIC', 1), ('LN_STATUS_PARA_AORTIC', 1),
    ('LN_STATUS_FEMORAL_INGUINAL', 1), ('RX_HOSP_SURG_BREAST', 4),
    ('RX_SUMM_SURG_BREAST', 4), ('RX_HOSP_RECON_BREAST', 4),
    ('RX_SUMM_RECON_BREAST', 4), ('ALK_REARRANGEMENT', 1),
    ('EGFR_MUTATIONAL_ANALYSIS', 1), ('BRAF_MUTATIONAL_ANALYSIS', 1),
    ('NRAS_MUTATIONAL_ANALYSIS', 1), ('CA_19_9_PRETX_LAB_VALUE', 6)
)

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
