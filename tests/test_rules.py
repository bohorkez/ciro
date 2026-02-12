import os
import tempfile
import unittest
from pyhocon import ConfigFactory
from src.ciro.rules import read_rules_from_hocon, Rule

class TestReadRulesFromHocon(unittest.TestCase):
    def test_read_rules_from_hocon(self):
        # Sample HOCON content with camelCase keys
        hocon_content = '''
        hammurabi {
          input {
            type = "table"
            tables = [
              ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
            ]
          }
          dataFrameInfo {
            cutoffDate = ${CUTOFF_DATE}
            uuaa = "kmfi"
            targetPathName = ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
            physicalTargetName = "t_kmfi_prospective_metrics_ol"
            subset = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
          }
          temporaryObjects = [
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp2"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_net_fee_pptv_no_lc_amount"
                  "gf_ooff_fee_pptv_no_lc_amount"
                  "gf_pdic_fee_pptv_no_lc_amount"
                  "gf_svc_fee_pptv_no_lc_amount"
                  "gf_pac_pptv_no_lc_amount"
                  "gf_cst_nii_ne_pno_lc_amount"
                  "gf_fdg_nii_pptv_no_lc_amount"
                  "gf_nii_fee_pptv_no_lc_amount"
                  "gf_spread_pptv_no_per"
                  "gf_rof_pptv_no_lc_amount"
                  "gf_net_fee_pptv_mt_lc_amount"
                  "gf_ooff_fee_pptv_mt_lc_amount"
                  "gf_pdic_fee_pptv_mt_lc_amount"
                  "gf_svc_fee_pptv_mt_lc_amount"
                  "gf_pac_pptv_mt_lc_amount"
                  "gf_cst_nii_ne_pmt_lc_amount"
                  "gf_fdg_nii_pptv_mt_lc_amount"
                  "gf_nii_fee_pptv_mt_lc_amount"
                  "gf_spread_pptv_mt_per"
                  "gf_rof_pptv_mt_lc_amount"
                  "gf_rk_crr_pptv_mt_lc_amount"
                  "gf_el_ek_pptv_no_lc_amount"
                  "gf_el_ek_pptv_mt_lc_amount"
                  "gf_cst_nii_ne_pstk_lc_amount"
                  "gf_cst_nii_xe_pstk_lc_amount"
                  "gf_cst_nii_xe_pmt_lc_amount"
                  "gf_rk_crr_pptv_no_lc_amount"
                  "gf_cst_nii_xe_pno_lc_amount"
                  "gf_pac_pptv_stk_lc_amount"
                  "gf_gs_cr_pptv_no_lc_amount"
                  "gf_ctg_liab_pptv_no_lc_amount"
                  "gf_obe_pptv_no_lc_amount"
                  "gf_cust_liab_pptv_no_lc_amount"
                ]
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_no_event_info_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "g_detail_event_id"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp3"
                keyColumns = [
                  {
                    columnInput = "gf_initial_catalog_val_id"
                    columnOrigin = "g_detail_event_id_"
                  }
                ]
                leftValues {
                  type = "parquet"
                  paths = [
                    "tmp://tmp2"
                  ]
                }
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_ktny_rel_values_taxonomy"
                  ]
                }
                rightCondition = "gf_frequency_type='M' AND gf_cutoff_date='"${CUTOFF_DATE}"'"
                rightColumns = [
                  "gf_initial_catalog_val_id"
                  "gf_final_catalog_val_id"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp4"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' andgf_cutoff_date='"${CUTOFF_DATE_PREVIOUS_MONTH}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_pac_pptv_stk_lc_amount"
                ]
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_no_event_info_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE_PREVIOUS_MONTH}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "g_detail_event_id"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp5"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_net_fee_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_pac_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_cst_nii_xe_pstk_lc_amount"
                  "gf_cst_nii_ne_pstk_lc_amount"
                ]
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}" and gf_cutoff_date='"${CUTOFF_DATE_PREVIOUS_MONTH}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "gf_net_fee_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_pac_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_cst_nii_xe_pstk_lc_amount"
                  "gf_cst_nii_ne_pstk_lc_amount"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp6"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "parquet"
                  paths = [
                    "tmp://tmp5"
                  ]
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_net_fee_pptv_stk_lc_amount"
                  "gf_net_fee_pptv_stk_lc_amount_"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount_"
                  "gf_pac_pptv_stk_lc_amount"
                  "gf_pac_pptv_stk_lc_amount_"
                  "gf_fdg_nii_pptv_stk_lc_amount"
                  "gf_fdg_nii_pptv_stk_lc_amount_"
                  "gf_cst_nii_xe_pstk_lc_amount"
                  "gf_cst_nii_xe_pstk_lc_amount_"
                  "gf_cst_nii_ne_pstk_lc_amount"
                  "gf_cst_nii_ne_pstk_lc_amount_"
                ]
                leftCondition = "1=1"
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_no_event_info_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "g_detail_event_id"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp7"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kctk_credit_risk_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' gf_cutoff_date='"${CUTOFF_DATE}"'"
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_gl_el_ptl_oc_amount"
                ]
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kctk_credit_risk_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE_PREVIOUS_MONTH}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "gf_gl_el_ptl_oc_amount"
                ]
              }
            }
            {
              class = "com.datio.hammurabi.temporary.TemporaryObject"
              config {
                id = "tmp8"
                keyColumns = [
                  {
                    columnInput = "g_operation_number_id"
                    columnOrigin = "g_operation_number_id"
                  }
                ]
                leftValues {
                  type = "parquet"
                  paths = [
                    "tmp://tmp7"
                  ]
                }
                leftColumns = [
                  "g_operation_number_id"
                  "gf_gl_el_ptl_oc_amount"
                  "gf_gl_el_ptl_oc_amount_"
                ]
                leftCondition = "1=1"
                rightValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_no_event_info_ol"
                  ]
                  options.where = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"'"
                }
                rightColumns = [
                  "g_operation_number_id"
                  "g_detail_event_id"
                ]
              }
            }
          ]
          rules = [
            {
              class = "com.datio.hammurabi.rules.consistence.ValueConciliationRule"
              config {
                column = "g_operation_number_id"
                keyColumns = [
                  {
                    columnOrigin = "g_operation_number_id"
                    columnInput = "g_operation_number_id"
                  }
                ]
                dataValuesSubset = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"'"
                dataValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kaoc_eom_operation"
                  ]
                }
                dataSystemId = "DATIO-ContractOperationsAttributes"
                acceptanceMin = 100
                isCritical = true
                withRefusals = true
                id = "GL_STR_t_kmfi_prospective_metrics_ol_4.3_000"
                internalId = "GL_STR_t_kmfi_prospective_metrics_ol_4.3_000"
              }
            }
            {
              class = "com.datio.hammurabi.rules.TemporalRule"
              config {
                parentClass = "com.datio.hammurabi.rules.integrity.ValueComparisonRule"
                temporalPath = "tmp://tmp2"
                keyColumns = [
                  {
                    columnOrigin = "g_operation_number_id"
                    columnInput = "g_operation_number_id"
                  }
                ]
                column = "gf_pac_pptv_no_lc_amount"
                condition = "gf_pac_pptv_no_lc_amount"
                dataValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                }
                dataValuesSubset = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                dataSystemId = "DATIO-finantialmetrics"
                comparison = "="
                dataValuesCondition = "gf_pac_pptv_stk_lc_amount"
                acceptanceMin = 100
                subset = "g_detail_event_id_ == 'AL'"
                isCritical = false
                withRefusals = true
                id = "GL_DOM_t_kmfi_prospective_metrics_ol_5.2_031"
                internalId = "GL_DOM_t_kmfi_prospective_metrics_ol_5.2_031"
              }
            }
            {
              class = "com.datio.hammurabi.rules.TemporalRule"
              config {
                parentClass = "com.datio.hammurabi.rules.integrity.ValueComparisonRule"
                temporalPath = "tmp://tmp2"
                keyColumns = [
                  {
                    columnOrigin = "g_operation_number_id"
                    columnInput = "g_operation_number_id"
                  }
                ]
                column = "gf_pac_pptv_no_lc_amount"
                condition = "gf_pac_pptv_no_lc_amount"
                dataValues {
                  type = "table"
                  tables = [
                    ${MASTERSCHEMA}".t_kmfi_prospective_metrics_ol"
                  ]
                }
                dataValuesSubset = "g_entific_id='"${ENTIFIC_ID}"' and gf_cutoff_date='"${CUTOFF_DATE}"' and gf_information_origin_id='"${INFORMATION_ORIGIN_ID}"'"
                dataSystemId = "DATIO-finantialmetrics"
                comparison = "="
                dataValuesCondition = "gf_pac_pptv_stk_lc_amount"
                acceptanceMin = 100
                subset = "g_detail_event_id_ == 'CC'"
                isCritical = false
                withRefusals = true
                id = "GL_DOM_t_kmfi_prospective_metrics_ol_5.2_032"
                internalId = "GL_DOM_t_kmfi_prospective_metrics_ol_5.2_032"
              }
            }
          ]
        }
        '''
        # Write to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.conf') as tmp:
            tmp.write(hocon_content)
            tmp_path = tmp.name
        try:
            rules = read_rules_from_hocon(tmp_path)
            self.assertEqual(len(rules), 3)
            rule = rules[0]
            self.assertIsInstance(rule, Rule)
            self.assertEqual(rule.column, "g_operation_number_id")
            self.assertEqual(rule.acceptance_min, 100)
            self.assertTrue(rule.is_critical)
            self.assertTrue(rule.with_refusals)
            self.assertEqual(rule.id, "GL_STR_t_kmfi_prospective_metrics_ol_4.3_000")
            self.assertEqual(rule.internal_id, "GL_STR_t_kmfi_prospective_metrics_ol_4.3_000")
        finally:
            os.remove(tmp_path)
