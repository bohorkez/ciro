columns_integer_default = {}
columns_date_default = {}
columns_string_default = {}
columns_decimal_default = {}

# Procesamiento de las reglas
rules = [
    {
        "column": "g_operation_number_id"
    },
    {
        "column": "gf_pac_pptv_no_lc_amount"
    },
    {
        "column": "gf_pac_pptv_stk_lc_amount"
    },
    {
        "column": "gf_pac_pptv_mt_lc_amount"
    },
    {
        "column": "gf_el_ek_pptv_mt_lc_amount"
    },
    {
        "column": "gf_fdg_nii_pptv_no_lc_amount"
    },
    {
        "column": "gf_net_fee_pptv_no_lc_amount"
    }
]

for rule in rules:
    column = rule["column"]
    if column.endswith("amount") or column.endswith("number"):
        columns_decimal_default[column] = 0.0
    elif column.endswith("id") or column.endswith("type") or column.endswith("desc"):
        columns_string_default[column] = ""
    elif column.endswith("date"):
        columns_date_default[column] = "2000-01-01"
    else:
        columns_integer_default[column] = 0

print("columns_integer_default:", columns_integer_default)
print("columns_date_default:", columns_date_default)
print("columns_string_default:", columns_string_default)
print("columns_decimal_default:", columns_decimal_default)