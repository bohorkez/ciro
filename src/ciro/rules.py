from pyhocon import ConfigFactory
import re


def read_rules_from_hocon(hocon_path):
    regex_acceptanceMin = r"^\s*acceptanceMin\s*=\s*\$\{[^\}]+\}.*\n"
    with open(hocon_path, 'r') as f:
        hocon_file_content_raw = f.read()
        hocon_file_content = re.sub(regex_acceptanceMin, '', hocon_file_content_raw, flags=re.M)

    config = ConfigFactory.parse_string(hocon_file_content, resolve=False, unresolved_value="")
    rules_list = []
    rules_data = config.get('hammurabi.rules', [])
    for rule_data in rules_data:
        rule = Rule(
            parent_class=rule_data.get('config.parentClass', ''),
            rule_class=rule_data.get('class', ''),
            temporal_path=rule_data.get('config.temporalPath', ''),
            column=rule_data.get('config.column', ''),
            columns=rule_data.get('config.columns', ''),
            format=rule_data.get('config.format', ''),
            subset=rule_data.get('config.subset', ''),
            key_columns=rule_data.get('config.keyColumns', []),
            condition=rule_data.get('config.condition', ''),
            acceptance_min=int(rule_data.get('config.acceptanceMin', 0)),
            is_critical=bool(rule_data.get('config.isCritical', False)),
            with_refusals=bool(rule_data.get('config.withRefusals', False)),
            id=rule_data.get('config.id', ''),
            internal_id=rule_data.get('config.internalId', ''),
            data_values_condition=rule_data.get('config.dataValuesCondition', ''),
            comparison=rule_data.get('config.comparison', ''),
            data_system_id=rule_data.get('config.dataSystemId', ''),
            data_values_subset=rule_data.get('config.dataValuesSubset', ''),
            data_values_type=rule_data.get('config.dataValues.type', ''),
            data_values_tables=rule_data.get('config.dataValues.tables', ''),
            data_values_options_where=rule_data.get('config.dataValues.options.where', ''),
            values=rule_data.get('config.values', ''),
            min_threshold=rule_data.get('config.minThreshold', ''),
            target_threshold=rule_data.get('config.targetThreshold', ''),
            data_values_paths=rule_data.get('config.dataValues.paths', ''),
            drill_down=rule_data.get('config.drillDown', ''),
            acceptance_min_balance=int(rule_data.get('config.acceptanceMinBalance', 0)),
            balance_ids=rule_data.get('config.balanceIds', ''),
            lower_bound=rule_data.get('config.lowerBound', ''),
            upper_bound=rule_data.get('config.upperBound', ''),
            delimiter=rule_data.get('config.delimiter', ''),
            min_value=rule_data.get('config.minValue', ''),
            max_value=rule_data.get('config.maxValue', ''),
            aggregation=rule_data.get('config.aggregation', ''),
            variation_allowed=bool(rule_data.get('config.variationAllowed', False)),
            data_values_column=rule_data.get('config.dataValuesColumn', ''),
            over_column=rule_data.get('config.overColumn', ''),
            conf_name=hocon_path
        )

        if rule.data_values_type == 'parquet':
            rule.convert_hdfs_format_to_ada_format()

        if rule.data_values_type == 'table':
            rule.tweak_ada_format()

        rules_list.append(rule)
    return rules_list

def normalize_sql_query(query):
    clauses = query.split(' and ')
    sorted_clauses = sorted(clauses)
    normalized_query = ' and '.join(sorted_clauses).replace(' ', '')
    return normalized_query

class Rule:
    def __init__(self, parent_class: str, rule_class: str, temporal_path: str, column: str, columns: str, subset: str, key_columns: list, condition: str, acceptance_min: int, is_critical: bool, with_refusals: bool, id: str, internal_id: str, data_values_condition: str, comparison: str, data_system_id: str, format: str, data_values_subset: str, data_values_type: str, data_values_tables: str, data_values_options_where: str, values: str, min_threshold: str, target_threshold: str, data_values_paths: str, drill_down: str, acceptance_min_balance: str, balance_ids: str, lower_bound: str, upper_bound: str, delimiter: str, min_value: str, max_value: str, aggregation: str, variation_allowed: str, data_values_column: str, over_column: str, conf_name: str = '',detail: str = ''):
        self.parent_class = parent_class
        self.rule_class = rule_class
        self.temporal_path = temporal_path
        self.column = column
        self.columns = columns
        self.subset = subset
        self.key_columns = key_columns
        self.condition = condition
        self.acceptance_min = acceptance_min
        self.is_critical = is_critical
        self.with_refusals = with_refusals
        self.id = id
        self.internal_id = internal_id
        self.data_values_condition = data_values_condition
        self.comparison = comparison
        self.data_system_id = data_system_id
        self.format = format
        self.data_values_subset = data_values_subset
        self.data_values_type = data_values_type
        self.data_values_tables = data_values_tables
        self.data_values_options_where = data_values_options_where
        self.values = values
        self.min_threshold = min_threshold
        self.target_threshold = target_threshold
        self.data_values_paths = data_values_paths
        self.drill_down = drill_down
        self.acceptance_min_balance = acceptance_min_balance
        self.balance_ids = balance_ids
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.delimiter = delimiter
        self.min_value = min_value
        self.max_value = max_value
        self.aggregation = aggregation
        self.variation_allowed = variation_allowed
        self.data_values_column = data_values_column
        self.over_column = over_column
        self.conf_name = conf_name
        self.detail = detail

    def __eq__(self, other):
        if not isinstance(other, Rule):
            return False
        return (
            self.id == other.id
        )

    def to_dict(self):
        return {
            "parent_class": self.parent_class,
            "rule_class": self.rule_class,
            "temporal_path": self.temporal_path,
            "column": self.column,
            "columns": self.columns,
            "subset": self.subset,
            "key_columns": self.key_columns,
            "condition": self.condition,
            "acceptance_min": self.acceptance_min,
            "is_critical": self.is_critical,
            "with_refusals": self.with_refusals,
            "id": self.id,
            "internal_id": self.internal_id,
            "data_values_condition": self.data_values_condition,
            "comparison": self.comparison,
            "data_system_id": self.data_system_id,
            "format": self.format,
            "data_values_subset": self.data_values_subset,
            "data_values_type": self.data_values_type,
            "data_values_tables": self.data_values_tables,
            "data_values_options_where": self.data_values_options_where,
            "values": self.values,
            "min_threshold": self.min_threshold,
            "target_threshold": self.target_threshold,
            "data_values_paths": self.data_values_paths,
            "drill_down": self.drill_down,
            "acceptance_min_balance": self.acceptance_min_balance,
            "balance_ids": self.balance_ids,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "delimiter": self.delimiter,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "aggregation": self.aggregation,
            "variation_allowed": self.variation_allowed,
            "data_values_column": self.data_values_column,
            "over_column": self.over_column,
            "conf_name": self.conf_name,
            "detail": self.detail
        }
    
    def convert_hdfs_format_to_ada_format(self):
        if self.data_values_paths != '':
            str_data_values_path = self.data_values_paths[0]
            #Extrae el nombre de la tabla a partir de la ruta HDFS /data/master/...
            table_pattern = r"/data/master/[^/]+/data/([^/]+)"
            match = re.search(table_pattern, str_data_values_path)
            self.data_values_tables = ['.' + match.group(1)] if match else []

            #Extrae las particiones de la ruta HDFS /data/master/...
            #Ejemplo: /data/master/PE/data/gf_cutoff_date=/gf_information_origin_id=SKCO entonces matches = g_entific_id=PE/gf_cutoff_date=/gf_information_origin_id=SKCO
            partition_pattern = r"([^/]+=[^'\s]*)"
            matches = re.findall(partition_pattern, str_data_values_path)
            partitions = (matches[0]).split('/') if matches else []
            # Formatea cada partición como "clave = 'valor'" agregando comillas simples a los valores
            for i, partition in enumerate(partitions):
                if partition.endswith("="):
                    partitions[i] = partition + "''"
                else:
                    # Separar en clave y valor por el primer '='
                    key, value = partition.split('=', 1)
                    partitions[i] = f"{key} = '{value}'"

            if partitions:
                self.data_values_options_where = ' and '.join(partitions)
            else:
                self.data_values_options_where = ''
                
            self.data_values_paths = ''
            self.data_values_subset = ''
        return self
    
    def tweak_ada_format(self):
        if self.data_values_options_where == '':
            self.data_values_options_where = self.data_values_subset
        else:
            self.data_values_options_where += ' and ' + self.data_values_subset

        self.data_values_subset = ''
        return self

    def diff(self, other):
        if not isinstance(other, Rule):
            return {"error": "Cannot compare objects of different types"}
        
        differences = {}
        if self.parent_class != other.parent_class:
            differences["parent_class"] = {"old": self.parent_class, "new": other.parent_class}
        if self.rule_class != other.rule_class:
            differences["rule_class"] = {"old": self.rule_class, "new": other.rule_class}
        if self.temporal_path != other.temporal_path:
            differences["temporal_path"] = {"old": self.temporal_path, "new": other.temporal_path}
        if self.subset != other.subset:
            differences["subset"] = {"old": self.subset, "new": other.subset}
        if self.key_columns != other.key_columns:
            differences["key_columns"] = {"old": self.key_columns, "new": other.key_columns}
        if self.condition != other.condition:
            differences["condition"] = {"old": self.condition, "new": other.condition}
        if self.acceptance_min != other.acceptance_min:
            differences["acceptance_min"] = {"old": self.acceptance_min, "new": other.acceptance_min}
        if self.is_critical != other.is_critical:
            differences["is_critical"] = {"old": self.is_critical, "new": other.is_critical}
        if self.with_refusals != other.with_refusals:
            differences["with_refusals"] = {"old": self.with_refusals, "new": other.with_refusals}
        if self.internal_id != other.internal_id:
            differences["internal_id"] = {"old": self.internal_id, "new": other.internal_id}
        if self.data_values_condition != other.data_values_condition:
            differences["data_values_condition"] = {"old": self.data_values_condition, "new": other.data_values_condition}
        if self.comparison != other.comparison:
            differences["comparison"] = {"old": self.comparison, "new": other.comparison}
        if self.data_system_id != other.data_system_id:
            differences["data_system_id"] = {"old": self.data_system_id, "new": other.data_system_id}
        if self.format != other.format:
            differences["format"] = {"old": self.format, "new": other.format}
        if self.data_values_subset != other.data_values_subset:
            differences["data_values_subset"] = {"old": self.data_values_subset, "new": other.data_values_subset}
        if self.data_values_tables != other.data_values_tables:
            differences["data_values_tables"] = {"old": self.data_values_tables, "new": other.data_values_tables}
        if normalize_sql_query(self.data_values_options_where) != normalize_sql_query(other.data_values_options_where):
            differences["data_values_options_where"] = {"old": self.data_values_options_where, "new": other.data_values_options_where}
        if self.values != other.values:
            differences["values"] = {"old": self.values, "new": other.values}
        if self.min_threshold != other.min_threshold:
            differences["min_threshold"] = {"old": self.min_threshold, "new": other.min_threshold}
        if self.target_threshold != other.target_threshold:
            differences["target_threshold"] = {"old": self.target_threshold, "new": other.target_threshold}
        if self.data_values_paths != other.data_values_paths:
            differences["data_values_paths"] = {"old": self.data_values_paths, "new": other.data_values_paths}
        if self.drill_down != other.drill_down:
            differences["drill_down"] = {"old": self.drill_down, "new": other.drill_down}
        if self.acceptance_min_balance != other.acceptance_min_balance:
            differences["acceptance_min_balance"] = {"old": self.acceptance_min_balance, "new": other.acceptance_min_balance}
        if self.balance_ids != other.balance_ids:
            differences["balance_ids"] = {"old": self.balance_ids, "new": other.balance_ids}
        if self.lower_bound != other.lower_bound:
            differences["lower_bound"] = {"old": self.lower_bound, "new": other.lower_bound}
        if self.upper_bound != other.upper_bound:
            differences["upper_bound"] = {"old": self.upper_bound, "new": other.upper_bound}
        if self.delimiter != other.delimiter:
            differences["delimiter"] = {"old": self.delimiter, "new": other.delimiter}
        if self.min_value != other.min_value:
            differences["min_value"] = {"old": self.min_value, "new": other.min_value}
        if self.max_value != other.max_value:
            differences["max_value"] = {"old": self.max_value, "new": other.max_value}
        if self.aggregation != other.aggregation:
            differences["aggregation"] = {"old": self.aggregation, "new": other.aggregation}
        if self.variation_allowed != other.variation_allowed:
            differences["variation_allowed"] = {"old": self.variation_allowed, "new": other.variation_allowed}
        if self.data_values_column != other.data_values_column:
            differences["data_values_column"] = {"old": self.data_values_column, "new": other.data_values_column}
        if self.over_column != other.over_column:
            differences["over_column"] = {"old": self.over_column, "new": other.over_column}
        if self.column != other.column:
            differences["column"] = {"old": self.column, "new": other.column}
        if self.columns != other.columns:
            differences["columns"] = {"old": self.columns, "new": other.columns}
        return differences