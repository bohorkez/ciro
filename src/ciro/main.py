import rules
import os
import re
import fnmatch
from tabulate import tabulate

def get_unique_filename(base_name="output", extension=".txt"):
    """Genera un nombre de archivo único si el archivo ya existe."""
    counter = 0
    filename = f"{base_name}{extension}"
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name} ({counter}){extension}"
    return filename

def sort_file(file_path):
    # Leer el contenido del archivo
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()
    
    # Ordenar las líneas alfabéticamente
    sorted_lines = sorted(lines)
    
    # Sobrescribir el archivo con las líneas ordenadas
    with open(file_path, "w", encoding="utf-8") as file:
        file.writelines(sorted_lines)

def compare_configs():
    old_file = None
    new_file = None
    for file in os.listdir('.'):
        if file == 'old.conf':
            old_file = file
        if file == 'new.conf':
            new_file = file
    
    if old_file is None or new_file is None:
        print("No se encontraron los archivos old.conf o new.conf en el directorio actual.")
        exit(1)
    
    #old_quality_rules = rules.read_rules_from_hocon("D:\\Julio\\Training\\ciro\\src\\ciro\\old.conf")
    old_quality_rules = rules.read_rules_from_hocon(old_file)
    print(f"Loaded {len(old_quality_rules)} rules.")
    
    #new_quality_rules = rules.read_rules_from_hocon("D:\\Julio\\Training\\ciro\\src\\ciro\\new.conf")
    new_quality_rules = rules.read_rules_from_hocon(new_file)
    print(f"Loaded {len(new_quality_rules)} rules.")
    
    # Crear diccionarios por id con objetos Rule
    old_rules_by_id = {rule.id: rule for rule in old_quality_rules}
    new_rules_by_id = {rule.id: rule for rule in new_quality_rules}
    
    # Obtener todos los ids únicos
    all_ids = set(old_rules_by_id.keys()) | set(new_rules_by_id.keys())
    
    # Comparar reglas
    differences = {}
    for id in all_ids:
        old_rule = old_rules_by_id.get(id)
        new_rule = new_rules_by_id.get(id)
        if old_rule and new_rule:            
            differences[id] = old_rule.diff(new_rule)  # Obtener diferencias detalladas
            if differences[id] == {}:
                del differences[id]
        elif old_rule:
            differences[id] = {"old": old_rule.id, "new": None}  # Regla eliminada
        elif new_rule:
            differences[id] = {"old": None, "new": new_rule.id}  # Regla añadida

    # Escribir diferencias
    with open(get_unique_filename(), "w", encoding="utf-8") as output_file:  # Abre el archivo en modo escritura
        for diff_id, diff in differences.items():
            if list(diff.keys()) == ['old', 'new']:
                if diff_id == diff['old'] and diff['new'] is None:
                    output_file.write(f"Se elimina la regla: {diff_id}\n")
                elif diff_id == diff['new'] and diff['old'] is None:
                    output_file.write(f"Se añade la regla: {diff_id}\n")
            else:
                output_file.write(f"Se modifica la regla: {diff_id} -> ")
                output_file.write(f"{diff}\n")
    sort_file(output_file.name)
    print("Comparación completada. -> " + output_file.name)

def get_tables():
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.conf'):
            result = get_conf_tables(file)
            print("Archivo: " + str(file))
            for table in result:
                print(table)

def convert_hocon_to_json():
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.conf'):
            conf_rules = rules.read_rules_from_hocon(file)
            json_output = [rule.to_dict() for rule in conf_rules]
            output_file = get_unique_filename(base_name=file[:-5], extension=".json")
            with open(output_file, 'w', encoding='utf-8') as json_file:
                import json
                json.dump(json_output, json_file, indent=4)
            print(f"Archivo JSON generado: {output_file}")

def get_variables():
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.conf'):
            env_variables = get_conf_variables(file)
            print("Archivo: " + str(file))
            for element in env_variables:
                print(element)                       

def get_conf_tables(conf_file):
    #lectura del archivo conf
    hocon_file = open(conf_file,'r')
    data = hocon_file.read()
    hocon_file.close()
    
    #se obtienen las tablas del conf
    #pattern_ant = r'\/data\/master\/[^"]*\/data\/[^"]*'
    pattern = r'\/data\/master\/[^"]*\/data\/[^\/"]*'
    matches = re.findall(pattern, data)
    if matches == []:
        pattern = r'(?<=\.)t_\w+'
        matches = re.findall(pattern, data)

    return list(dict.fromkeys(matches))

def get_conf_variables(conf_file):
    #lectura del archivo conf
    hocon_file = open(conf_file,'r')
    data = hocon_file.read()
    hocon_file.close()
    
    #se obtienen las variables de entorno
    pattern = r'\$\{([^\}]+)\}'
    matches = re.findall(pattern, data)
    
    #se eliminan las variables de entorno duplicadas y se omiten variables de entorno no deseadas
    matches_without_duplicates = []
    omitted_variables = ["ARTIFACTORY_UNIQUE_CACHE","?TEST_PATH","SCHEMAS_REPOSITORY","MASTERSCHEMA"]
    
    for match in matches:
        if (match not in matches_without_duplicates) and (match not in omitted_variables):
            matches_without_duplicates.append(match)
    
    try:
        index = matches_without_duplicates.index("ENTIFIC_ID")
        matches_without_duplicates.insert(0, matches_without_duplicates.pop(index))
    except ValueError:
        pass

    return matches_without_duplicates


def show_rules(conf_file):
    conf_rules = []
    rules_from_all_confs = []    
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.conf'):
            conf_rules = rules.read_rules_from_hocon(file)
            for rule in conf_rules:
                rules_from_all_confs.append(rule)

    #Conversion de la lista de objetos Regla en una lista de cadenas de texto para escribirlas en archivo
    rule_in_list_format = []
    rules_summary = []
    for rule in rules_from_all_confs:
        if "_" in str(rule.id):
            rule_in_list_format.append(rule.id)
        else:
            rule_in_list_format.append(rule.id + " " + "(" + rule.internal_id + ")")
    
        rule_in_list_format.append(rule.column)
        rule_in_list_format.append(rule.min_threshold)
        rule_in_list_format.append(rule.target_threshold)
        rule_in_list_format.append(rule.acceptance_min)
        rule_in_list_format.append(rule.is_critical)
        rule_in_list_format.append(rule.with_refusals)
        rule_in_list_format.append(rule.conf_name)
        rules_summary.append(rule_in_list_format)
        rule_in_list_format = []

    #Escritura de archivo de resultados
    titles = ["ID", "column", "minThreshold", "targetThreshold", "acceptanceMin", "isCritical", "withRefusals", "Conf."]
    result_file = open('resultado.txt', 'w')
    result_file.write(tabulate(rules_summary, headers=titles))
    result_file.close()
    print("Carga completada. -> " + result_file.name)

def get_path(argument):
    # Definicion de tablas conocidas
    known_tables = {"t_kbtq_eom_customer": "/g_entific_id=PE/gf_cutoff_date=2040-07-11/gf_information_origin_id=PE",
                    "t_rzua_ctrct_goods_emiss_intst": "/g_entity_id=PE0011/gf_cutoff_date=2040-07-11",
                    "t_krdc_issuances_fixed_income": "/gf_odate_date_id=2024-07-11",
                    "t_kgug_guarantees":"/g_entific_id=PE/gf_cutoff_date=2040-07-11/gf_information_origin_id=LEDT"
                }    
    uuaa = argument[2:6]
    
    if argument not in known_tables:    
        response = "hdfs dfs -ls -R /data/master/"+uuaa+"/data/"+argument    
    else:
        response = "hdfs dfs -ls -R /data/master/"+uuaa+"/data/"+argument+known_tables[argument]
    print(response)

def show_detail(conf_file):
    conf_rules = []
    rules_from_all_confs = []    
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.conf'):
            conf_rules = rules.read_rules_from_hocon(file)
            for rule in conf_rules:
                rules_from_all_confs.append(rule)

    #Conversion de la lista de objetos Regla en una lista de cadenas de texto para escribirlas en archivo
    rule_in_list_format = []
    rules_summary = []
    for rule in rules_from_all_confs:
        if "_" in str(rule.id):
            id = rule.id
        else:
            id = rule.id + " " + "(" + rule.internal_id + ")"

        if "6.9" in str(id) or "5.4" in str(id) or "5.5" in str(id) or "2.1" in str(id) or "2.3" in str(id):
            column = "No aplica, regla a nivel objeto"
        else:
            if "4.2" in str(id):
                column = "Multiple columns"
            else:
                column = rule.column 

        rule_class = rule.rule_class
        parent_class = rule.parent_class         
        temporal_path = "Y" if rule.temporal_path else ""       

        detail = ""
        if "3.2" in str(id):
            detail = rule.format            
        elif "3.3" in str(id):
            detail = "Forbidden: " + str(rule.values)
        elif "3.4" in str(id):
            detail += "lowerBound: " + str(rule.lower_bound) + " "
            detail += "upperBound: " + str(rule.upper_bound) + " "
        elif "3.5" in str(id):
            if "StaticCatalogRule" in str(rule_class) or "StaticCatalogRule" in str(parent_class):
                detail += "Allowed values " + str(rule.values)
            else:                
                if "DynamicCatalogRule" in str(rule_class) or "DynamicCatalogRule" in str(parent_class):
                    values = rule.data_values_subset
                    if values == "":
                        values = rule.data_values_condition
                    
                    ktny_path = rule.data_values_paths
                    if "gf_frequency_type=D" in str(ktny_path):
                        detail += " Type = 'D' "
                    else:
                        if "gf_frequency_type=M" in str(ktny_path):
                            detail += " Type = 'M' "
                    detail += values                        
        elif "4.3" in str(id):            
            value_conciliation_path = rule.data_values_paths
            pattern = r"t_.*?(?=\/|$)"
            match = re.search(pattern, str(value_conciliation_path))
            if match:
                value_conciliation_path = match.group(0)

        elif "5.2" in str(id):
            value_comparison_path = rule.data_values_paths
            pattern = r"t_[^\"\/']+"
            match = re.search(pattern, str(value_comparison_path))
            if match:
                value_comparison_path = match.group(0)

            detail = rule.condition                             
            
        elif "5.3" in str(id):
            detail = rule.condition

        elif "5.4" in str(id):
            detail = rule.condition                     
    
        rule.detail = detail
        
        rule_in_list_format.append(id)
        rule_in_list_format.append(column)        
        rule_in_list_format.append(rule.is_critical)
        rule_in_list_format.append(rule.detail)    
        rule_in_list_format.append(temporal_path)    
        rule_in_list_format.append(rule.subset)
        rule_in_list_format.append(rule.conf_name)

        rules_summary.append(rule_in_list_format)
        rule_in_list_format = []

    #Escritura de archivo de resultados
    titles = ["ID", "column", "isCritical", "detail", "temp","subset", "Conf."]
    result_file = open('detail.txt', 'w')
    result_file.write(tabulate(rules_summary, headers=titles))
    result_file.close()
    print("Carga completada. -> " + result_file.name)    

# Punto de entrada
if __name__ == "__main__":
    # Ejemplo de menú o argumentos
    import argparse
    parser = argparse.ArgumentParser(description="Hammurabi confs analysis tool.")
    parser.add_argument("--comp",action="store_true", help="Get the differences between two configuration files.")
    parser.add_argument("--tables",action="store_true", help="Get the tables in a configuration file.")
    parser.add_argument("--vars",action="store_true", help="Get the variables in a configuration file.")
    parser.add_argument("--path", metavar="PATH", help="Get the full path of a table.")
    parser.add_argument("--rules", action="store_true", help="Show rules from configuration files.")
    parser.add_argument("--detail", action="store_true", help="Show rules with detailed information.")
    parser.add_argument("--json", action="store_true", help="Convert HOCON to JSON format.")
    args = parser.parse_args()

    if args.comp:
        compare_configs()
    elif args.tables:
        get_tables()
    elif args.vars:
        get_variables()
    elif args.path:
        get_path(args.path)
    elif args.rules:
        show_rules(args.rules)
    elif args.detail:
        show_detail(args.detail)        
    elif args.json:
        convert_hocon_to_json()
    else:
        print("Por favor, usa --help para ver las opciones disponibles.")