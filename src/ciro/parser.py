import pyhocon
import json
import re
from ciro.rules import Rule

def get_rules_from_conf(hocon_file):
    list_of_rules = []
    
    #Config file reading supressing input and dataFrameInfo keys   
    config = pyhocon.ConfigFactory.parse_file(hocon_file, resolve=False, unresolved_value="")
    json_data = json.dumps(config.as_plain_ordered_dict(), indent=4)
    datos = json.loads(json_data)
    del datos["hammurabi"]["input"], datos["hammurabi"]["dataFrameInfo"]
    datos = datos["hammurabi"]["rules"]
    
    i = 0
    
    while i < len(datos):

        try:
            id_rule = datos[i]["config"]["id"]            
        except KeyError:
            id_class = datos[i]["class"]
            
        try:
            internal_id = datos[i]["config"]["internalId"]
        except KeyError:
            internal_id = ""

        if ("6.9" in str(id_rule) or "5.4" in str(id_rule) or "5.5" in str(id_rule) or "2.1" in str(id_rule) or "2.3" in str(id_rule) or 
            "6.9" in str(internal_id) or "5.4" in str(internal_id) or "5.5" in str(internal_id) or "2.1" in str(internal_id) or "2.3" in str(internal_id)):
            column = "No aplica, regla a nivel objeto"
        else:
            try:
                column = datos[i]["config"]["column"]
            except KeyError:
                column = datos[i]["config"]["columns"]
                patron_no_brackets = r"\['(.+)'\]"
                column_no_brackets = re.findall(patron_no_brackets, str(column))
                patron_no_quote = r"\'"
                column_no_brackets_no_quote = re.sub(patron_no_quote,"",column_no_brackets[0])
                column = column_no_brackets_no_quote
        
        try:
            acceptance_min = datos[i]["config"]["acceptanceMin"]
        except KeyError:
            acceptance_min = 100

        try:
            is_critical = datos[i]["config"]["isCritical"]
        except KeyError:
            if "5.3" in str(id_rule) or "5.5" in str(id_rule):
                is_critical = False
            else:
                is_critical = ""     
    
        try:
            with_refusals = datos[i]["config"]["withRefusals"]
        except KeyError:
            if "5.3" in str(id_rule):
                with_refusals = False
            else:
                with_refusals = ""       
    
        if is_critical == True:
            is_critical = "TRUE"
        if is_critical == False:
            is_critical = "FALSE"
        if with_refusals == True:
            with_refusals = "TRUE"
        if with_refusals == False:
            with_refusals = "FALSE" 

     
        try:
            min_threshold = datos[i]["config"]["minThreshold"]
        except KeyError:
            min_threshold = ""
        
        try:
            target_threshold = datos[i]["config"]["targetThreshold"]
        except KeyError:
            target_threshold = ""

        conf_name = hocon_file                
        subset = ""
        temporal_path= ""
        detail = ""    
        rule_obj = Rule(id_rule,column,acceptance_min,is_critical,with_refusals,min_threshold,target_threshold,internal_id,temporal_path,subset,detail,conf_name)

        list_of_rules.append(rule_obj)         
        i += 1
        
    return list_of_rules