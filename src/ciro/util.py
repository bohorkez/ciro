import rules
old_quality_rules = rules.read_rules_from_hocon("D:\\Julio\\Training\\ciro\\src\\ciro\\old.conf")
print(f"Loaded {len(old_quality_rules)} rules.")

new_quality_rules = rules.read_rules_from_hocon("D:\\Julio\\Training\\ciro\\src\\ciro\\new.conf")
print(f"Loaded {len(new_quality_rules)} rules.")


differences = {}
def show_results():
    # Crear diccionarios por id con objetos Rule
    old_rules_by_id = {rule.id: rule for rule in old_quality_rules}
    new_rules_by_id = {rule.id: rule for rule in new_quality_rules}
    
    # Obtener todos los ids únicos
    all_ids = set(old_rules_by_id.keys()) | set(new_rules_by_id.keys())

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
    
    # Mostrar diferencias
    if differences:
        print("Differences found between old and new quality rules:")
        for id, diff in differences.items():
            print(f"id: {id}")
            print(diff)
    else:
        print("No differences found between old and new quality rules.")