prefix="rule-collector-p-otc-fwd-inter-"

# Lista con el nombre de las reglas que deseas omitir
rules_to_skip=("rule-collector-p-otc-fwd-inter-USDCRC" "rule-collector-p-otc-fwd-inter-USDGTQ" "rule-collector-p-otc-fwd-inter-USDINR")

# Lista con el nombre de todas las reglas
rule_names=$(aws events list-rules --name-prefix $prefix --query 'Rules[].Name' --output text)

# Desactivar cada regla de la lista obtenida, omitiendo las reglas especificadas
for rule in $rule_names; do
    # Verificar si la regla actual debe ser omitida
    skip_rule=false
    for skip_rule_name in "${rules_to_skip[@]}"; do
        if [ "$rule" == "$skip_rule_name" ]; then
            skip_rule=true
            break
        fi
    done

    # Desactivar la regla solo si no debe ser omitida
    if [ "$skip_rule" == false ]; then
        aws events disable-rule --name $rule
    else
        echo "Omitiendo regla: $rule"
    fi
done