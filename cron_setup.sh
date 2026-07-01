#!/bin/bash
# =============================================================
# Configura o cron para sincronizacao semanal.
#
# Por padrao, roda toda segunda-feira as 03:00 (horario local).
#
# Uso:
#   ./cron_setup.sh          # Instala o cron
#   ./cron_setup.sh --remove # Remove o cron
# =============================================================

PROJECT_DIR="/Users/eduardolelis/Desktop/Databricks"
PYTHON="$PROJECT_DIR/venv/bin/python"
SCRIPT="$PROJECT_DIR/pipeline.py"
LOG="$PROJECT_DIR/pipeline.log"

# Toda segunda-feira as 03:00
SCHEDULE="0 3 * * 1"
CRON_CMD="cd $PROJECT_DIR && $PYTHON $SCRIPT --days 7 >> $LOG 2>&1"
CRON_MARKER="# databricks-mysql-sync"

if [ "$1" = "--remove" ]; then
    crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | crontab -
    echo "Cron removido."
    exit 0
fi

# Adiciona sem duplicar
(crontab -l 2>/dev/null | grep -v "$CRON_MARKER"; echo "$SCHEDULE $CRON_CMD $CRON_MARKER") | crontab -

echo "Cron instalado:"
echo "  Schedule: $SCHEDULE (toda segunda, 03:00)"
echo "  Comando:  $CRON_CMD"
echo "  Log:      $LOG"
echo ""
echo "Para verificar: crontab -l"
echo "Para remover:   ./cron_setup.sh --remove"
