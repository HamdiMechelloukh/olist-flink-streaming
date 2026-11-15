#!/usr/bin/env bash
# Cree les topics avant toute soumission de job.
#
# Pourquoi c'est necessaire : l'auto-creation Kafka ne se declenche qu'a la
# premiere ECRITURE. Les jobs Flink, eux, demarrent en LISANT le topic. Soumis
# avant le simulateur, leur source Kafka echoue sur
#   RuntimeException: Failed to get metadata for topics [orders]
# et les jobs bouclent en RESTARTING jusqu'a ce que quelque chose ecrive.
#
# Le creer explicitement supprime ce demarrage en crash-loop et donne la
# maitrise du nombre de partitions, qui plafonne le debit : voir
# docs/BENCHMARKS.md.
set -euo pipefail

PARTITIONS="${TOPIC_PARTITIONS:-4}"
CONTAINER="${KAFKA_CONTAINER:-docker-kafka-1}"
KT="docker exec $CONTAINER /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

TOPICS=(orders revenue-by-category order-alerts realtime-kpis)

echo "Attente de Kafka..."
for i in $(seq 1 60); do
  if $KT --list > /dev/null 2>&1; then
    echo "  pret apres ${i}x2s"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "Kafka injoignable dans $CONTAINER" >&2
    exit 1
  fi
  sleep 2
done

echo "Creation des topics a $PARTITIONS partition(s)..."
existing=$($KT --list 2>/dev/null || true)
for t in "${TOPICS[@]}"; do
  if echo "$existing" | grep -qx "$t"; then
    echo "  $t : deja present"
  else
    $KT --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 > /dev/null
    echo "  $t : cree"
  fi
done

echo "Verification..."
fail=0
for t in "${TOPICS[@]}"; do
  got=$($KT --describe --topic "$t" 2>/dev/null | grep -oP 'PartitionCount:\s*\K[0-9]+' || echo 0)
  printf '  %-22s %s partition(s)' "$t" "$got"
  if [ "$got" = "$PARTITIONS" ]; then
    echo ""
  else
    echo "   <- attendu $PARTITIONS"
    # Un topic preexistant peut avoir moins de partitions : on le signale sans
    # echouer, la suppression d'un topic Kafka etant asynchrone et risquee ici.
    [ "$got" = "0" ] && fail=1
  fi
done

exit "$fail"
