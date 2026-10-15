# Source me: points the P9 fingerprinter at crawler2's dev compose stack (design §15).
# Throwaway keyspace/buckets/key prefixes so tests never touch a real deployment:
# keyspace fingerprinter_it_p9, buckets fp-it-*, Redis keys fpit:*, streams fpit-events:*.
# Override any FINGERPRINTER_* variable before sourcing to change a value.
_c2=${CRAWLER2_DIR:-$HOME/anti_piracy/crawler2}
set -a; . "$_c2/.env"; set +a
_scylla_ip=$(sg docker -c "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${COMPOSE_PROJECT_NAME:-crawler2}-scylla-1")
export FINGERPRINTER_SCYLLA__CONTACT_POINTS=${FINGERPRINTER_SCYLLA__CONTACT_POINTS:-$_scylla_ip}
export FINGERPRINTER_SCYLLA__KEYSPACE=${FINGERPRINTER_SCYLLA__KEYSPACE:-fingerprinter_it_p9}
export FINGERPRINTER_REDIS__HOST=127.0.0.1 FINGERPRINTER_REDIS__PORT=${REDIS_HOST_PORT:-16379}
export FINGERPRINTER_REDIS__PREFIX=${FINGERPRINTER_REDIS__PREFIX:-fpit:}
export FINGERPRINTER_REDIS__STREAM_PREFIX=${FINGERPRINTER_REDIS__STREAM_PREFIX:-fpit-events:}
export FINGERPRINTER_MINIO__ENDPOINT=127.0.0.1:${MINIO_HOST_PORT:-19000}
export FINGERPRINTER_MINIO__ACCESS_KEY=$MINIO_ROOT_USER FINGERPRINTER_MINIO__SECRET_KEY=$MINIO_ROOT_PASSWORD
export FINGERPRINTER_MINIO__REPRESENTATIONS_BUCKET=${FINGERPRINTER_MINIO__REPRESENTATIONS_BUCKET:-fp-it-representations}
export FINGERPRINTER_MINIO__TARGETS_BUCKET=${FINGERPRINTER_MINIO__TARGETS_BUCKET:-fp-it-targets}
export RUN_P9_INTEGRATION=1
unset _c2 _scylla_ip
