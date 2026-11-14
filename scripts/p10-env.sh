# Source me: the P10 test environment (validation.md "Test classes").
# Reuses scripts/p9-env.sh (crawler2's dev stack: Scylla, Redis, MinIO) with a
# separate throwaway keyspace and prefixes, and points the matcher at the
# fingerprinter-owned dev Qdrant (deploy/qdrant/compose.yaml; key in
# deploy/qdrant/.env, untracked). Tests create and delete their own
# collections (prefix fp_), never touching another keyspace's state.
_fp=${FINGERPRINTER_DIR:-$HOME/anti_piracy/fingerprinter}
export FINGERPRINTER_SCYLLA__KEYSPACE=${FINGERPRINTER_SCYLLA__KEYSPACE:-fingerprinter_it_p10}
export FINGERPRINTER_REDIS__PREFIX=${FINGERPRINTER_REDIS__PREFIX:-fpit10:}
export FINGERPRINTER_REDIS__STREAM_PREFIX=${FINGERPRINTER_REDIS__STREAM_PREFIX:-fpit10-events:}
export FINGERPRINTER_MINIO__REPRESENTATIONS_BUCKET=${FINGERPRINTER_MINIO__REPRESENTATIONS_BUCKET:-fp-it10-representations}
export FINGERPRINTER_MINIO__TARGETS_BUCKET=${FINGERPRINTER_MINIO__TARGETS_BUCKET:-fp-it10-targets}
. "$_fp/scripts/p9-env.sh"
set -a; . "$_fp/deploy/qdrant/.env"; set +a
export FINGERPRINTER_INDEX__URL=${FINGERPRINTER_INDEX__URL:-http://127.0.0.1:${FINGERPRINTER_INDEX_HTTP_PORT:-16333}}
export RUN_P10_INTEGRATION=1
unset _fp
