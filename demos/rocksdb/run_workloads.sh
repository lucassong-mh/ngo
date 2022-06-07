#!/bin/sh

DIR="/async_sfs"
SCRIPT="/bin/db_bench"
duration=30
threads=4
num_keys=1000000

K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

const_params="
  --db=$DIR \
  --wal_dir=$DIR \
  \
  --num=$num_keys \
  --num_levels=6 \
  --cache_numshardbits=6 \
  --compression_ratio=0.5 \
  --level_compaction_dynamic_level_bytes=true \
  --bytes_per_sync=$((8 * M)) \
  --cache_index_and_filter_blocks=0 \
  --pin_l0_filter_and_index_blocks_in_cache=1 \
  \
  --write_buffer_size=$((128 * M)) \
  --target_file_size_base=$((128 * M)) \
  --max_bytes_for_level_base=$((1 * G)) \
  \
  --verify_checksum=1 \
  --delete_obsolete_files_period_micros=$((60 * M)) \
  --max_bytes_for_level_multiplier=8 \
  \
  --statistics=0 \
  --stats_per_interval=1 \
  --stats_interval_seconds=60 \
  --histogram=1 \
  \
  --memtablerep=skip_list \
  --bloom_bits=10 \
  --open_files=-1 \
  "


params=" --level0_file_num_compaction_trigger=4 \
        --level0_stop_writes_trigger=20 \
        --max_background_compactions=16 \
        --max_write_buffer_number=8 \
        --max_background_flushes=7 \
        "

bulkload=" --benchmarks=fillrandom \
        --use_existing_db=0 \
        --threads=1 \
        --disable_auto_compactions=1 \
        --sync=0 \
        --memtablerep=vector \
        --allow_concurrent_memtable_write=false \
        --disable_wal=1 \
        --max_background_compactions=16 \
        --max_write_buffer_number=8 \
        --allow_concurrent_memtable_write=false \
        --max_background_flushes=7 \
        --level0_file_num_compaction_trigger=$((10 * M)) \
        --level0_slowdown_writes_trigger=$((10 * M)) \
        --level0_stop_writes_trigger=$((10 * M)) \
        ${const_params} \
        "

randomread=" --benchmarks=readrandom \
        --use_existing_db=1 \
        --duration=$((duration)) \
        --threads=$((threads)) \
        ${params} \
        ${const_params} \
        "

rangescan=" --benchmarks=seekrandom \
        --use_existing_db=1 \
        --duration=$((duration)) \
        --threads=$((threads)) \
        --seek_nexts=10 \
        --reverse_iterator=false \
        ${params} \
        ${const_params} \
        "

overwrite=" 
        --benchmarks=overwrite \
        --use_existing_db=1 \
        --duration=$((duration)) \
        --threads=$((threads)) \
        --sync=0 \
        --writes=1250000 \
        --subcompactions=4 \
        --soft_pending_compaction_bytes_limit=$((1 * T)) \
        --hard_pending_compaction_bytes_limit=$((4 * T)) \
        ${params} \
        ${const_params} \
        "


${SCRIPT}  ${bulkload}

${SCRIPT}  ${randomread}

${SCRIPT}  ${rangescan}

${SCRIPT}  ${overwrite}
