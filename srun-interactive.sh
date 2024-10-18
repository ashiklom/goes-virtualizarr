#!/usr/bin/env bash
set -euo pipefail

if [[ $(hostname) =~ "discover" ]]; then
  echo "Assuming Discover"
  srun --account s2826 --cpus-per-task 16 --time 00:59:00 --pty bash
else
  echo "Assuming smce-eso pcluster"
  # eso-c5n18xlarge-spot-dy-c5n18xlarge-[1-100]
  # srun -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand --pty bash
  srun --pty bash
  # salloc -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand
fi
