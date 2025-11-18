#!/bin/bash
set -o errexit

tag="geoi"

cpath=$(pwd)  # get current path

# Check if there's an existing epos opensource deployment with the current tag
n=$(epos-opensource docker list | grep ${tag})
if [ ${#n} -gt 0 ]; then
  # Deployment exists, clean it
  echo "=== Cleaning environment ${tag}"
  epos-opensource docker clean ${tag}
else
  echo "=== Deploying environment ${tag}"
  epos-opensource docker deploy ${tag}
fi

echo "=== Copy EMSO facets"
cp "0_facets-EMSO.ttl" "conf"
echo "=== Populating environment ${tag}"
epos-opensource docker populate ${tag} "${cpath}/conf"

echo "=== Restarting ${tag}-resources-service"
docker container restart ${tag}-resources-service
# Wait until restarted
n=""
while [ ${#n} -lt 1 ]; do
  n=$(docker ps | grep ${tag}-resources-service | grep healthy)
  sleep 1
done