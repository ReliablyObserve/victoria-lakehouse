#!/usr/bin/env bash
# Run a Lakehouse-relevant subset of ceph/s3-tests against an S3 endpoint.
#
#   tests/s3compat/s3tests.sh <endpoint-host:port> <access> <secret> <out-dir>
#
# Env: S3TESTS_DIR  checkout of https://github.com/ceph/s3-tests (default /tmp/s3bake-work/s3-tests)
#      PYTHON       python with s3-tests requirements + pytest-timeout (default /tmp/s3bake-work/venv/bin/python)
#      S3TESTS_REF  informational only (recorded in summary)
#
# Writes <out-dir>/s3tests.conf, <out-dir>/<group>.txt (pytest -rfE output) and
# <out-dir>/summary.tsv (group, passed, failed, errors, skipped, deselected).
set -uo pipefail
hp=${1:?host:port}; ak=${2:?access}; sk=${3:?secret}; out=${4:?out-dir}
dir=${S3TESTS_DIR:-/tmp/s3bake-work/s3-tests}
py=${PYTHON:-/tmp/s3bake-work/venv/bin/python}
mkdir -p "$out"
host=${hp%:*}; port=${hp#*:}
cat >"$out/s3tests.conf" <<EOF
[DEFAULT]
host = $host
port = $port
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3t-{random}-

[s3 main]
display_name = main
user_id = main
email = main@example.com
api_name = default
access_key = $ak
secret_key = $sk

[s3 alt]
display_name = alt
user_id = alt
email = alt@example.com
access_key = $ak
secret_key = $sk

[s3 tenant]
display_name = tenant
user_id = tenant
email = tenant@example.com
access_key = $ak
secret_key = $sk
tenant = tenant

[iam]
display_name = iam
user_id = iam
email = iam@example.com
access_key = $ak
secret_key = $sk

[iam root]
access_key = $ak
secret_key = $sk
user_id = iamroot
email = iamroot@example.com

[iam alt root]
access_key = $ak
secret_key = $sk
user_id = iamaltroot
email = iamaltroot@example.com
EOF

# Group -> pytest -k expression (all in s3tests/functional/test_s3.py).
# Features Lakehouse never uses (ACLs, policies, SSE, versioning, object lock,
# lifecycle, website, CORS, tagging, logging, torrent) are deselected.
declare -A K=(
  [listing]='(test_bucket_list_ or test_bucket_listv2_ or test_basic_key_count or test_bucket_list_return_data)'
  [ranges]='(test_ranged_)'
  [multipart]='(multipart or test_abort_multipart or test_list_multipart)'
  [conditional_writes]='(ifnonmatch or ifmatch or if_none_match or if_match or conditional_write)'
  [checksums]='(checksum)'
  [delete_objects]='(test_multi_object_delete or test_object_delete or test_bucket_delete or test_delete_objects)'
  [copy]='(test_object_copy_)'
  [basic_rw]='(test_object_write or test_object_read or test_object_set_get or test_object_head or test_object_create_unreadable or test_object_write_check_etag or test_bucket_create or test_bucket_head or test_bucket_delete_notexist or test_bucket_notexist or test_bucket_recreate)'
)
order=(listing ranges multipart conditional_writes checksums delete_objects copy basic_rw)
: >"$out/summary.tsv"
cd "$dir" || exit 2
for g in "${order[@]}"; do
  expr="${K[$g]} and not acl and not policy and not sse and not encrypt and not versioning and not lock and not lifecycle and not website and not cors and not tagging and not logging and not torrent and not anonymous and not presign and not post_object and not _header_"
  case $g in
    checksums) mark=(-m checksum) ;;
    conditional_writes) mark=() ;;
    *) mark=() ;;
  esac
  S3TEST_CONF="$out/s3tests.conf" "$py" -m pytest s3tests/functional/test_s3.py "${mark[@]}" -k "$expr" \
    -p no:cacheprovider --timeout=120 -q -rfE --tb=line -o addopts= >"$out/$g.txt" 2>&1
  line=$(tail -n 1 "$out/$g.txt")
  p=$(echo "$line" | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+'); f=$(echo "$line" | grep -oE '[0-9]+ failed' | grep -oE '[0-9]+')
  e=$(echo "$line" | grep -oE '[0-9]+ error' | grep -oE '[0-9]+'); s=$(echo "$line" | grep -oE '[0-9]+ skipped' | grep -oE '[0-9]+')
  d=$(echo "$line" | grep -oE '[0-9]+ deselected' | grep -oE '[0-9]+')
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$g" "${p:-0}" "${f:-0}" "${e:-0}" "${s:-0}" "${d:-0}" >>"$out/summary.tsv"
  echo "$g: passed=${p:-0} failed=${f:-0} errors=${e:-0} skipped=${s:-0}"
done
