# Helm release image references

Anonymous registry checks on 2026-10-04 confirmed why the chart distributed with
[v0.143.17](https://github.com/ReliablyObserve/victoria-lakehouse/releases/tag/v0.143.17)
could not pull its default images. The release publisher uses the project
repository namespace and `v`-prefixed image tags; the chart used flat repositories
and an unprefixed app version.

| Signal | Released chart reference | Anonymous status | Corrected reference | Anonymous status |
| --- | --- | --- | --- | --- |
| Logs | `ghcr.io/reliablyobserve/lakehouse-logs:0.143.17` | 403 | `ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-logs:v0.143.17` | 200 |
| Traces | `ghcr.io/reliablyobserve/lakehouse-traces:0.143.17` | 403 | `ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-traces:v0.143.17` | 200 |

Changing only the repository still fails: both corrected repositories return
404 for `0.143.17`. The successful `v0.143.17` image indexes contain Linux amd64
and arm64 manifests. Their digests are
`sha256:be9e64c5f61989fd70ff2159e71f299c9141c8c55f95241bc56d7cb470b0b271`
(logs) and
`sha256:317470e55315f5a6267a6d46213a74693e17c3a5e590cf9cdfc101a50dfeac62`
(traces).

The corrected chart renders these references for both insert and select pods.
Default installation enables logs only. Traces-only installation uses
`logs.enabled=false,traces.enabled=true`; `mode` and `lakehouseConfig.mode`
do not select the chart's enabled signals.

```bash
helm lint charts/victoria-lakehouse --strict
bash charts/victoria-lakehouse/test_images.sh
bash charts/victoria-lakehouse/test_templates.sh

helm template logs charts/victoria-lakehouse \
  --set logs.enabled=true --set traces.enabled=false
helm template traces charts/victoria-lakehouse \
  --set logs.enabled=false --set traces.enabled=true
```

The CI Helm job runs ten exact image-render assertions and the existing 38
template checks. The image assertions cover default logs, traces only, both
signals, explicit tags, custom repositories, and explicit FIPS tags. Explicit
`image.tag=build-123` remains `build-123`; custom repositories also remain
unchanged. With a blank tag, custom or explicitly selected legacy flat repositories
retain the previous unprefixed chart app-version fallback. Only each signal's
canonical published repository receives the default `v` prefix, so mixed custom
and default repositories resolve independently. Preserving a legacy reference's
rendering does not establish that its image is publicly available: the anonymous
403 results above still apply to the tested flat release references.

For example, `--set image.logs.repository=registry.example.test/custom/logs`
with a blank `image.tag` renders `registry.example.test/custom/logs:0.143.17`,
while an unchanged traces repository renders its published `v0.143.17` tag.
The regression checks both mixed directions, custom blank tags and legacy flat
blank tags. No cluster deployment or backend API change is claimed by this proof.
