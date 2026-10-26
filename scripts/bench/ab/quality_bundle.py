"""The artifacts of the quality report: the comment artifact for the commenter, the data artifact, schema validation and the Markdown safety helpers.

The job runs the PR's own code and the commenter (a trusted `workflow_run` job with a write token) treats what it uploads as untrusted data.
`lh-proof-comment` (manifest.json + pr-quality.md) is the commenter's contract; `check_manifest` / `check_body` hold this producer to it.
`pr-quality-data` (quality.json) is validated against schemas/pr-quality.schema.json by `validate` (a small validator for the subset the
schema uses). Everything that reaches the body goes through `md_safe` / `md_code` and the size cap `cap_body`.
"""
from __future__ import annotations

import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMAS = os.path.join(HERE, "schemas")
SCHEMA_VERSION = 1
BODY_CAP = 60000  # GitHub rejects a comment above 65536 characters
MAX_FILE_BYTES = 5_000_000
QUALITY_FILE, REPORT_FILE, MANIFEST_FILE = "quality.json", "pr-quality.md", "manifest.json"


# ------------------------------------------------------------------------------------------------ schema validation
def load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS, name), encoding="utf-8") as f:
        return json.load(f)


_PY_TYPES = {"string": str, "array": list, "object": dict}


def _is_type(v, t: str) -> bool:
    if t == "null":
        return v is None
    if t == "boolean":
        return isinstance(v, bool)
    if t == "integer":
        return isinstance(v, int) and not isinstance(v, bool)
    if t == "number":
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    return isinstance(v, _PY_TYPES[t])


def validate(doc, schema: dict, root: dict | None = None, path: str = "$") -> list[str]:
    """Errors of `doc` against `schema` (keywords: $ref, anyOf, const, enum, type, required, properties, additionalProperties,
    maxProperties, items, maxItems, maxLength, minimum, maximum, pattern). Empty list: valid."""
    root = root or schema
    if "$ref" in schema:
        node = root
        for part in schema["$ref"].lstrip("#/").split("/"):
            node = node[part]
        return validate(doc, node, root, path)
    if "anyOf" in schema:
        trials = [validate(doc, s, root, path) for s in schema["anyOf"]]
        return [] if any(not t for t in trials) else [f"{path}: matches none of the allowed shapes ({'; '.join(t[0] for t in trials if t)})"]
    errs: list[str] = []
    if "const" in schema and (doc != schema["const"] or isinstance(doc, bool) != isinstance(schema["const"], bool)):
        return [f"{path}: must be {schema['const']!r}"]
    if "enum" in schema and not any(doc == e and type(doc) is type(e) for e in schema["enum"]):
        return [f"{path}: {doc!r} is not one of {schema['enum']!r}"]
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_is_type(doc, t) for t in types):
            return [f"{path}: expected {'/'.join(types)}, got {type(doc).__name__}"]
    if isinstance(doc, str):
        if len(doc) > schema.get("maxLength", 10**9):
            errs.append(f"{path}: longer than {schema['maxLength']} characters")
        if "pattern" in schema and not re.search(schema["pattern"], doc):
            errs.append(f"{path}: does not match {schema['pattern']}")
    if isinstance(doc, (int, float)) and not isinstance(doc, bool):
        if "minimum" in schema and doc < schema["minimum"]:
            errs.append(f"{path}: below {schema['minimum']}")
        if "maximum" in schema and doc > schema["maximum"]:
            errs.append(f"{path}: above {schema['maximum']}")
    if isinstance(doc, list):
        if len(doc) > schema.get("maxItems", 10**9):
            errs.append(f"{path}: more than {schema['maxItems']} items")
        if "items" in schema:
            for i, v in enumerate(doc[:schema.get("maxItems", len(doc))]):
                errs += validate(v, schema["items"], root, f"{path}[{i}]")
    if isinstance(doc, dict):
        if len(doc) > schema.get("maxProperties", 10**9):
            errs.append(f"{path}: more than {schema['maxProperties']} properties")
        for k in schema.get("required", []):
            if k not in doc:
                errs.append(f"{path}: missing {k!r}")
        props = schema.get("properties", {})
        extra = schema.get("additionalProperties", True)
        for k, v in doc.items():
            if k in props:
                errs += validate(v, props[k], root, f"{path}.{k}")
            elif extra is False:
                errs.append(f"{path}: unexpected property {k!r}")
            elif isinstance(extra, dict):
                errs += validate(v, extra, root, f"{path}.{k}")
    return errs


def validate_quality(doc) -> list[str]:
    return validate(doc, load_schema("pr-quality.schema.json"))


# ------------------------------------------------------------------------------------------------ the comment artifact
# The commenter (.github/workflows/pr-proof-comment.yaml) reads ONE artifact named `lh-proof-comment`: manifest.json and one <kind>.md per comment.
# It sanitises every body again as untrusted data; this producer writes only what it accepts, and `check_body` / `check_manifest` hold the
# producer to the documented rules (docs/ci-proof-comment.md in the commenter's change) so a body is never silently neutralised.
KIND = "pr-quality"
COMMENT_ARTIFACT = "lh-proof-comment"
DATA_ARTIFACT = "pr-quality-data"
VERDICTS = ("pass", "fail", "incomplete", "skipped")
COMMENT_CAP = 65000
MANIFEST_KEYS = {"schema_version", "pr_number", "head_sha", "comments", "images", "allowed_issue_refs"}
ALLOWED_TAGS = ("details", "summary", "sub")
REPO_LINK = re.compile(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/(?:actions/runs/\d+|issues/\d+|pull/\d+|commit/[0-9a-f]{7,40})")


def proof_manifest(pr_number: int, head_sha: str, verdict: str, allowed_issue_refs: list[int]) -> dict:
    return {"schema_version": SCHEMA_VERSION, "pr_number": pr_number, "head_sha": head_sha, "comments": [{"kind": KIND, "verdict": verdict}],
            "images": [], "allowed_issue_refs": sorted(set(allowed_issue_refs))[:20]}


def check_manifest(doc) -> list[str]:
    """Problems of a manifest.json against the commenter's contract (scripts/ci/proof-comment-manifest.schema.json); this producer posts one kind, no image."""
    if not isinstance(doc, dict) or set(doc) != MANIFEST_KEYS:
        return [f"keys must be exactly {sorted(MANIFEST_KEYS)}"]
    errs = []
    if doc["schema_version"] != 1 or isinstance(doc["schema_version"], bool):
        errs.append("schema_version must be 1")
    if not (isinstance(doc["pr_number"], int) and not isinstance(doc["pr_number"], bool) and 1 <= doc["pr_number"] <= 1_000_000_000):
        errs.append("pr_number must be a positive integer")
    if not (isinstance(doc["head_sha"], str) and re.fullmatch(r"[0-9a-f]{40}", doc["head_sha"])):
        errs.append("head_sha must be 40 lower-case hex")
    c = doc["comments"]
    if not (isinstance(c, list) and len(c) == 1 and isinstance(c[0], dict) and set(c[0]) == {"kind", "verdict"} and c[0]["kind"] == KIND and c[0]["verdict"] in VERDICTS):
        errs.append(f"comments must be exactly one {{kind: {KIND}, verdict: one of {list(VERDICTS)}}}")
    if doc["images"] != []:
        errs.append("pr-quality carries no image")
    r = doc["allowed_issue_refs"]
    if not (isinstance(r, list) and len(r) <= 20 and len(set(r)) == len(r) and all(isinstance(x, int) and not isinstance(x, bool) and 1 <= x <= 9999999 for x in r)):
        errs.append("allowed_issue_refs must be up to 20 distinct issue numbers")
    return errs


def check_body(text: str, allowed_refs=()) -> list[str]:
    """Problems of a comment body against the commenter's rules: size, the first line a marker, only attribute-free details/summary/sub tags,
    no image, links only to this host's runs/issues/pulls/commits, `#N` only for the allowed numbers."""
    errs = []
    if len(text) > COMMENT_CAP:
        errs.append(f"{len(text)} characters, above {COMMENT_CAP}")
    if not text.startswith("<!-- lh-pr-quality -->\n"):
        errs.append("the first line must be the marker")
    body = text.split("\n", 1)[1] if "\n" in text else ""
    body = re.sub(r"`[^`\n]*`", "", body)  # a code span renders literally
    for m in re.finditer(r"<(/?)([A-Za-z][A-Za-z0-9]*)([^>]*)>", body):
        if m.group(2).lower() not in ALLOWED_TAGS or m.group(3).strip():
            errs.append(f"tag not allowed: {m.group(0)[:40]}")
    if "![" in body:
        errs.append("an image is not allowed")
    for m in re.finditer(r"\]\(([^)]*)\)", body):
        if not REPO_LINK.fullmatch(m.group(1)):
            errs.append(f"link not allowed: {m.group(1)[:60]}")
    prose = re.sub(r"https://github\.com/[^\s)]*", "", body)
    for m in re.finditer(r"(?<![\w&/])#(\d+)", prose):
        if int(m.group(1)) not in set(allowed_refs):
            errs.append(f"#{m.group(1)} is not in allowed_issue_refs")
    if re.search(r"(?<![\w`])@[A-Za-z0-9]", prose) or re.search(r"https?://", prose):
        errs.append("a bare mention or URL")
    return errs


def write_comment_artifact(out_dir: str, manifest: dict, body: str) -> None:
    """manifest.json and pr-quality.md, after checking both; raises ValueError with the problems."""
    errs = check_manifest(manifest) + check_body(body, manifest["allowed_issue_refs"])
    if errs:
        raise ValueError("; ".join(errs[:10]))
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, MANIFEST_FILE), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
        f.write("\n")
    with open(os.path.join(out_dir, REPORT_FILE), "w", encoding="utf-8") as f:
        f.write(body)


def verify_comment_artifact(directory: str) -> list[str]:
    """Problems of a `lh-proof-comment` directory as this producer writes it (exactly manifest.json and pr-quality.md, regular files, valid)."""
    names = set(os.listdir(directory))
    if names != {MANIFEST_FILE, REPORT_FILE}:
        return [f"the artifact holds {sorted(names)}, expected exactly {sorted([MANIFEST_FILE, REPORT_FILE])}"]
    for n in names:
        p = os.path.join(directory, n)
        if os.path.islink(p) or not os.path.isfile(p) or os.path.getsize(p) > MAX_FILE_BYTES:
            return [f"{n} is not a regular file of at most {MAX_FILE_BYTES} bytes"]
    try:
        with open(os.path.join(directory, MANIFEST_FILE), encoding="utf-8") as f:
            manifest = json.load(f)
        with open(os.path.join(directory, REPORT_FILE), encoding="utf-8") as f:
            body = f.read()
    except (OSError, ValueError) as e:
        return [f"unreadable: {e}"]
    errs = check_manifest(manifest)
    return errs + (check_body(body, manifest["allowed_issue_refs"]) if not errs else [])


# ------------------------------------------------------------------------------------------------ Markdown safety
_CTRL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f  ‪-‮⁦-⁩]")


def _clean(s, limit: int) -> str:
    s = _CTRL.sub("", str(s)).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return s if len(s) <= limit else s[: limit - 1] + "…"


ZWSP = "\u200b"
_ENTITY_OK = re.compile(r"&(?!(?:amp|lt|gt|#91|#93);)")


def _neutral(s: str) -> str:
    """The text with everything that could notify, link, cross-reference or open HTML defused the way the commenter's sanitiser does it
    (`<`, `[`, `]` as entities; a zero-width space after `@`, after a `#` before digits, in `www.` and `://`), so that the sanitiser
    leaves it unchanged and the result is the same however many times it is applied."""
    s = _ENTITY_OK.sub("&amp;", s)
    s = re.sub(r"@(?=[A-Za-z0-9_])", "@" + ZWSP, s)
    s = re.sub(r"(?<!&)#(?=\d)", "#" + ZWSP, s)
    s = s.replace("<", "&lt;").replace("[", "&#91;").replace("]", "&#93;")
    s = re.sub(r"(?<![A-Za-z0-9])(GH-)(?=\d)", r"\1" + ZWSP, s, flags=re.I)
    s = re.sub(r"(?<![A-Za-z0-9.])(www)\.(?=[A-Za-z0-9])", r"\1" + ZWSP + ".", s, flags=re.I)
    return s.replace("://", ":" + ZWSP + "//")


def md_safe(s, limit: int = 200) -> str:
    """Untrusted text as inert Markdown prose: no mention, link, autolink, issue reference, HTML, emphasis or table break."""
    t = _neutral(_clean(s, limit))
    return re.sub(r"([\\*_~|])", r"\\\1", t).replace("`", "'")


def md_code(s, limit: int = 200) -> str:
    """Untrusted text in a code span: backticks and table pipes cannot break out, and it carries the same defusing as `md_safe`."""
    t = _neutral(_clean(s, limit)).replace("`", "'").replace("|", "\\|")
    return f"`{t}`" if t else "``"


def cap_body(text: str, cap: int = BODY_CAP) -> str:
    """The comment body within `cap` characters: cut at a line end, with a note, the marker line (first) kept."""
    if len(text) <= cap:
        return text
    note = "\n\n_The report was cut to fit the size limit of a PR comment; the full data is in the `pr-quality` artifact of the run._\n"
    cut = text[: cap - len(note)]
    cut = cut[: cut.rfind("\n")] if "\n" in cut else cut
    return cut + note
