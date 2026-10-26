"""The `pr-quality` artifact: schema validation, the manifest, and the Markdown safety helpers of the comment.

The artifact is produced by an untrusted-code job (the PR's own tree) and consumed by a trusted `workflow_run` job that holds a write token.
So the consumer never trusts it: `validate` checks a document against the schemas in schemas/ (a small validator for the subset those
schemas use, so the consumer needs no extra package), `verify_bundle` checks the manifest against the two files, and everything
that reaches the comment goes through `md_safe` / `md_code` and the size cap `cap_body`.
"""
from __future__ import annotations

import hashlib
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


def validate_manifest(doc) -> list[str]:
    return validate(doc, load_schema("pr-quality-manifest.schema.json"))


# ------------------------------------------------------------------------------------------------ the artifact
def sha256_file(path: str) -> dict:
    with open(path, "rb") as f:
        data = f.read()
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def manifest_for(out_dir: str, pr_number: int, head_sha: str, base_sha: str, merge_base_sha: str, run_id: str, run_attempt: int, verdict: dict) -> dict:
    return {"schema_version": SCHEMA_VERSION, "kind": "lh-pr-quality", "pr_number": pr_number, "head_sha": head_sha, "base_sha": base_sha,
            "merge_base_sha": merge_base_sha, "run_id": str(run_id), "run_attempt": run_attempt,
            "files": {QUALITY_FILE: sha256_file(os.path.join(out_dir, QUALITY_FILE)), REPORT_FILE: sha256_file(os.path.join(out_dir, REPORT_FILE))},
            "verdict": {"failed": bool(verdict["failed"]), "gates": len(verdict["gates"])}}


def verify_bundle(directory: str) -> list[str]:
    """Problems of a downloaded `pr-quality` artifact directory (empty list: usable). The consumer calls this before it reads anything else."""
    errs: list[str] = []
    names = set(os.listdir(directory))
    if names != {QUALITY_FILE, REPORT_FILE, MANIFEST_FILE}:
        return [f"the artifact holds {sorted(names)}, expected exactly {sorted([QUALITY_FILE, REPORT_FILE, MANIFEST_FILE])}"]
    for n in names:
        p = os.path.join(directory, n)
        if os.path.islink(p) or not os.path.isfile(p):
            return [f"{n} is not a regular file"]
        if os.path.getsize(p) > MAX_FILE_BYTES:
            return [f"{n} is larger than {MAX_FILE_BYTES} bytes"]
    try:
        with open(os.path.join(directory, MANIFEST_FILE), encoding="utf-8") as f:
            manifest = json.load(f)
        with open(os.path.join(directory, QUALITY_FILE), encoding="utf-8") as f:
            quality = json.load(f)
    except (OSError, ValueError) as e:
        return [f"unreadable JSON: {e}"]
    errs += [f"manifest {e}" for e in validate_manifest(manifest)] + [f"quality {e}" for e in validate_quality(quality)]
    if errs:
        return errs
    for n in (QUALITY_FILE, REPORT_FILE):
        got = sha256_file(os.path.join(directory, n))
        if got != manifest["files"][n]:
            errs.append(f"{n} does not match its manifest entry")
    if manifest["verdict"] != {"failed": quality["verdict"]["failed"], "gates": len(quality["verdict"]["gates"])}:
        errs.append("the manifest verdict differs from quality.json")
    return errs


# ------------------------------------------------------------------------------------------------ Markdown safety
_CTRL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f  ‪-‮⁦-⁩]")


def _clean(s, limit: int) -> str:
    s = _CTRL.sub("", str(s)).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return s if len(s) <= limit else s[: limit - 1] + "…"


def md_safe(s, limit: int = 200) -> str:
    """Untrusted text as inert Markdown prose: no mention, no link, no autolink, no issue reference, no HTML, no table break."""
    out = []
    for ch in _clean(s, limit):
        if ch in "<>&@#|[]!`*_~\\":
            out.append(f"&#{ord(ch)};")
        else:
            out.append(ch)
    return re.sub(r"(?i)www\.", "www&#46;", "".join(out).replace("://", ":&#47;&#47;"))


def md_code(s, limit: int = 200) -> str:
    """Untrusted text in a code span: backticks and table pipes cannot break out, nothing in it renders."""
    t = _clean(s, limit).replace("`", "'").replace("|", "\\|")
    return f"`{t}`" if t else "``"


def cap_body(text: str, cap: int = BODY_CAP) -> str:
    """The comment body within `cap` characters: cut at a line end, with a note, the marker line (first) kept."""
    if len(text) <= cap:
        return text
    note = "\n\n_The report was cut to fit the size limit of a PR comment; the full data is in the `pr-quality` artifact of the run._\n"
    cut = text[: cap - len(note)]
    cut = cut[: cut.rfind("\n")] if "\n" in cut else cut
    return cut + note
