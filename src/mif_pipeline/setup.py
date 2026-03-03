from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import guess_alias_from_path, guess_nimbus_name_from_path


def generate_channel_map(slide_cfg: dict[str, Any]) -> dict[str, Any]:
    setup_cfg = slide_cfg.get("setup", {})
    source_dir = Path(setup_cfg.get("channel_source_dir", slide_cfg.get("slide_dir", ".")))
    patterns = setup_cfg.get("channel_patterns", ["*.tif", "*.tiff", "*.ome.tif", "*.ome.tiff"])
    output_json = Path(setup_cfg.get("channel_map_output", Path(slide_cfg.get("slide_dir", ".")) / "channel_map.generated.json"))

    files: list[Path] = []
    for pat in patterns:
        files.extend(sorted(source_dir.glob(pat)))

    dedup: list[Path] = []
    seen: set[str] = set()
    for f in files:
        s = str(f)
        if s not in seen:
            seen.add(s)
            dedup.append(f)

    channel_map = [{"alias": guess_alias_from_path(str(f)), "path": str(f), "nimbus_name": guess_nimbus_name_from_path(str(f))} for f in dedup]
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(channel_map, indent=2), encoding="utf-8")
    return {"channel_map_file": str(output_json), "count": len(channel_map), "source_dir": str(source_dir)}
