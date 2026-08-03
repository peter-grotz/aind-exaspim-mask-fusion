#!/usr/bin/env python3
"""Emit the brain-mask fusion process record.

Writes a v2 DataProcess document to /results/mask_fusion only. The upload capsule
merges it into the root processing.json (it mounts this capsule's /results at
../data/mask_fusion -- MASK_META_DIR -- a separate mount from the CCF fusion
record to avoid a Nextflow input-name collision).

Importable: call emit(start, status, input_xml) in-process. Also runnable:
    python emit_mask_fusion_record.py [START_ISO] [STATUS] [INPUT_XML_REL]
"""
import os
import sys
from datetime import datetime, timezone

from aind_process_record import make_data_process, write_data_process


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rhapso_version():
    """The installed Rhapso version, so the record cannot drift from the environment."""
    try:
        from importlib.metadata import version
        return version("Rhapso")
    except Exception:
        return os.environ.get("RHAPSO_VERSION", "unknown")


def emit(start=None, status="SUCCESS", input_xml=None, config=None):
    start = start or _now()
    end = _now()
    config = config or {}

    parameters = {
        "engine": "Rhapso",
        "rhapso_version": _rhapso_version(),
        # Taken from the run's resolved path and its actual config, not restated here --
        # a hardcoded overlap_strategy previously reported lowest_view_wins while the
        # config said max_blend, hiding the setting that was failing every fusion task.
        "input_xml": input_xml or "unresolved",
        "input_tiles": "flatfield_correction/mask/SPIM.ome.zarr",
        "output_zarr_version": config.get("output_zarr_version", "unknown"),
        "overlap_strategy": config.get("overlap_strategy", "unknown"),
        "mask_fusion_status": status,
    }

    dp = make_data_process(
        process_type="Image tile fusing",
        name="Brain mask fusion",
        start=start,
        end=end,
        code_url="https://codeocean.allenneuraldynamics.org/capsule/1213439/tree",
        code_name="aind-exaspim-mask-fusion",
        code_version=os.environ.get("CODE_VERSION", "0.0.0"),
        run_script="/code/run",
        language="Python",
        experimenters=["Peter Grotz"],
        parameters=parameters,
        output_path="fusion/fused_mask_ch.zarr",
        notes=("Fuses the flat-field brain mask (Zarr v3 tiles) onto the CCF-channel grid "
               "with Rhapso; writes Zarr v2. Best-effort registration aid: on failure the "
               "partial output is removed and registration runs unmasked."),
    )

    local = write_data_process(dp, "/results/mask_fusion")
    print(f"wrote {local} (results-only; not published to S3)")
    return local


def main():
    start = sys.argv[1] if len(sys.argv) > 1 else None
    status = sys.argv[2] if len(sys.argv) > 2 else "SUCCESS"
    input_xml = sys.argv[3] if len(sys.argv) > 3 else None
    emit(start, status, input_xml)


if __name__ == "__main__":
    main()
