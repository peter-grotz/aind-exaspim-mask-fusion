#!/usr/bin/env python3
"""Emit the brain-mask fusion process record.

Writes a v2 DataProcess document to /results/mask_fusion only. The upload capsule
merges it into the root processing.json (it mounts this capsule's /results at
../data/mask_fusion -- MASK_META_DIR -- a separate mount from the CCF fusion
record to avoid a Nextflow input-name collision).

Importable: call emit(start, status) in-process. Also runnable:
    python emit_mask_fusion_record.py [START_ISO] [STATUS]
"""
import os
import sys
from datetime import datetime, timezone

from aind_process_record import make_data_process, write_data_process


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def emit(start=None, status="SUCCESS"):
    start = start or _now()
    end = _now()

    parameters = {
        "engine": "Rhapso",
        "rhapso_version": os.environ.get("RHAPSO_VERSION", "0.3.9"),
        "input_xml": "tile_alignment/ch_ccf_xmls/bigstitcher_split_affine_ch_ccf.xml",
        "input_tiles": "flatfield_correction/mask/SPIM.ome.zarr",
        "output_zarr_version": 2,
        "overlap_strategy": "lowest_view_wins",
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
    emit(start, status)


if __name__ == "__main__":
    main()
