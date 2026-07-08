#!/usr/bin/env python3
"""Fuse the flat-field brain mask with Rhapso on a local Ray runtime.

Reads the cp_jsons manifest to locate the CCF split-affine alignment XML and the
mask tiles, fuses the mask onto the CCF-channel grid with Rhapso AffineFusion +
MultiScale, and writes fusion/fused_mask_ch.zarr (Zarr v2). The mask tiles are
Zarr v3 (which Rhapso reads); the output is v2 (which registration reads).

Rhapso reads tiles from `zarr_input_prefix` + each tile's relative path in the
XML, so pointing the prefix at the mask tiles fuses the mask with the CCF
channel's transforms -- no XML editing needed.

Best-effort leaf: ANY failure (including a missing manifest) removes the partial
output and exits 0 so the pipeline continues (registration runs unmasked).

S3 I/O uses boto3 (already a Rhapso dependency) -- no AWS CLI needed, which keeps
the environment expressible through the Code Ocean GUI editor.
"""
from __future__ import annotations

import glob
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import yaml

CONFIG_PATH = "/code/config/fusion_params.yml"
CCF_XML_REL = "tile_alignment/ch_ccf_xmls/bigstitcher_split_affine_ch_ccf.xml"
MASK_TILES_REL = "flatfield_correction/mask/SPIM.ome.zarr"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _split_s3(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def read_manifest_input_uri() -> str:
    paths = [p for p in glob.glob("../data/*.json") if "manifest" in os.path.basename(p).lower()]
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one manifest json in ../data, found: {paths}")
    cfg = json.loads(Path(paths[0]).read_text())
    return str(cfg["zarr_multiscale"]["input_uri"])


def s3_read(uri: str) -> bytes:
    bucket, key = _split_s3(uri)
    return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()


def voxel_size_zyx(xml_bytes: bytes) -> list[float]:
    root = ET.fromstring(xml_bytes)
    el = root.find(".//ViewSetup/voxelSize/size")
    if el is None or not el.text:
        raise ValueError("ViewSetup/voxelSize/size not found in XML")
    xyz = [float(x) for x in el.text.strip().split()]
    return xyz[::-1]  # ZYX for MultiScale


def verify_level3(mask_out: str) -> None:
    """Confirm the level registration reads (3) holds chunk data, so an empty fusion
    is caught at the source rather than silently ignored downstream. (A degenerate-
    but-present mask is still caught by registration's foreground gate.)"""
    bucket, prefix = _split_s3(f"{mask_out.rstrip('/')}/3/")
    s3 = boto3.client("s3")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if not name.startswith(".") and name != "zarr.json":
                return  # a real chunk exists
    raise RuntimeError(f"fused mask level-3 has no chunk data under s3://{bucket}/{prefix} "
                       f"-- the fusion produced an empty mask")


def s3_rm_recursive(uri: str) -> None:
    bucket, prefix = _split_s3(f"{uri.rstrip('/')}/")
    s3 = boto3.client("s3")
    batch: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def emit_record(start: str, status: str) -> None:
    """Emit the process record in-process."""
    try:
        import emit_mask_fusion_record
        emit_mask_fusion_record.emit(start, status)
    except Exception as e:
        print(f"WARNING: could not emit mask fusion record ({type(e).__name__}: {e})",
              file=sys.stderr)


def main() -> int:
    start = _now()
    status = "SUCCESS"
    mask_out = None
    try:
        cfg = yaml.safe_load(Path(CONFIG_PATH).read_text())
        input_uri = read_manifest_input_uri()      # s3://.../<asset>/fusion/fused_ccf_ch.zarr/
        in_base = input_uri.split("/fusion/")[0]    # s3://.../<asset>  (read from aind-open-data)

        # Write to the input asset, or under OUTPUT_PREFIX/<asset> for scratch testing.
        scratch = os.environ.get("OUTPUT_PREFIX", "").rstrip("/")
        out_base = f"{scratch}/{in_base.rstrip('/').split('/')[-1]}" if scratch else in_base

        ccf_xml = f"{in_base}/{CCF_XML_REL}"
        mask_prefix = f"{in_base}/{MASK_TILES_REL}"
        mask_out = f"{out_base}/fusion/fused_mask_ch.zarr"

        print(f"input asset : {in_base}")
        print(f"output base : {out_base}")
        print(f"ccf xml     : {ccf_xml}")
        print(f"mask tiles  : {mask_prefix}")
        print(f"mask output : {mask_out}")

        vsz = voxel_size_zyx(s3_read(ccf_xml))

        # Start from a clean output. Rhapso >=0.3.9 opens the output group in append
        # mode and (as of 0.4.1) refuses to append when an existing store's Zarr
        # format differs from output_zarr_version -- so a stale v3 partial from an
        # earlier run would abort a v2 run. Removing it first also avoids resuming
        # onto a half-written pyramid. The mask is regenerated in full every run.
        print(f"clearing any existing output at {mask_out}")
        s3_rm_recursive(mask_out)

        # Imported here so an import error is caught by the graceful-degradation path.
        from Rhapso.pipelines.ray.affine_fusion import AffineFusion
        from Rhapso.pipelines.ray.multiscale import MultiScale
        import ray

        # Rhapso manages its own local Ray runtime (bare ray.init() -> all cores).
        # Size the instance for RAM: each parallel fuse worker holds a per-task read
        # (the v3 mask tiles are sharded at 512^3, so a read materializes whole shards),
        # so peak RAM ~= n_cores * per-task read. Run on a high-memory instance, NOT a
        # small one -- an undersized box (e.g. 8 GB) OOMs regardless of block size.
        AffineFusion(
            aligned_xml_path=ccf_xml,          # transforms + tile sizes (same as CCF)
            zarr_input_prefix=mask_prefix,     # read the v3 mask tiles instead of the signal
            output_path=mask_out,
            block_size=cfg["block_size"],
            intensity_range=cfg["intensity_range"],
            block_scale=cfg["block_scale"],
            overlap_strategy=cfg["overlap_strategy"],
            output_zarr_version=cfg["output_zarr_version"],
        ).run()

        # AffineFusion leaves a local Ray runtime up; free its workers before
        # MultiScale (which uses dask) so the two don't hold memory simultaneously.
        ray.shutdown()

        MultiScale(
            zarr_path=mask_out,
            chunk_size=cfg["multiscale_chunk_size"],
            voxel_size=vsz,
            n_lvls=cfg["n_lvls"],
            scale_factor=cfg["scale_factor"],
            target_block_size_mb=cfg["target_block_size_mb"],
            base_level=cfg["base_level"],
        ).run()

        verify_level3(mask_out)
        print("mask fusion + multiscale complete")

    except Exception as e:  # best-effort leaf: never fail the pipeline on the mask
        status = "FAILED"
        print(f"WARNING: mask fusion failed ({type(e).__name__}: {e}); removing partial "
              f"output and continuing unmasked.", file=sys.stderr)
        if mask_out:
            try:
                s3_rm_recursive(mask_out)
            except Exception as ce:
                print(f"  (partial-output cleanup failed: {ce})", file=sys.stderr)

    emit_record(start, status)
    print(f"mask fusion status: {status}")
    return 0  # leaf node: a mask failure must not fail the pipeline


if __name__ == "__main__":
    sys.exit(main())
