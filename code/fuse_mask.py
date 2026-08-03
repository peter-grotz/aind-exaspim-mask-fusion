#!/usr/bin/env python3
"""Fuse the flat-field brain mask with Rhapso on a local Ray runtime.

Reads the cp_jsons manifest to locate the CCF split-affine alignment XML and the
mask tiles, fuses the mask onto the CCF-channel grid with Rhapso AffineFusion +
MultiScale, and writes fusion/fused_mask_ch.zarr (Zarr v2). The mask tiles are
Zarr v3 (which Rhapso reads); the output is v2 (which registration reads).
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
from botocore.exceptions import ClientError

# Retry transient S3 errors up to 100x so a blip on a tile-metadata read isn't
# seen as GroupNotFound and dropped as an empty block (hole). Also set in code/run;
# set here before any boto3/s3fs client is created and before ray.init() so the
# local Ray workers inherit it too.
os.environ.setdefault("AWS_MAX_ATTEMPTS", "100")

CONFIG_PATH = "/code/config/fusion_params.yml"

# The CCF split-affine XML moved when tile alignment switched from BigStitcher to
# Rhapso: assets processed through ~2026-07 carry ch_ccf_xmls/, later ones rhapso/.
# Exactly one of the two exists per asset. Legacy path first, so a previously-fused
# sample re-fuses from the same XML it was fused with.
CCF_XML_RELS = (
    "tile_alignment/ch_ccf_xmls/bigstitcher_split_affine_ch_ccf.xml",
    "tile_alignment/rhapso/rhapso-solver-split-affine-ccf.xml",
)
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


def s3_exists(uri: str) -> bool:
    """True if the key exists. Only a definitive not-found answers False -- a 403 or
    a throttle is re-raised rather than reported as absent, so a transient blip can't
    be misread as 'this layout isn't here' and silently select the wrong XML."""
    bucket, key = _split_s3(uri)
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def resolve_ccf_xml(in_base: str) -> str:
    """First CCF_XML_RELS candidate that exists under the input asset."""
    for rel in CCF_XML_RELS:
        uri = f"{in_base}/{rel}"
        if s3_exists(uri):
            print(f"ccf xml     : resolved to {rel}")
            return uri
        print(f"ccf xml     : not at {rel}")
    raise RuntimeError(
        f"no CCF split-affine XML under {in_base}/tile_alignment/ "
        f"(checked: {', '.join(CCF_XML_RELS)})")


def voxel_size_zyx(xml_bytes: bytes) -> list[float]:
    root = ET.fromstring(xml_bytes)
    el = root.find(".//ViewSetup/voxelSize/size")
    if el is None or not el.text:
        raise ValueError("ViewSetup/voxelSize/size not found in XML")
    xyz = [float(x) for x in el.text.strip().split()]
    return xyz[::-1]  # ZYX for MultiScale


def s3_prefix_exists(uri: str) -> bool:
    """True if at least one object exists under `uri` as a prefix (zarr groups are
    prefixes, not keys, so head_object cannot answer this)."""
    bucket, prefix = _split_s3(f"{uri.rstrip('/')}/")
    r = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return r.get("KeyCount", 0) > 0


def zgroup_tiles(root) -> dict:
    """base setup id -> tile path. BigStitcher writes the path as a zgroup attribute,
    Rhapso as a child element; handle both."""
    tiles = {}
    for zg in root.iter("zgroup"):
        path = zg.get("path") or zg.findtext("path")
        if path and path.strip():
            tiles[int(zg.get("setup"))] = path.strip()
    return tiles


def drop_absent_tiles(xml_bytes: bytes, mask_prefix: str) -> tuple[bytes, list, dict]:
    """Remove views whose mask tile is absent, returning (xml, dropped, bbox_delta).

    The flat-field step omits a mask tile when it finds no brain in it -- normally the
    corner tiles -- so the CCF XML references tiles that do not exist under the mask
    prefix. Rhapso indexes its path map directly, so those views raise GroupNotFound
    mid-fusion. Drop them instead.

    Rhapso derives the output bounding box from the same ViewRegistration loop it
    derives tile paths from, so dropping a view also drops its bbox contribution.
    bbox_delta reports that shift so the caller can log it and verify the result.
    """
    root = ET.fromstring(xml_bytes)
    tiles = zgroup_tiles(root)
    absent = {sid: rel for sid, rel in tiles.items()
              if not s3_prefix_exists(f"{mask_prefix}/{rel}")}
    if not absent:
        return xml_bytes, [], {}
    if len(absent) == len(tiles):
        raise RuntimeError(
            f"no mask tiles present under {mask_prefix} "
            f"({len(tiles)} referenced by the XML) -- the mask was never generated")

    before = _bbox(root)
    # Split XMLs map each split view (NewId) back to a base tile (OldId).
    setup_ids = root.find("./SequenceDescription/ImageLoader/SetupIds")
    doomed, defs = set(), []
    if setup_ids is not None:
        for d in setup_ids.findall("./SetupIdDefinition"):
            if int(d.findtext("./OldId")) in absent:
                doomed.add(int(d.findtext("./NewId")))
                defs.append(d)
        for d in defs:
            setup_ids.remove(d)
    else:
        doomed = set(absent)

    vs = root.find("./SequenceDescription/ViewSetups")
    for v in [v for v in vs.findall("./ViewSetup") if int(v.findtext("./id")) in doomed]:
        vs.remove(v)
    vr = root.find("./ViewRegistrations")
    for r in [r for r in vr.findall("./ViewRegistration") if int(r.get("setup")) in doomed]:
        vr.remove(r)

    after = _bbox(root)
    delta = {"origin_shift": (after[0] - before[0]).tolist(),
             "dims_change": ((after[1] - after[0]) - (before[1] - before[0])).tolist()}
    return ET.tostring(root), sorted(absent.values()), delta


def _bbox(root):
    """Output bbox min/max the way Rhapso's ComputeBBox derives it."""
    import numpy as np
    sizes = {int(v.findtext("./id")): [int(x) for x in v.findtext("./size").split()]
             for v in root.findall("./SequenceDescription/ViewSetups/ViewSetup")}
    lo = np.full(3, np.inf)
    hi = np.full(3, -np.inf)
    for r in root.findall("./ViewRegistrations/ViewRegistration"):
        T = np.eye(4)
        for vt in r.findall("./ViewTransform"):
            M = np.eye(4)
            M[:3, :] = np.array([float(x) for x in vt.findtext("./affine").split()]).reshape(3, 4)
            T = T @ M
        sx, sy, sz = sizes[int(r.get("setup"))]
        C = np.array([[x, y, z, 1.0] for x in (0, sx - 1) for y in (0, sy - 1) for z in (0, sz - 1)]).T
        w = (T @ C)[:3]
        lo = np.minimum(lo, w.min(axis=1))
        hi = np.maximum(hi, w.max(axis=1))
    return np.floor(lo), np.ceil(hi)


def _level3_has_chunks(mask_out: str) -> bool:
    """True if level 3 holds at least one real chunk object (not just metadata)."""
    bucket, prefix = _split_s3(f"{mask_out.rstrip('/')}/3/")
    s3 = boto3.client("s3")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if not name.startswith(".") and name != "zarr.json":
                return True
    return False


def _zarray(level_uri: str) -> dict | None:
    """Parse a Zarr v2 .zarray at `level_uri`, or None when it isn't v2/absent."""
    try:
        return json.loads(s3_read(f"{level_uri.rstrip('/')}/.zarray"))
    except Exception:
        return None


def verify_level3(mask_out: str, ccf_fused: str | None = None) -> None:
    """Gate the fused mask on what registration actually needs from level 3.

    Three checks, because "the fusion ran" is not the same as "the mask is usable":

    1. Level 3 holds real chunk data -- catches an empty fusion at the source.
    2. Level 3 is Zarr v2. Registration pins zarr==2.16.1 and cannot open v3, and
       MultiScale does not take output_zarr_version, so the pyramid's format is not
       guaranteed to match AffineFusion's. A v3 level 3 reads to registration as a
       missing mask.
    3. Level 3's shape matches fused_ccf_ch.zarr level 3. This is the grid-parity
       gate: the mask is multiplied into the sample, so a mismatched grid misaligns
       silently. Registration squeezes two leading axes off, so a mask that is not
       5-D also reads as missing. Skipped with a warning if the CCF reference cannot
       be read -- an unreadable reference is no reason to discard a good mask.
    """
    if not _level3_has_chunks(mask_out):
        bucket, prefix = _split_s3(f"{mask_out.rstrip('/')}/3/")
        raise RuntimeError(f"fused mask level-3 has no chunk data under s3://{bucket}/{prefix} "
                           f"-- the fusion produced an empty mask")

    mask_meta = _zarray(f"{mask_out}/3")
    if mask_meta is None:
        raise RuntimeError(
            f"fused mask level-3 has no Zarr v2 .zarray at {mask_out}/3 -- it was most "
            f"likely written as v3, which registration (zarr==2.16.1) cannot open")

    if not ccf_fused:
        print("verify: no CCF reference given; skipping grid-parity check")
        return
    ccf_meta = _zarray(f"{ccf_fused}/3")
    if ccf_meta is None:
        print(f"WARNING: could not read {ccf_fused}/3/.zarray; skipping grid-parity check",
              file=sys.stderr)
        return
    if list(mask_meta.get("shape", [])) != list(ccf_meta.get("shape", [])):
        raise RuntimeError(
            f"fused mask level-3 shape {mask_meta.get('shape')} != fused_ccf_ch level-3 "
            f"shape {ccf_meta.get('shape')} -- the mask is on a different grid than the "
            f"sample it multiplies into")
    print(f"verify: level-3 grid matches fused_ccf_ch {ccf_meta.get('shape')} (Zarr v2)")




def emit_record(start: str, status: str, input_xml: str | None = None,
                config: dict | None = None) -> None:
    """Emit the process record in-process."""
    try:
        import emit_mask_fusion_record
        emit_mask_fusion_record.emit(start, status, input_xml, config)
    except Exception as e:
        print(f"WARNING: could not emit mask fusion record ({type(e).__name__}: {e})",
              file=sys.stderr)


def main() -> int:
    start = _now()
    status = "SUCCESS"
    mask_out = None
    ccf_xml_rel = None
    cfg = {}
    try:
        cfg = yaml.safe_load(Path(CONFIG_PATH).read_text())
        input_uri = read_manifest_input_uri()      # s3://.../<asset>/fusion/fused_ccf_ch.zarr/
        in_base = input_uri.split("/fusion/")[0]    # s3://.../<asset>  (read from aind-open-data)

        # Write to the input asset, or under OUTPUT_PREFIX/<asset> for scratch testing.
        scratch = os.environ.get("OUTPUT_PREFIX", "").rstrip("/")
        out_base = f"{scratch}/{in_base.rstrip('/').split('/')[-1]}" if scratch else in_base

        mask_prefix = f"{in_base}/{MASK_TILES_REL}"
        mask_out = f"{out_base}/fusion/fused_mask_ch.zarr"

        print(f"input asset : {in_base}")
        print(f"output base : {out_base}")
        ccf_xml = resolve_ccf_xml(in_base)
        ccf_xml_rel = ccf_xml[len(in_base) + 1:]   # recorded in the process metadata
        print(f"mask tiles  : {mask_prefix}")
        print(f"mask output : {mask_out}")

        xml_bytes = s3_read(ccf_xml)
        vsz = voxel_size_zyx(xml_bytes)

        # Drop views whose mask tile the flat-field step omitted. Runs before anything
        # destructive, and raises if no tiles are present at all.
        filtered, dropped, delta = drop_absent_tiles(xml_bytes, mask_prefix)
        aligned_xml = ccf_xml
        if dropped:
            out = Path("/results/mask_fusion")
            out.mkdir(parents=True, exist_ok=True)
            aligned_xml = str(out / "ccf_split_affine_filtered.xml")
            Path(aligned_xml).write_bytes(filtered)
            print(f"mask tiles  : {len(dropped)} absent, views dropped: {dropped}")
            print(f"mask tiles  : bbox origin shift {delta['origin_shift']}, "
                  f"dims change {delta['dims_change']} (level-0 voxels)")
        else:
            print("mask tiles  : all referenced tiles present")

        # Imported here so an import error is caught by the graceful-degradation path.
        from Rhapso.pipelines.ray.affine_fusion import AffineFusion
        from Rhapso.pipelines.ray.multiscale import MultiScale
        import ray

        # No pre-clean: the capsule's role has PutObject but not DeleteObject on
        # aind-open-data (verified -- s3:DeleteObject, the bulk API and
        # s3:DeleteObjectVersion all return AccessDenied). Every run therefore writes
        # over whatever is already at mask_out and is treated as a fresh run.
        # verify_level3 is the gate that catches a result the leftovers made unusable.

        # Rhapso manages its own local Ray runtime (bare ray.init() -> all cores).
        AffineFusion(
            aligned_xml_path=aligned_xml,      # transforms + tile sizes (same as CCF)
            zarr_input_prefix=mask_prefix,     # read the v3 mask tiles instead of the signal
            output_path=mask_out,
            block_size=cfg["block_size"],
            output_block_size=cfg["output_block_size"],
            intensity_range=cfg["intensity_range"],
            overlap_strategy=cfg["overlap_strategy"],
            output_zarr_version=cfg["output_zarr_version"],
            compressor_cname=cfg["compressor_cname"],
            compressor_clevel=cfg["compressor_clevel"],
            compressor_shuffle=cfg["compressor_shuffle"],
        ).run()

        # AffineFusion leaves a local Ray runtime up; free its workers before
        # MultiScale (which uses dask) so the two don't hold memory simultaneously.
        ray.shutdown()

        # MultiScale takes no output_zarr_version in any 0.4.x release -- it reads the
        # format off the store AffineFusion just wrote and matches it. verify_level3
        # asserts level 3 really came out v2.
        MultiScale(
            zarr_path=mask_out,
            chunk_size=cfg["multiscale_chunk_size"],
            voxel_size=vsz,
            n_lvls=cfg["n_lvls"],
            scale_factor=cfg["scale_factor"],
            base_level=cfg["base_level"],
            compressor_cname=cfg["compressor_cname"],
            compressor_clevel=cfg["compressor_clevel"],
            compressor_shuffle=cfg["compressor_shuffle"],
        ).run()

        verify_level3(mask_out, ccf_fused=f"{in_base}/fusion/fused_ccf_ch.zarr")
        print("mask fusion + multiscale complete")

    except Exception as e:  # best-effort leaf: never fail the pipeline on the mask
        status = "FAILED"
        print(f"WARNING: mask fusion failed ({type(e).__name__}: {e}); continuing unmasked.",
              file=sys.stderr)
        # No cleanup: deletes are denied on this bucket. Whatever was written stays, and
        # registration falls back to unmasked when level 3 is unreadable or below its
        # foreground threshold.
        if mask_out:
            print(f"  partial output left at {mask_out}", file=sys.stderr)

    emit_record(start, status, ccf_xml_rel, cfg)
    print(f"mask fusion status: {status}")
    return 0  # leaf node: a mask failure must not fail the pipeline


if __name__ == "__main__":
    sys.exit(main())
