# aind-exaspim-mask-fusion

Fuses the exaSPIM **flat-field brain mask** into `fusion/fused_mask_ch.zarr` with
**Rhapso** (Python, local Ray). It exists because recent masks are written in **Zarr v3**,
which the BigStitcher-Spark path can't read; Rhapso reads v3 tiles and writes v2 output.

Runs as its own pipeline node **after** the BigStitcher CCF-channel fusion and **before**
registration. The mask is a best-effort registration aid: on any failure the partial
output is removed and the run exits 0, so registration simply proceeds unmasked.

## What it does
1. Reads the cp_jsons manifest (`../data/*manifest*.json`) → `input_uri` → input asset base.
2. Fuses the mask tiles onto the **CCF-channel grid** with Rhapso:
   - `AffineFusion(aligned_xml_path=<CCF split-affine XML>, zarr_input_prefix=<mask tiles>,
     output_zarr_version=2, overlap_strategy="lowest_view_wins", …)`
   - `MultiScale(...)` for the pyramid (level 3 = 8× is what registration reads).
   No XML editing is needed: Rhapso reads tiles from `zarr_input_prefix` + the XML's relative
   tile paths, so pointing the prefix at the mask tiles reuses the CCF transforms directly.
3. Emits `mask_fusion_data_process.json` to `/results/mask_fusion` for the upload capsule.

## Verified before build
- **Grid parity:** Rhapso's `ComputeBBox` on the CCF XML gives `(X,Y,Z)=[8244,4974,2378]`,
  **exactly** BigStitcher's `fused_ccf_ch` (823508) → the mask lands on the same grid.
- **Repoint validity:** mask tiles (`ch_488`) match the CCF tiles' dims `[2304,1330,1774]`;
  only the format (v3/uint8 vs v2/uint16) differs, which Rhapso handles.

## Files
- `code/fuse_mask.py` — driver (manifest → local Rhapso fusion → graceful cleanup → metadata).
- `code/emit_mask_fusion_record.py`, `code/aind_process_record.py` — process metadata.
- `code/config/fusion_params.yml` — Rhapso params (`output_zarr_version: 2`, level-3 = 8×).

## Environment (custom Dockerfile — editor disabled, as with the other capsules)
Rhapso 0.3.9 needs **exactly Python 3.11**: it requires ≥3.11, and its pinned `ray==2.9.1` has no
3.12 wheel. Code Ocean's GUI base images don't offer 3.11, so a custom `environment/Dockerfile` is
required (the GUI editor stays disabled). It:
- bases on the 3.10 image + `mamba install python=3.11`,
- `pip install Rhapso==0.3.9 boto3 PyYAML` (Rhapso pulls ray 2.9.1, zarr>=3, s3fs, scipy, dask),
- declares `ARG AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_DEFAULT_REGION` so CO can attach the AWS
  role to this custom-Dockerfile capsule (without these lines CO blocks credential attachment).
S3 I/O uses boto3 — no AWS CLI.

## Compute / resources (LOCAL Ray — runs on this instance)
Unlike the BigStitcher path (which offloaded to EMR-Serverless), this capsule fuses **on the
Code Ocean instance itself**. Sizing driver = the fused dims (= `fused_ccf_ch`); for 823508
that's `[2378,4974,8244]` uint16, ~1,000 grid blocks (mostly empty background → skipped).
Peak RAM ≈ CPUs × ~1 GB. **Recommended: ~16–32 vCPU / 64–128 GB.** Confirm on the largest
sample during the GATE run; fall back to a Ray-on-EC2 cluster only if a sample doesn't fit.

## To set in Code Ocean (not in git)
- **AWS role** with read/write to the input asset on `aind-open-data` (S3 + reading v3 tiles).
- **`resources.json`** to a large instance (see above) — local Ray uses all its CPUs.
- Set `code_url` in `emit_mask_fusion_record.py` to this capsule's real CO URL.

## GATE test (run before wiring into the pipeline)
1. Set `OUTPUT_PREFIX=s3://aind-scratch-data/exaspim_processing_test` and run on a **v3-mask**
   sample (823508): confirm it reads v3 tiles and writes `fused_mask_ch.zarr` (v2), exit 0.
2. Assert `fused_mask_ch.zarr/3` shape == `fused_ccf_ch.zarr/3` (`[297,621,1030]` for 823508).
3. Record peak RAM + wall-clock to pick the instance tier.
