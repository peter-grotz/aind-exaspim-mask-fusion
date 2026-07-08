FROM registry.codeocean.allenneuraldynamics.org/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive

# AWS credential build ARGs. Code Ocean requires these in a custom Dockerfile
# before it will attach the AWS role/secret. Matches the other exaSPIM capsules.
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

# Rhapso 0.4.1 needs EXACTLY Python 3.11 (requires >=3.11; its pinned ray 2.9.1 has
# no 3.12 wheel). Create a dedicated 3.11 env with the base conda -- do NOT upgrade
# python in the base env in place, which breaks the base conda/mamba tooling. Put the
# env first on PATH so `python`/`pip` resolve to it at build and run time.
RUN conda create -y -n rhapso python=3.11 && conda clean -ya
ENV PATH=/opt/conda/envs/rhapso/bin:$PATH

# Rhapso 0.4.1 pins its own ray (2.9.1), zarr (>=3), s3fs, scipy, and dask.
# Do NOT pin a different ray/zarr alongside it. S3 I/O uses boto3 (no AWS CLI).
# 0.4.1 fixes 0.3.9's bug where the append-mode refactor dropped zarr_format on
# group creation, so the output was silently written as v3 regardless of
# output_zarr_version -- now output_zarr_version: 2 actually yields v2.
RUN python -m pip install -U --no-cache-dir \
    "Rhapso==0.4.1" \
    boto3 \
    PyYAML
