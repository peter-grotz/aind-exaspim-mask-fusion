FROM registry.codeocean.allenneuraldynamics.org/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive

# AWS credential build ARGs. Code Ocean requires these to be present in a custom
# Dockerfile before it will attach the AWS role/secret (it can only auto-insert
# them into a GUI-managed Dockerfile). Matches the other exaSPIM capsules.
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

# Rhapso 0.3.9 needs EXACTLY Python 3.11: it requires >=3.11, and its pinned
# ray 2.9.1 has no 3.12 wheel. The base image ships 3.10, so upgrade in place.
RUN mamba install -y "python=3.11" && mamba clean -ya

# Rhapso 0.3.9 pins its own ray (2.9.1), zarr (>=3), s3fs, scipy, and dask.
# Do NOT pin a different ray/zarr alongside it. S3 I/O uses boto3 (no AWS CLI).
RUN python -m pip install -U --no-cache-dir \
    "Rhapso==0.3.9" \
    boto3 \
    PyYAML
