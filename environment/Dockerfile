FROM registry.codeocean.allenneuraldynamics.org/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive

# AWS CLI v2 -- removes a partial output on mask-fusion failure and sanity-checks
# the fused output level.
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip \
  && rm -rf /var/lib/apt/lists/* \
  && curl -sS "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip \
  && unzip -q awscliv2.zip && ./aws/install && rm -rf aws awscliv2.zip

# Rhapso 0.3.9 requires Python >=3.11, but this base image ships 3.10 -- upgrade
# the base conda env before installing. (If the Code Ocean registry offers a
# python3.11 mambaforge tag, using it directly is cleaner than this in-place bump.)
RUN mamba install -y "python=3.11" && mamba clean -ya

# Rhapso 0.3.9 pins its own ray (2.9.1), zarr (>=3), s3fs, scipy, and dask.
# Do NOT pin a different ray/zarr alongside it -- let Rhapso resolve them.
RUN python -m pip install -U --no-cache-dir \
    "Rhapso==0.3.9" \
    boto3 \
    PyYAML
