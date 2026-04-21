# Dropbox to AWS Glacier Transfer

This repository includes `dropbox_to_glacier.py`, a resumable migration script for transferring large Dropbox datasets (including multi-TB migrations) to Amazon S3 using Glacier storage classes.

## Features

- Streams file data from Dropbox directly to S3 (no full-file local staging).
- Stores progress in a local SQLite checkpoint database so interrupted runs can resume.
- Retries transient Dropbox/AWS failures automatically.
- Preserves source metadata (`dropbox-path`, `dropbox-rev`, `dropbox-size`) on S3 objects.

## Requirements

- Python 3.10+
- AWS credentials configured in environment/instance profile with permission to write to the destination bucket
- Dropbox API token with access to the source files

Install dependencies:

```bash
python -m pip install boto3 dropbox
```

## Example

```bash
python dropbox_to_glacier.py \
  --dropbox-token "$DROPBOX_TOKEN" \
  --dropbox-root "" \
  --bucket my-archive-bucket \
  --prefix dropbox-archive \
  --storage-class DEEP_ARCHIVE \
  --region us-east-1 \
  --checkpoint-db ./checkpoint.db \
  --multipart-chunk-mb 64 \
  --max-concurrency 4 \
  --retries 8
```

## Operational notes for ~30TB transfers

- Run from a stable compute environment (EC2 preferred) with persistent disk for `checkpoint.db`.
- Start with a small Dropbox subfolder to validate IAM, bucket policies, and throughput.
- Use S3 Lifecycle rules if you initially upload into a warmer class then transition to Glacier tiers.
- Consider enabling S3 Object Lock / bucket versioning if immutability or rollback is required.
