# S3 cross-account replication — adding the `crm-customers` rule

Goal: replicate objects the Lambda writes to
`engineering-s3-data-share/crm-customers/**` in the **data account
`051826722213`** into a bucket in the **engineering account `908519936890`**.

You already have one replication rule on a bucket. S3 supports **multiple rules
per bucket**, so this is an *additional* rule scoped to the `crm-customers/`
prefix — you do not touch the existing one. Each rule needs a distinct
**priority** and (to be safe) a non-overlapping prefix scope.

Two accounts are involved:

| Role        | Account        | Bucket                                          |
|-------------|----------------|-------------------------------------------------|
| Source      | 051826722213   | `engineering-s3-data-share` (prefix `crm-customers/`) |
| Destination | 908519936890   | the engineering-side bucket you replicate into  |

---

## Prerequisites (both buckets)

1. **Versioning must be ON** on both the source and the destination bucket —
   replication requires it.
   - Source: S3 → `engineering-s3-data-share` → Properties → Bucket Versioning → Enable.
   - Destination: same, in the engineering account.

---

## Step 1 — (Data account) IAM role for replication

In account `051826722213`, create an IAM role assumable by S3.

Trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "s3.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

Permissions policy (replace `DEST_BUCKET` with the engineering bucket name):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetReplicationConfiguration",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::engineering-s3-data-share"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObjectVersionForReplication",
        "s3:GetObjectVersionAcl",
        "s3:GetObjectVersionTagging"
      ],
      "Resource": "arn:aws:s3:::engineering-s3-data-share/crm-customers/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ReplicateObject",
        "s3:ReplicateDelete",
        "s3:ReplicateTags",
        "s3:ObjectOwnerOverrideToBucketOwner"
      ],
      "Resource": "arn:aws:s3:::DEST_BUCKET/*"
    }
  ]
}
```

---

## Step 2 — (Engineering account) destination bucket policy

In account `908519936890`, attach a bucket policy that lets the source
replication role write replicas and hand ownership to the destination owner
(replace `DEST_BUCKET` and the role ARN):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::051826722213:role/<replication-role-name>"
    },
    "Action": [
      "s3:ReplicateObject",
      "s3:ReplicateDelete",
      "s3:ReplicateTags",
      "s3:ObjectOwnerOverrideToBucketOwner",
      "s3:GetBucketVersioning",
      "s3:PutBucketVersioning"
    ],
    "Resource": [
      "arn:aws:s3:::DEST_BUCKET",
      "arn:aws:s3:::DEST_BUCKET/*"
    ]
  }]
}
```

---

## Step 3 — (Data account) add the replication rule

S3 → `engineering-s3-data-share` → Management → **Replication rules** →
**Create replication rule**:

1. **Name**: `crm-customers-to-engineering`.
2. **Status**: Enabled.
3. **Priority**: give it a number different from the existing rule (e.g. if the
   existing one is `1`, use `2`). Higher number = higher priority when scopes overlap.
4. **Scope**: *Limit to a prefix* → `crm-customers/`. This keeps it independent of
   your existing rule.
5. **Destination**: *Specify a bucket in another account* → account ID
   `908519936890`, bucket = `DEST_BUCKET`.
6. **Change object ownership to destination bucket owner**: **enable** (recommended
   for cross-account, so the engineering account owns the replicas). This is what
   requires the `ObjectOwnerOverrideToBucketOwner` permission above.
7. **IAM role**: choose the role from Step 1.
8. (Optional) Enable **Replication Time Control (RTC)** if you need an SLA on
   replication latency; otherwise leave off to save cost.
9. Save. When prompted, choose **Do not replicate existing objects** (we only need
   new daily folders) — or replicate existing if you want a backfill.

---

## Step 4 — verify

- Trigger the Lambda once (Console → `crm-profile-klaviyo-export` → Test, or wait
  for the 08:00 UTC run).
- Confirm objects under `engineering-s3-data-share/crm-customers/<date>/` appear in
  `DEST_BUCKET/crm-customers/<date>/` in the engineering account within a few minutes.
- Source object → Properties → **Replication status** should read `COMPLETED`.

> Replication only copies objects created **after** the rule is enabled. Anything
> written before then must be backfilled (S3 Batch Replication or a one-off copy).
