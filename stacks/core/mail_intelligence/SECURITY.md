# Data-at-Rest Security
## Mail Intelligence Stack — Matheny Manor Server

---

## 1. Full-Disk Encryption (First Line of Defense)

**Ubuntu 24.04 — LUKS2 on the data partition**

If the server is not already using LUKS, this is the single highest-leverage thing
you can do. Physical access to the machine yields nothing without the passphrase.

```bash
# Check if your data partition is already encrypted
lsblk -o NAME,FSTYPE,MOUNTPOINT | grep crypt

# If not, encrypt a new data partition (do this before writing data to it)
cryptsetup luksFormat --type luks2 /dev/sdX
cryptsetup open /dev/sdX mail_data
mkfs.ext4 /dev/mapper/mail_data
```

For a home server that reboots occasionally, store the LUKS key in a USB-backed
keyfile so it can boot headlessly — but keep the USB physically separate from the
machine.

---

## 2. PostgreSQL — Encrypt Sensitive Columns

LUKS protects against physical theft. Column-level encryption protects against
a compromised application layer (e.g., a bug that exposes raw SQL access).

**pgcrypto for sensitive enrichment fields:**

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Store summary and action_description encrypted at rest
-- Decrypt only when needed by the application

-- Example: encrypt on insert
INSERT INTO mail_enrichments (document_id, summary, ...)
VALUES (
    1,
    pgp_sym_encrypt('Pay this bill by May 1', current_setting('app.encryption_key')),
    ...
);

-- Decrypt on read
SELECT pgp_sym_decrypt(summary::bytea, current_setting('app.encryption_key'))
FROM mail_enrichments WHERE document_id = 1;
```

**Set the encryption key via a Postgres parameter (not hardcoded in SQL):**

```bash
# In postgresql.conf or via ALTER SYSTEM
ALTER SYSTEM SET app.encryption_key = 'your-strong-key-here';
SELECT pg_reload_conf();
```

**Practical scope:** Encrypt `summary`, `action_description`, and `raw_text`.
Leave `document_type`, `sender_normalized`, and `document_date` unencrypted —
they're the columns dbt queries heavily and encrypting them would break indexing
and mart model logic.

---

## 3. MinIO — Encryption at Rest (SSE)

MinIO supports Server-Side Encryption natively. Enable it so scan files on disk
are encrypted even if someone copies the MinIO data directory.

```bash
# In your MinIO environment / docker-compose
MINIO_KMS_SECRET_KEY=mail-master-key:YOUR_BASE64_32_BYTE_KEY

# Verify SSE is active
mc admin info myminio | grep -i encrypt
```

**Set a bucket-level default encryption policy:**

```bash
mc encrypt set SSE-S3 myminio/mail-raw
```

All objects written to `mail-raw/` are now encrypted with AES-256 using
MinIO's KMS key. The key itself is stored in MinIO's internal KMS —
keep a secure backup of `MINIO_KMS_SECRET_KEY`.

---

## 4. Secrets Management — No Plaintext .env Files

**Use `direnv` + an encrypted secrets store, not a plain `.env`:**

```bash
# Option A: SOPS + age (lightweight, perfect for home server)
brew install sops age

# Generate an age key pair
age-keygen -o ~/.config/sops/age/keys.txt

# Encrypt your secrets file
sops --encrypt --age $(cat ~/.config/sops/age/keys.txt | grep "public key" | cut -d: -f2) \
     secrets.yaml > secrets.enc.yaml

# Decrypt at runtime (in your Dagster launch script)
sops --decrypt secrets.enc.yaml | dagster dev
```

**What to store in SOPS:**

```yaml
# secrets.yaml (encrypted by SOPS — safe to commit)
MINIO_ACCESS_KEY: ENC[...]
MINIO_SECRET_KEY: ENC[...]
POSTGRES_PASSWORD: ENC[...]
ANTHROPIC_API_KEY: ENC[...]
```

**Option B: HashiCorp Vault** — more overhead, but if you're already running
it for other services in your stack it gives you dynamic secrets and audit logs.

---

## 5. PostgreSQL Network Hardening

```sql
-- Create a dedicated app user with minimum permissions
CREATE USER mail_app WITH PASSWORD 'strong-random-password';
GRANT CONNECT ON DATABASE mail_intelligence TO mail_app;
GRANT USAGE ON SCHEMA public TO mail_app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO mail_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO mail_app;

-- Read-only user for Superset dashboards
CREATE USER mail_readonly WITH PASSWORD 'another-strong-password';
GRANT CONNECT ON DATABASE mail_intelligence TO mail_readonly;
GRANT USAGE ON SCHEMA public TO mail_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mail_readonly;
```

**pg_hba.conf — restrict connections to localhost + require scram-sha-256:**

```
# TYPE  DATABASE          USER          ADDRESS        METHOD
local   mail_intelligence mail_app                     scram-sha-256
host    mail_intelligence mail_app      127.0.0.1/32   scram-sha-256
host    mail_intelligence mail_readonly 127.0.0.1/32   scram-sha-256
```

Do not expose port 5432 externally. If you need remote access, use an
SSH tunnel:

```bash
ssh -L 5432:localhost:5432 yourserver
```

---

## 6. MinIO Access Control

```bash
# Dedicated policy for the Dagster pipeline user (read/write mail-raw only)
cat > mail-pipeline-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::mail-raw", "arn:aws:s3:::mail-raw/*"]
    }
  ]
}
EOF

mc admin policy create myminio mail-pipeline mail-pipeline-policy.json
mc admin user create myminio dagster-mail strongpassword
mc admin policy attach myminio mail-pipeline --user dagster-mail

# Read-only user for the FastAPI search service (presigned URL generation only)
mc admin user create myminio mail-readonly strongpassword2
```

---

## 7. UFW Firewall Rules

```bash
# Allow SSH only from your LAN subnet
ufw allow from 192.168.1.0/24 to any port 22

# Dagster UI — LAN only
ufw allow from 192.168.1.0/24 to any port 3000

# Superset — LAN only
ufw allow from 192.168.1.0/24 to any port 8088

# MinIO console — LAN only
ufw allow from 192.168.1.0/24 to any port 9001

# Block everything else
ufw default deny incoming
ufw enable
```

---

## 8. Backup Strategy for Encrypted Data

Mail documents you've filed are irreplaceable. Encrypt backups before
they leave your LAN.

```bash
# PostgreSQL — encrypted daily backup via pg_dump + age
pg_dump mail_intelligence | \
  age -r $(cat ~/.config/sops/age/keys.txt | grep public | cut -d: -f2) \
  > /mnt/backup/mail_db_$(date +%Y%m%d).sql.age

# MinIO — sync to a second local drive or NAS (encrypted at source via SSE)
mc mirror myminio/mail-raw /mnt/backup/mail-raw/

# Retention: keep 90 days of DB backups
find /mnt/backup -name "mail_db_*.sql.age" -mtime +90 -delete
```

---

## Priority Order (What to Do First)

| Priority | Action | Effort |
|----------|--------|--------|
| 1 | LUKS2 on data partition (if not already) | Medium — requires repartitioning |
| 2 | SOPS secrets management | Low — 1-2 hours |
| 3 | Dedicated Postgres users + pg_hba.conf | Low — 30 minutes |
| 4 | UFW firewall rules | Low — 30 minutes |
| 5 | MinIO SSE + access policies | Low — 1 hour |
| 6 | pgcrypto column encryption on raw_text | Medium — requires app code changes |
| 7 | Encrypted offsite backups | Medium — ongoing |

---

*The raw scan files and extracted text are the most sensitive artifacts in this
system. LUKS + MinIO SSE covers them at the storage layer. The PII redaction
step in the enrichment asset is your protection at the transit layer. Together
these form a layered defense without requiring enterprise tooling.*
