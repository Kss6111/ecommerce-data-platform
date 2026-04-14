#!/bin/bash
set -e
cat > /var/lib/postgresql/data/pg_hba.conf << EOF
local   all   all                trust
host    all   all   0.0.0.0/0    trust
host    all   all   ::1/128      trust
EOF