# ENGINEERING_RULES.md

# TiDB/TiKV Large Go Project Engineering Rules

These rules are mandatory for ALL agents.

The goal is:

* reduce source corruption
* reduce cascading compile failures
* reduce cache confusion
* reduce uncontrolled modifications

---

# Rule 1: Small-Step Modification

NEVER perform large multi-file modifications in one step.

Required workflow:

1. modify ONE file
2. run build
3. verify binary updated
4. continue next file

Forbidden:

* modify many files before validation
* batch refactor
* broad sed replacement

---

# Rule 2: Avoid Blind Multi-line Replacement

Multi-line replacement is HIGH RISK.

Avoid:

* sed multi-line replacement
* regex structural replacement
* automatic bracket rewrite

Common failure modes:

* broken braces
* broken imports
* broken commas
* malformed struct
* invalid indentation

Preferred approach:

append small helper functions at file end:

```bash
cat << 'EOF' >> file.go
...
EOF
```

instead of modifying middle structures.

---

# Rule 3: Validate After Every Step

After EVERY modification:

```bash
go build ./...
```

or:

```bash
make dev
```

MUST succeed before continuing.

Never accumulate compile failures.

---

# Rule 4: Binary Timestamp Verification

Go build cache may return stale results.

After build:

MUST verify:

```bash
ls -l binary
```

timestamp changed.

Recommended workflow:

```bash
rm -f /tmp/tidb-server-pcr
go build -o /tmp/tidb-server-pcr
ls -l /tmp/tidb-server-pcr
```

Never trust "go build success" alone.

---

# Rule 5: One Logical Change Per Commit

Each commit should contain:

* one feature
* one bugfix
* one refactor

Avoid mixed commits.

---

# Rule 6: Avoid Import Reordering

Automatic import rewrite may create conflicts.

Prefer minimal import modifications.

---

# Rule 7: Preserve Existing Structure

Prefer:

* wrapper
* adapter
* helper function

Avoid:

* moving functions
* reorganizing files
* rewriting interfaces

---

# Rule 8: Avoid Agent Scope Expansion

Do NOT:

* "clean up" unrelated code
* refactor nearby modules
* optimize unrelated paths

Only modify approved scope.

---

# Rule 9: Build Frequently

For TiDB/TiKV projects:

build frequency is MORE important than coding speed.

Preferred:

small validated steps.

Forbidden:

large unvalidated modifications.

---

# Rule 10: Compile Success != Correctness

Even after successful build:

must validate:

* runtime behavior
* failover behavior
* compatibility
* integration tests

Especially for:

* replication
* PD
* TSO
* region cache
* watch stream

---

# Important Principle

For distributed systems:

stability > elegance

compatibility > cleanliness

small safe patch > large beautiful rewrite

