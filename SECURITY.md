# Security Policy

## Supported Versions

The following versions of Atomix STM receive security updates:

| Version | Supported          |
|---------|--------------------|
| 3.3.x   | ✅ Active support  |
| 3.2.x   | ⚠️ Critical fixes only |
| < 3.2   | ❌ End of life     |

We recommend always using the latest stable release.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

To report a vulnerability, please:

1. **Open a [private security advisory](https://github.com/JohnMikron/Atomix/security/advisories/new)** on GitHub.
2. Include the following information:
   - A description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact (e.g., data corruption, deadlock exploitation, privilege escalation)
   - Any known mitigations or workarounds
   - Your contact information (optional)

## Response Timeline

| Stage | Time |
|-------|------|
| Initial acknowledgment | Within 48 hours |
| Severity assessment | Within 5 business days |
| Fix development | Depends on complexity |
| Public disclosure | After fix is released |

## Security Considerations

Atomix STM is a **concurrency library** — security vulnerabilities specific to this domain include:

- **Livelock / Starvation**: A crafted transaction load could exhaust retry budgets
- **Memory exhaustion**: Excessive MVCC history could be abused to exhaust heap
- **Thread safety violations**: Race conditions in internal state management
- **Denial of service via deadlock**: Exploiting lock ordering assumptions

We apply the following mitigations:
- Configurable `max_retries` limits per transaction
- Adaptive `STMReaper` for history garbage collection
- All internal locks use `threading.RLock()` where reentrance is required
- Transaction timeouts prevent indefinite blocking

## Scope

This policy covers the `atomix_stm` Python package. It does **not** cover:
- Third-party dependencies (there are none — Atomix is zero-dependency)
- The user's application code built on top of Atomix
- Python interpreter vulnerabilities
