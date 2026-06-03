---
name: Bug Report
about: Report a bug or unexpected behavior in Atomix STM
title: '[BUG] '
labels: bug
assignees: ''
---

## Bug Description

A clear and concise description of the bug.

## Steps to Reproduce

```python
# Minimal reproducible example:
from atomix_stm import Ref, atomically

# Your code here...
```

1. Step 1
2. Step 2
3. Step 3

## Expected Behavior

What you expected to happen.

## Actual Behavior

What actually happened. Include the full traceback if applicable:

```
Traceback (most recent call last):
  ...
```

## Environment

- **OS**: (e.g., Ubuntu 24.04, macOS 14, Windows 11)
- **Python version**: (run `python3 --version`)
- **Atomix STM version**: (run `python3 -c "import atomix_stm; print(atomix_stm.__version__)"`)
- **Free-threading enabled**: Yes / No (Python 3.13+ only)

## Additional Context

Any other context, logs, or information about the problem.
