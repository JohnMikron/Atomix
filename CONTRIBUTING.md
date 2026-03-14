# Contributing to Atomix STM

Thank you for your interest in contributing to Atomix STM! This guide will help you get started.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Style Guide](#style-guide)
- [Reporting Issues](#reporting-issues)

---

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

---

## Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Atomix.git
   cd Atomix
   ```
3. **Create a branch** for your work:
   ```bash
   git checkout -b feature/your-feature-name
   ```

---

## Development Setup

### Prerequisites

- **Python 3.13+** (recommended for free-threading features)
- **Git** for version control

### Install in Development Mode

```bash
# Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate   # Windows

# Install in editable mode
pip install -e .

# Install dev dependencies
pip install pytest pytest-timeout
```

### Project Structure

```
Atomix/
├── atomix_stm/
│   ├── __init__.py       # Package exports
│   └── core.py           # Core STM implementation
├── tests/
│   ├── test_stm.py            # Basic tests
│   ├── test_stm_advanced.py   # Advanced concurrency tests
│   └── test_deep_evaluation.py # Deep evaluation tests
├── benchmarks/           # Performance benchmarks
├── examples/             # Usage examples
├── pyproject.toml        # Build configuration
└── README.md
```

---

## Making Changes

### What We Accept

- 🐛 **Bug fixes** — Always welcome!
- ✨ **New features** — Please open an issue first to discuss
- 📖 **Documentation** — Improvements, typo fixes, examples
- ⚡ **Performance** — Benchmarked improvements with evidence
- 🧪 **Tests** — More test coverage is always appreciated

### What to Avoid

- Breaking changes to the public API without prior discussion
- Large refactors without an approved design proposal
- Commits that reduce test coverage
- Adding external dependencies (Atomix is zero-dependency by design)

---

## Testing

### Running Tests

```bash
# Run all tests
python3 -m pytest tests/ -v --timeout=30

# Run a specific test file
python3 -m pytest tests/test_deep_evaluation.py -v

# Run with unittest (no pytest required)
python3 -m unittest discover -s tests -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Use descriptive test names: `test_atom_swap_does_not_deadlock`
- Test concurrency scenarios with `threading.Thread` and `threading.Barrier`
- Always use timeouts for concurrency tests to detect deadlocks
- Test both success and failure paths

### Test Categories

| File | Purpose |
|------|---------|
| `test_stm.py` | Core functionality (transactions, vectors, queues) |
| `test_stm_advanced.py` | Concurrency (contention, ABA, nested transactions) |
| `test_deep_evaluation.py` | Deep evaluation (all bug regression + improvements) |

---

## Pull Request Process

1. **Ensure all tests pass** before submitting
2. **Update documentation** if you changed functionality
3. **Add tests** for any new features or bug fixes
4. **Fill out the PR template** with a clear description
5. **Keep PRs focused** — one feature/fix per PR

### PR Title Format

```
[TYPE] Brief description

Examples:
[FIX] Resolve deadlock in Atom.swap()
[FEAT] Add compare_and_set() to Atom
[DOCS] Update installation instructions
[TEST] Add concurrent stress tests for STMAgent
[PERF] Optimize SeqLock read path
```

### Review Process

- All PRs require at least one maintainer review
- CI must pass (all tests green)
- Conversations must be resolved before merge

---

## Style Guide

### Python Code

- Follow **PEP 8** with a max line length of 120 characters
- Use **type hints** for all public function signatures
- Use **docstrings** for all public classes and methods
- Prefer `threading.RLock()` over `threading.Lock()` when reentrance is possible
- Never use bare `except:` — always specify exception types

### Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Classes | PascalCase | `TransactionCoordinator` |
| Functions | snake_case | `dosync`, `atomically` |
| Constants | UPPER_SNAKE | `PYTHON_3_13_PLUS` |
| Private | Leading underscore | `_history_lock` |

### Commit Messages

- Use present tense: "Fix bug" not "Fixed bug"
- Keep the first line under 72 characters
- Reference issues: "Fix #42 — Resolve deadlock in Atom.swap()"

---

## Reporting Issues

### Bug Reports

Please include:
- Python version (`python3 --version`)
- OS and version
- Minimal reproducible example
- Expected vs actual behavior
- Full traceback if applicable

### Feature Requests

Please include:
- Clear description of the proposed feature
- Use case / motivation
- Example of how the API would look

---

## Questions?

Open a [GitHub Discussion](https://github.com/JohnMikron/Atomix/discussions) or file an issue at [github.com/JohnMikron/Atomix/issues](https://github.com/JohnMikron/Atomix/issues).

Thank you for contributing to Atomix STM! 🎉
