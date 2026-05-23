# Contributing to Lakehouse Platform

Thank you for your interest in contributing! 💙

## 🚀 How to Contribute

### 🐛 Found a Bug?
- Check [existing issues](../../issues) first
- Open a **Bug Report** issue with reproduction steps

### ✨ Have a Feature Idea?
- Open a **Feature Request** issue to discuss it first
- We welcome PRs for well-scoped enhancements!

### 📖 Improve Documentation?
- Typos, clarifications, and examples are always welcome
- Just open a PR directly

## 🛠️ Development Setup

```bash
# Clone the repo
git clone git@github.com:nuyoah666/lakehouse-platform.git
cd lakehouse-platform

# Install backend dependencies
cd backend && npm install

# Start backend (port 7071)
node server.js

# Open frontend
# Open frontend/index.html in browser
```

### Prerequisites
- Windows 11 + WSL2 (recommended) or Linux
- Node.js ≥ 18
- Flink 1.20.x (in WSL2)
- Doris 2.1.x (in WSL2)
- Paimon 1.0.x (in `$FLINK_HOME/lib/`)

## 📋 Pull Request Guidelines

1. **Fork** the repo and create a feature branch
2. **Describe** your changes clearly in the PR description
3. **Test** with Flink + Paimon + Doris running
4. **Update docs** if you change user-facing behavior
5. **Keep PRs focused** — one feature/fix per PR

## 💡 Good First Issues

Look for issues labeled [`good first issue`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) — these are great for newcomers!

## 📧 Questions?

Feel free to open a **Question** issue — we're happy to help!

---

Thank you for making Lakehouse Platform better! 🎉
