# Releasing

Tickle is published to PyPI as `tickle-soren-n`. Releases are tagged `vX.Y.Z` on `main`.

1. **Check the tree.** On an up-to-date `main` with a clean working tree, run the tests:
   ```
   uv run minigun --time-budget 60
   ```
   This takes about ten minutes.
   Also run the example project in a copy outside the repo (`tickle offline`, edit a source, `tickle offline`, `tickle clean`), and check that CI passed on the commit, which covers Linux, macOS and Windows.

2. **Bump the version** in `tickle/__init__.py` (`__version__`). It is the only place the version is recorded. Rename the "Unreleased" section of `CHANGELOG.md` to the new version, then commit both as "Prepare vX.Y.Z".

3. **Build** the sdist and wheel and check their contents:
   ```
   rm -rf dist
   uv build
   ```

4. **Publish** to PyPI with an API token:
   ```
   UV_PUBLISH_TOKEN=<token> uv publish
   ```
   To try the release first, publish to TestPyPI with `uv publish --publish-url https://test.pypi.org/legacy/` and a TestPyPI token.

5. **Tag and push:**
   ```
   git tag vX.Y.Z
   git push origin main vX.Y.Z
   ```

Versioning follows the existing 0.x scheme: bump the minor version for changes in behaviour or requirements (such as a new minimum Python version), and the patch version for fixes.
