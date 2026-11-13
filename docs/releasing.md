# Releasing

Tickle is published to PyPI as `tickle-soren-n`. Releases are tagged `vX.Y.Z` on `main`.

1. **Check the tree.** On an up-to-date `main` with a clean working tree, run the lint, the type check and the tests:
   ```
   uv run ruff check . && uv run pyright
   uv run minigun --time-budget 60
   ```
   The tests take about four minutes.
   Also run the example project in a copy outside the repo (`tickle offline`, edit a source, `tickle offline`, `tickle clean`).

2. **Prepare the release.** Bump the version in `tickle/__init__.py` (`__version__`); it is the only place the version is recorded. Rename the "Unreleased" section of `CHANGELOG.md` to the new version, keeping any "Upgrading" notes at its top, and add an empty "Unreleased" section above it. Commit both as "Prepare vX.Y.Z" and push `main`.

3. **Wait for CI** on that commit; it covers Linux, macOS and Windows, and Python 3.11 end to end. Release only the commit CI passed.

4. **Build** the sdist and wheel and check their contents:
   ```
   rm -rf dist
   uv build
   ```
   Then install the wheel in a clean environment with the oldest supported Python and run it:
   ```
   uv run --isolated --no-project --python 3.11 --with dist/*.whl tickle version
   ```
   and, in a copy of `examples/hello_world` outside the repo, `uv run --isolated --no-project --python 3.11 --with <path to the wheel> tickle offline`.

5. **Publish** to PyPI with an API token:
   ```
   UV_PUBLISH_TOKEN=<token> uv publish
   ```
   To try the release first, publish to TestPyPI with `uv publish --publish-url https://test.pypi.org/legacy/` and a TestPyPI token.

6. **Tag and push** the commit CI passed:
   ```
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
   and create a GitHub release for the tag with the changelog section as its notes, upgrade notes first.

Versioning follows the existing 0.x scheme: bump the minor version for changes in behaviour or requirements (such as a new minimum Python version), and the patch version for fixes.
