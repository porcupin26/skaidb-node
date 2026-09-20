# Releasing

Every release is cut by pushing a version tag; GitHub Actions
(`.github/workflows/publish.yml`, on GitHub-hosted `ubuntu-latest`) does the
rest: it runs the tests, publishes `@skaidb/client` to npm and creates the
GitHub Release.

1. Pick the next [semver](https://semver.org/) version and put it in the
   four places that pin it: `package.json` (`version`), `test/client.test.js`
   (the `assert.equal(pkg.version, '…')` line) and the
   `github:porcupin26/skaidb-node#vX.Y.Z` install pins in `README.md` and
   `docs/getting-started.md`.
2. Add a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`; it becomes
   the GitHub Release notes.
3. Check, commit, tag and push:

   ```sh
   npm test && npm run pack:check
   git commit -am "chore(release): X.Y.Z"
   git tag vX.Y.Z
   git push origin main vX.Y.Z
   ```

4. Watch the "Publish to npm" run for the tag under Actions, then confirm
   with `npm view @skaidb/client versions`.

The publish job refuses a tag that does not equal `package.json`'s version,
skips npm (with a notice, still green) when the `NPM_TOKEN` repository
secret is missing, and leaves alone a version that is already on npm or a
release that already exists, so re-running a tag's workflow is safe. npm
never lets a published version be replaced: to fix a bad release, bump the
patch version and tag again.

`NPM_TOKEN` is an npm access token of a package maintainer that can publish
without a 2FA code (a classic **automation** token, or a granular access
token with *bypass 2FA* and read+write on `@skaidb/client`) and that has
no IP allowlist (GitHub-hosted runners use changing addresses), set under
the repository's Settings → Secrets and variables → Actions. If the
publish step fails with `404 Not Found - PUT …`, the token is what to
check; then re-run the failed job.
