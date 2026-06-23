# Releasing

This package publishes to [NuGet.org](https://www.nuget.org) from GitHub Actions using
**Trusted Publishing** (OIDC). No long-lived API key is stored anywhere.

Versions are **tag-driven** via [MinVer](https://github.com/adamralph/minver): the git tag
sets the package version. A tag of `v0.1.0` produces package version `0.1.0`. Untagged builds
get a `0.0.0-alpha.*` version, so they can never accidentally publish over a real release.

## One-time setup

1. **Create / sign in to a NuGet.org account** that will own the `Databricks.Zerobus.Sdk` id.

2. **Configure a Trusted Publishing policy** on NuGet.org
   (Account -> Trusted Publishing) pointing at this repository:
   - Repository owner: `guanjieshen`
   - Repository: `zerbus-dotnet`
   - Workflow file: `release.yml`
   - Environment: `release`

3. **Add a repository variable** `NUGET_USER` (Settings -> Secrets and variables -> Actions ->
   Variables) set to your NuGet.org account name. The release workflow passes it to the
   `NuGet/login` action.

4. *(Optional, recommended)* Create a GitHub **Environment** named `release` (Settings ->
   Environments) and add required reviewers, so a human approves each publish.

> First publish of a brand-new package id: if Trusted Publishing rejects the very first push
> because the id does not exist yet, do a single manual `dotnet nuget push` with a temporary
> API key to create the id, then rely on Trusted Publishing for every release after that.

## Cutting a release

1. Make sure `main` is green (the CI workflow builds, tests, and runs the consumer smoke test).
2. Tag and push:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. The **Release** workflow then restores, builds, tests, packs, and publishes that version to
   NuGet.org. Publishing only happens if the tests pass.

That is the whole process: tag, push, done.

## Local verification

```bash
dotnet pack src/Databricks.Zerobus.Sdk -c Release -o ./artifacts
# inspect ./artifacts/*.nupkg, or install it from a local feed to smoke-test a consumer
```
