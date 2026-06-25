# Shockingly faster cache action

This action is a drop-in replacement for the official `actions/cache@v6` action, for use with the [RunsOn](https://runs-on.com/?ref=cache) self-hosted GitHub Action runner provider, or with your own self-hosted runner solution.

![image](https://github.com/runs-on/cache/assets/6114/e61c5b6f-aa86-48be-9e1b-baac6dce9b84)

It will automatically store your caches in a dedicated RunsOn S3 bucket that lives close to your self-hosted runners, ensuring you get at least 200MiB/s download and upload throughput when using caches in your workflows. The larger the cache, the faster the speed.

Also note that you no longer have any limit on the size of the cache. The bucket has a lifecycle rule to remove items older than 10 days.

If no S3 bucket is provided, it will also transparently switch to the default behaviour. This means you can use this action and switch between RunsOn runners and official GitHub runners with no change.

> [!IMPORTANT]
> This fork runs on the Node.js 24 runtime (`node24`) and requires a minimum Actions Runner version of `2.327.1`.
> If you are using self-hosted runners, ensure they are updated before upgrading.

## Usage with RunsOn

If using [RunsOn](https://runs-on.com), simply replace `actions/cache@v6` with `foresight-sports/cache@<sha>`. All the official options are supported.

```diff
- - uses: actions/cache@v6
+ - uses: foresight-sports/cache@<commit-sha>
    with:
      ...
```

Please refer to [actions/cache](https://github.com/actions/cache) for usage.

## Usage outside RunsOn

If you want to use this in your own infrastructure, setup your AWS credentials with [aws-actions/configure-aws-credentials](https://github.com/aws-actions/configure-aws-credentials), then:

```yaml
  - uses: aws-actions/configure-aws-credentials@v4
    ...
  - uses: foresight-sports/cache@<commit-sha>
    with:
      ...
    env:
      RUNS_ON_S3_BUCKET_CACHE: name-of-your-bucket
```

Be aware of S3 transfer costs if your runners are not in the same AWS region as your bucket.

## Special environment variables

* `RUNS_ON_S3_BUCKET_CACHE`: if set, the action will use this bucket to store the cache.
* `RUNS_ON_S3_BUCKET_ENDPOINT`: if set, the action will use this endpoint to connect to the bucket. This is useful if you are using AWS's S3 transfer acceleration or a non-AWS S3-compatible service.
* `RUNS_ON_RUNNER_NAME`: when running on RunsOn, where this environment variable is non-empty, existing AWS credentials from the environment will be discarded. If you want to preserve existing environment variables, set this to the empty string `""`.
* `RUNS_ON_S3_FORCE_PATH_STYLE` or `AWS_S3_FORCE_PATH_STYLE`: if one of those environment variables equals the string `"true"`, then the S3 client will be configured to force the path style.

## Compression level input

All variants of this action (`foresight-sports/cache`, `foresight-sports/cache/restore`, and `foresight-sports/cache/save`) accept a `compression-level` input. Set it to any integer from `0` to `9`:

* `0` (default) keeps using raw tar archives with no compression – the fastest option for large caches.
* `1-9` enable gzip compression at the requested level. Higher values trade additional CPU for slightly smaller archives.

Example:

```yaml
- uses: foresight-sports/cache@<commit-sha>
  with:
    path: ~/.npm
    key: node-deps-${{ hashFiles('package-lock.json') }}
    compression-level: 3
```

When gzip compression is enabled, both standard GitHub caches and RunsOn's S3 backend honor the chosen level.

## Action pinning

Contrary to the upstream action, `dev/no-compression` is a branch. When merging a stable release from upstream (e.g. v6.0.0), publish an equivalent tag in this repository. You can either pin to that tag, or a specific commit.

## Upstream v6 changes

This fork merges upstream `actions/cache@v6.0.0`, which:

* Updates `@actions/cache`, `@actions/core`, `@actions/exec` to latest major versions
* Migrates to ESM module system
* Runs on Node.js 24 (`node24`)

Foresight-specific behavior is preserved: S3-backed RunsOn cache backend, no-compression default, and split `restore/` + `save/` composite actions.
