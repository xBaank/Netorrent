# NuGet Publishing Setup Guide

This guide explains how the Netorrent library uses GitHub Actions to publish packages to NuGet with both nightly and stable releases.

## Overview

The project implements a dual-feed publishing strategy:
- **Nightly builds**: Per-commit packages from `develop` branch
- **Stable releases**: Versioned packages from git tags

## Required Setup

### 1. GitHub Secrets

Add these secrets to your GitHub repository:

#### `NUGET_API_KEY`
- **Purpose**: Authenticate with NuGet.org for publishing
- **How to get**: 
  1. Go to [nuget.org](https://www.nuget.org/)
  2. Sign in with your Microsoft account
  3. Go to Account > API Keys
  4. Create a new API key with `Push` scope
  5. Set the glob pattern to `Netorrent.*` (or leave blank for all packages)
  6. Copy the generated key

### 2. Environment Protection Rules

Configure these environments in GitHub repository settings:

#### `nightly` Environment
- **Purpose**: Protect nightly package publishing
- **Protection rules**:
  - No reviewers required (for rapid deployment)
  - Wait timer: 0 minutes

#### `production` Environment
- **Purpose**: Protect stable release publishing
- **Protection rules**:
  - Require reviewers (1-2 reviewers recommended)
  - Wait timer: 5 minutes (optional)
  - Prevent self-approval

## Workflow Triggers

### Automatic Triggers

#### Nightly Builds (Per-Commit)
- **Trigger**: Every push to `develop` branch
- **Version**: `1.0.0-nightly-YYYYMMDD-HHMM-commit`
- **Package Type**: Pre-release
- **Symbol Packages**: No
- **Retention**: 30 days

#### Release Builds
- **Trigger**: Git tags matching `v*` (e.g., `v1.0.0`, `v1.1.0`)
- **Version**: From git tag
- **Package Type**: Stable
- **Symbol Packages**: Yes
- **GitHub Release**: Created automatically

### Manual Triggers

#### Manual Publishing Workflow
- **Trigger**: Workflow dispatch from GitHub Actions tab
- **Options**:
  - Custom version specification
  - Environment selection (staging/production)
  - Optional GitHub release creation
  - Custom release notes

## Version Strategy

### Nightly Version Format
```
1.0.0-nightly-20250114-1430-a1b2c3d
│   │       │    │    │
│   │       │    │    └─ Short commit SHA
│   │       │    └─────── Timestamp (HHMM)
│   │       └───────────── Date (YYYYMMDD)
│   └───────────────────── Base version
└───────────────────────── Nightly identifier
```

### Release Version Format
```
1.2.3
│ │ │
│ │ └─ Patch: Bug fixes
│ └─── Minor: New features (backward compatible)
└───── Major: Breaking changes
```

## Configuration Files

### Main Workflow: `.github/workflows/nuget-publish.yml`

Key features:
- Smart version detection based on trigger type
- Comprehensive testing before publishing
- Conditional symbol package inclusion
- Artifact management with retention policies
- GitHub release automation

### Manual Workflow: `.github/workflows/manual-publish.yml`

Key features:
- Version format validation
- Environment-specific publishing
- Optional GitHub release creation
- Release artifact attachment

### CI Workflow: `.github/workflows/dotnet.yml`

Updated to avoid conflicts with publishing workflows:
- Limited to main and develop branches
- Added package caching
- Code coverage collection
- Artifact upload for debugging

## Publishing Process

### 1. Development Flow

```bash
# Create feature branch
git checkout -b feature/new-feature
# Make changes...
git commit -m "Add new feature"
git push origin feature/new-feature
# Create PR to develop
# Merge to develop
# → Automatic nightly build triggered
```

### 2. Release Flow

```bash
# Ensure develop is stable
git checkout develop
git pull origin develop

# Create release branch
git checkout -b release/v1.0.0

# Update version in project file if needed
# Commit version changes
git commit -m "Bump version to 1.0.0"
git push origin release/v1.0.0

# Merge to main
git checkout main
git merge release/v1.0.0
git push origin main

# Create and push tag
git tag v1.0.0
git push origin v1.0.0
# → Automatic release build triggered
```

### 3. Manual Publishing

1. Go to GitHub Actions tab
2. Select "Manual NuGet Publish" workflow
3. Click "Run workflow"
4. Fill in parameters:
   - Version: `1.0.0`, `1.0.0-preview-123`, or `1.0.0-nightly-custom`
   - Environment: `staging` or `production`
   - Create Release: Optional for production releases
5. Click "Run workflow"

## Consumer Usage

### Stable Installation
```xml
<PackageReference Include="Netorrent" Version="1.0.0" />
```

### Range Installation
```xml
<PackageReference Include="Netorrent" Version="[1.0.0,2.0.0)" />
```

### Nightly Installation
```xml
<PackageReference Include="Netorrent" Version="1.0.0-nightly-*" />
```

### Pre-release Installation
```bash
dotnet add package Netorrent --prerelease
```

## Troubleshooting

### Common Issues

#### 1. Package Already Exists
- **Cause**: Version already published
- **Solution**: Use `--skip-duplicate` flag (already configured)

#### 2. API Key Invalid
- **Cause**: Expired or incorrect API key
- **Solution**: Regenerate API key and update GitHub secrets

#### 3. Version Format Error
- **Cause**: Invalid semantic version format
- **Solution**: Ensure version follows `X.Y.Z[-suffix]` format

#### 4. Tests Failed
- **Cause**: Breaking changes introduced
- **Solution**: Fix test failures before publishing

### Debugging

#### Workflow Logs
- Check GitHub Actions workflow runs
- Review build and test logs
- Verify package creation steps

#### Package Verification
```bash
# Download and inspect package
dotnet nuget push --source ./local-feed package.nupkg

# Verify package contents
dotnet nuget locals --list all
```

## Best Practices

### 1. Version Management
- Follow semantic versioning
- Update version numbers for breaking changes
- Use consistent version formatting

### 2. Testing
- Ensure all tests pass before publishing
- Maintain code coverage
- Test package installation in clean environment

### 3. Release Notes
- Provide meaningful release notes
- Document breaking changes
- Include migration guides for major versions

### 4. Security
- Keep API keys secure
- Use environment protection rules
- Regularly rotate API keys

### 5. Monitoring
- Monitor package download statistics
- Set up alerts for publishing failures
- Track consumer feedback

## Advanced Configuration

### Multiple Package Feeds

For organizations requiring separate feeds:

```yaml
# Custom feed configuration
NIGHTLY_FEED: https://api.nuget.org/v3/index.json
STABLE_FEED: https://api.nuget.org/v3/index.json
PRIVATE_FEED: https://pkgs.dev.azure.com/yourorg/_packaging/yourfeed/nuget/v3/index.json
```

### Conditional Publishing

```yaml
# Only publish on specific conditions
- if: github.actor == 'dependabot[bot]'
  run: echo "Skipping publishing for dependabot updates"
```

### Package Validation

```yaml
# Additional validation steps
- name: Validate package
  run: |
    dotnet nuget verify artifacts/*.nupkg
    dotnet nuget verify artifacts/*.snupkg
```

This setup provides a robust, automated NuGet publishing pipeline that supports both rapid development cycles through nightly builds and stable releases for production use.