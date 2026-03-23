# Preprocessing Libraries

This directory vendors external preprocessing dependencies used by this repository.

Current libraries:

- `openalex-raw`: https://github.com/filipinascimento/openalex-raw
- `WOSRaw`: https://github.com/filipinascimento/WOSRaw

Install these packages before running the preprocessing pipeline once scripts start importing them.

## Option 1: Use The Git Submodules In This Repository

If you cloned this repository without submodules, initialize them first:

```bash
git submodule update --init --recursive preprocessing/libraries/openalex-raw preprocessing/libraries/WOSRaw
```

If you are cloning the repository from scratch, you can also do:

```bash
git clone --recurse-submodules <this-repo-url>
```

Then install both libraries into your active Python environment from the local submodule checkouts:

```bash
python -m pip install -e preprocessing/libraries/openalex-raw
python -m pip install -e preprocessing/libraries/WOSRaw
python -m pip install -U dbgz
```

Notes:

- Editable installs are useful here because the code lives inside the repository checkout.
- `WOSRaw` may require a recent `dbgz`, so the explicit upgrade is recommended.

## Option 2: Install Directly From GitHub

If you do not want to use the vendored submodules, install directly from the upstream repositories:

```bash
python -m pip install git+https://github.com/filipinascimento/openalex-raw.git
python -m pip install git+https://github.com/filipinascimento/WOSRaw.git
python -m pip install -U dbgz
```

## Recommended Order

1. Create or activate the Python environment you use for this project.
2. Install `openalex-raw` and `WOSRaw` using one of the options above.
3. Run the preprocessing scripts for this repository.

## Updating The Submodules

To pull the latest upstream versions later:

```bash
git submodule update --remote preprocessing/libraries/openalex-raw preprocessing/libraries/WOSRaw
```

Review and commit the resulting submodule pointer changes in this repository when you are ready.