# ANP Gov.br Migration Plan

## Summary

ANP open-data ingestion moves from the legacy compact source to official gov.br HTML discovery. The chosen architecture is B: one schema 2.0 Forest manifest per ANP collection. Public dataset IDs and slugs stay stable, while each catalog entry now points to `anp/<slug>/manifest.json`.

## Flow

```text
gov.br ANP hub
=> collection detail pages under /dados-abertos/
=> direct resource URLs and official indirect system links
=> local profiling with cache reuse
=> one schema 2.0 manifest per collection
=> publish-catalog
=> portal, API, and SDK consume normal manifests
```

## Implementation

- Scraper: `src/forest_pipelines/datasets/anp/govbr.py`
- Runner config: `configs/datasets/anp/govbr.yml`
- Catalog SSOT: `configs/catalog/open_data.yml`
- Registry: `src/forest_pipelines/registry/datasets.py`
- Legacy removal: old compact modules, old ANP CLI commands, old Makefile ANP targets, compact tests, portal fallback

## Scraper Rules

- Landing page: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos`
- Detail pages are parsed from article content only.
- Legacy open-data portal links are ignored.
- HTTP redirects and meta refresh are resolved before profiling.
- Direct resources include CSV, ZIP, XLS, XLSX, JSON, XML, TXT, PDF, DOC, DOCX, ODS, ODT, SHP, GeoJSON, GPKG, and KML.
- Official interactive pages are kept as indirect items with `profile_status: "skipped"`.
- Main metadata PDFs go to `meta.metadata_file`.
- Additional documentation goes to `meta.custom_tags.documentation_files`.

## Rollout

1. Land the scraper and fixture tests.
2. Register all ANP collection IDs and catalog entries.
3. Remove CKAN compact code, CLI commands, Make targets, tests, and docs.
4. Remove portal and API compact fallback.
5. Update API test scripts so ANP uses the same manifest path as other datasets.
6. Run `make sync` to publish ANP manifests, followed by catalog publication.

## Risks

- gov.br HTML may change. Unit tests use fixtures so selector drift is visible in scraper tests.
- Some collections expose only interactive systems. These remain URL-only items with skipped profiling and public-safe warnings.
- Full ANP profiling can be slow. Routine sync uses existing incremental profiling cache; `--force` performs a complete reprofile.

## Verification

- Pipeline unit tests cover hub discovery, detail extraction, metadata splitting, meta refresh, and manifest shape.
- The URL-only catalog validator must pass.
- Portal typecheck and lint must pass after compact fallback removal.
- API tests no longer skip ANP entries.
