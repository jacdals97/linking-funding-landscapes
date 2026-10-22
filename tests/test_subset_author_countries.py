import json

import polars as pl
import pytest

from analysis import recalculate_scie_ssci_results as subset


@pytest.mark.parametrize("countries,codes", [
    (["United States"], {"US"}),
    (["United States", "Canada"], {"US", "CA"}),
    (["United States", "Japan"], {"US", "JP"}),
])
def test_author_summary_does_not_require_particular_countries(tmp_path, monkeypatch, countries, codes):
    for name, path in {
        "RUN": tmp_path, "VIEW": tmp_path / "view",
        "YEARS": tmp_path / "retained.parquet", "AUTHORS": tmp_path / "authors.parquet",
        "AUTHOR_PAIRS": tmp_path / "pairs.parquet",
    }.items():
        monkeypatch.setattr(subset, name, path)
    (subset.VIEW / "plotting_data").mkdir(parents=True)
    pl.DataFrame({"UT": ["WOS:funded", "WOS:unfunded"],
                  "has_funding_info": [True, False]}).write_parquet(subset.YEARS)
    pl.DataFrame({"UT": ["WOS:funded", "WOS:funded", "WOS:unfunded", "WOS:excluded"],
                  "author_countries": [json.dumps(countries)] * 4}).write_parquet(subset.AUTHORS)

    subset.aggregate_authors()

    result = pl.read_csv(subset.VIEW / "plotting_data/wos_author_country.tsv", separator="\t")
    assert set(result["auth_countries"]) == codes
    assert result["share_has_grant"].to_list() == [0.5] * len(codes)
    assert result["fractional_funded_paper_count"].sum() == pytest.approx(1.0)
    summary = json.loads((tmp_path / "author_country_summary.json").read_text())
    assert summary["records_with_known_author_country"] == 2
    assert summary["funded_records_with_known_author_country"] == 1
