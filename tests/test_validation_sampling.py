from argparse import Namespace
from contextlib import nullcontext

import pandas as pd

from scripts_wos_funding import step2_build_wos_validation_samples as validation


def test_samples_do_not_depend_on_worker_completion_order(tmp_path, monkeypatch):
    matches = tmp_path / "grant_institutions_test.parquet"
    pd.DataFrame({"UT": [f"WOS:{i:02d}" for i in range(20)],
                  "display_name": [f"Funder {i}" for i in range(20)],
                  "gid": [f"OA_{i}" for i in range(20)], "source": ["funders"] * 20,
                  "grant_agency": [f"Agency {i}" for i in range(20)]}).to_parquet(matches)
    pieces = [pd.DataFrame({"UT": [f"WOS:{i:02d}"], "title": [f"Paper {i}"],
                            "DOI": [f"https://doi.org/10.1/{i}"]}) for i in range(20)]

    class Reader:
        def __init__(self, *args, **kwargs):
            pass

        def task_count(self):
            return len(pieces)

        def process(self, *args, **kwargs):
            return iter(pieces)

    monkeypatch.setattr(validation, "PieceReader", Reader)
    monkeypatch.setattr(validation, "ProcessPoolExecutor", lambda **kwargs: nullcontext(None))
    for name in ["forward", "reverse"]:
        monkeypatch.setattr(validation, "parse_args", lambda: Namespace(
            matches=matches, wos_main=tmp_path / "unused.parquet", output_dir=tmp_path / name,
            output_basename=None, workers=2, sample_size=5))
        validation.main()
        pieces.reverse()

    for name in ["complete", "sample_by_paper", "sample_by_agency"]:
        filename = f"{name}_validation_dataset_test.csv"
        assert (tmp_path / "forward" / filename).read_bytes() == (tmp_path / "reverse" / filename).read_bytes()
