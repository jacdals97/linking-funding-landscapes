"""Workers must receive lookup tables when Python starts fresh child interpreters."""
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

import pandas as pd

from scripts_wos_funding.step1_attach_matches_to_wos import (
    extract_author_rows_from_global,
    initialize_target_uts as initialize_author_targets,
)
from scripts_wos_funding.step2_build_wos_validation_samples import (
    extract_wos_rows_from_global,
    initialize_target_uts,
)


def test_validation_targets_survive_spawn():
    rows = pd.DataFrame({"UT": ["WOS:keep", "WOS:drop"], "TI": ["Keep", "Drop"],
                         "DI": ["10.1/example", ""]})
    with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                             initializer=initialize_target_uts, initargs=({"WOS:keep"},)) as pool:
        result = pool.submit(extract_wos_rows_from_global, rows).result(timeout=60)
    assert result["UT"].tolist() == ["WOS:keep"]
    assert result["DOI"].tolist() == ["https://doi.org/10.1/example"]


def test_author_targets_survive_spawn():
    rows = pd.DataFrame({"UT": ["WOS:keep", "WOS:drop"], "author_seq_no": ["1", "1"],
                         "author_reprint": ["Y", "N"], "author_countries": ['["US"]', '["JP"]'],
                         "author_affiliations": ['["Example University"]', '[]']})
    with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                             initializer=initialize_author_targets,
                             initargs=("AUTHOR_TARGET_UTS", {"WOS:keep"})) as pool:
        result = pool.submit(extract_author_rows_from_global, rows).result(timeout=60)
    assert result["UT"].tolist() == ["WOS:keep"]
    assert result["affiliations"].tolist() == [["Example University"]]
