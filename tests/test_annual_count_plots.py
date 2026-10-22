import math
import xml.etree.ElementTree as ET

import pandas as pd
import pytest

from analysis.build_wos_funding_year_counts import write_plot


@pytest.mark.parametrize("count", [1, 10, 100])
@pytest.mark.parametrize("funded", [False, True])
def test_single_decade_value_produces_finite_svg(tmp_path, count, funded):
    path = tmp_path / "annual.svg"
    write_plot(pd.DataFrame({"year": [2020], "total_papers": [count],
                             "funded_papers": [count if funded else 0]}), path)
    root = ET.fromstring(path.read_text())
    curves = root.findall(".//{http://www.w3.org/2000/svg}polyline")
    assert len(curves) == 2
    assert curves[0].get("points")
    for curve in curves:
        for point in curve.get("points").split():
            assert all(math.isfinite(float(value)) for value in point.split(","))
