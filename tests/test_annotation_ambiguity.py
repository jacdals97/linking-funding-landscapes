import polars as pl
import pytest

from scripts.update_v1_annotations import aggregate_source, reconcile_source
from scripts.build_annotarium_funder_evaluation import status_and_context


def records(funders):
    return pl.DataFrame([
        {"paper_id": paper, "doi": "10.1/example", "title": "Example paper",
         "funder_name": name, "funder_id": entity_id}
        for paper, name, entity_id in funders
    ])


@pytest.mark.parametrize("name", ["Shared name", "", None])
@pytest.mark.parametrize("source", ["openalex", "dimensions"])
def test_same_doi_different_ids_requires_reannotation(name, source):
    current = aggregate_source(records([("p1", name, "F1"), ("p2", name, "F2")]))
    found = current["10.1/example"]
    assert found["ambiguous"]
    assert len(found["funders"]) == 2
    name_column = "funder_display_name" if source == "openalex" else "display_names"
    previous = {"DOI": "10.1/example", "title": "Example paper", name_column: name or "",
                "Number of Correct Matches": "1", "Coder": "reviewer"}
    updated = reconcile_source(source, [previous], current)[0]
    assert updated["needs_revision"] == "true"
    assert "ambiguous_current_records" in updated["revision_reason"]
    assert updated["Number of Correct Matches"] == ""
    assert updated["Coder"] == ""
    status, _, pending = status_and_context(found, previous, include_new_source_entries=False)
    assert pending and status != "carried_forward"


def test_duplicate_rows_and_order_do_not_create_ambiguity():
    current = aggregate_source(records([
        ("p1", "Funder A", "F1"), ("p1", "Funder A", "F1"),
        ("p1", "Funder B", "F2"), ("p2", "Funder B", "F2"), ("p2", "Funder A", "F1"),
    ]))
    assert not current["10.1/example"]["ambiguous"]


def test_id_only_funder_differs_from_no_funder():
    current = aggregate_source(records([("p1", None, "F1"), ("p2", None, None)]))
    assert current["10.1/example"]["ambiguous"]
