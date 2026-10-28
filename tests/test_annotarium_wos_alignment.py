from scripts.build_annotarium_wos_expansion import aligned_wos_funders


def test_not_matched_does_not_consume_matched_metadata() -> None:
    ut = "WOS:example"
    labels = ["NOT MATCHED", "Funder A", "Funder B"]
    matches = {
        ut: {
            "funder a": [
                {
                    "grant_agency": "Funder A",
                    "gid": "OA_1",
                    "ids:ror": "https://ror.org/000000001",
                    "source": "funders",
                }
            ],
            "funder b": [
                {
                    "grant_agency": "Funder B",
                    "gid": "OA_2",
                    "ids:ror": "https://ror.org/000000002",
                    "source": "funders",
                }
            ],
        }
    }

    funders = aligned_wos_funders(ut, labels, matches)

    assert [funder["name"] for funder in funders] == labels
    assert funders[0] == {"name": "NOT MATCHED"}
    assert funders[1]["openalex_url"] == "https://openalex.org/F1"
    assert funders[1]["ror_url"] == "https://ror.org/000000001"
    assert funders[2]["openalex_url"] == "https://openalex.org/F2"
    assert funders[2]["ror_url"] == "https://ror.org/000000002"
