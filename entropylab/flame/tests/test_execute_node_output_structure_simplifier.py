from entropylab.flame.execute._flush_runtime_data import _simplify_output_json_structure


simple_job_output = {
    "clerk": {
        "requested_salary": [1.2, 2.4],
        "requested_salary_time": [
            "2022-06-25T12:52:14.435Z",
            "2022-06-25T12:52:14.554Z",
        ],
    }
}
complex_job_output = {
    "QPU": {
        "final_data": [5, 8],
        "final_data_time": ["2022-06-25T12:41:01.226Z", "2022-06-25T12:41:04.421Z"],
    },
    "correction_1": {
        "correction_data": [[[5, 4], [7, 4]], [[8, 2], [8, 7]]],
        "correction_data_time": [
            "2022-06-25T12:41:01.226Z",
            "2022-06-25T12:41:04.421Z",
        ],
    },
    "final_report": {
        "final_plot": [{"png": ""}, {"png": ""}],
        "final_plot_time": ["2022-06-25T12:41:01.531Z", "2022-06-25T12:41:04.723Z"],
    },
}
deep_job_output = {
    "node_name_1": {
        "output_name_1": {
            "key_1": 1,
            "key_2": 1.2,
            "key_3": "a",
            "key_4": [1, 2, 3],
            "key_5": ["a", "b", "c"],
            "key_6": {
                "key_7": 1,
                "key_8": "a",
                "key_9": [1, 2, 3],
                "key_10": ["a", "b", "c"],
            },
        },
        "output_name_2": {
            "key_1": None,
            "key_2": [],
        },
    },
    "node_name_2": {
        "output_name_3": {
            "key_1": 1,
            "key_2": [
                {"key_2": 2},
                {"key_3": 3},
                {"key_4": [4]},
                {"key_5": [{"key_6": 6}]},
            ],
        }
    },
}

simple_job_structure = {
    "clerk": {"requested_salary": None, "requested_salary_time": None}
}
complex_job_structure = {
    "QPU": {"final_data": None, "final_data_time": None},
    "correction_1": {"correction_data": None, "correction_data_time": None},
    "final_report": {
        "final_plot": [{"png": None}, {"png": None}],
        "final_plot_time": None,
    },
}
deep_job_structure = {
    "node_name_1": {
        "output_name_1": {
            "key_1": None,
            "key_2": None,
            "key_3": None,
            "key_4": None,
            "key_5": None,
            "key_6": {"key_7": None, "key_8": None, "key_9": None, "key_10": None},
        },
        "output_name_2": {
            "key_1": None,
            "key_2": None,
        },
    },
    "node_name_2": {
        "output_name_3": {
            "key_1": None,
            "key_2": [
                {"key_2": None},
                {"key_3": None},
                {"key_4": None},
                {"key_5": [{"key_6": None}]},
            ],
        }
    },
}


def test_simplify_output_json_structure():
    assert _simplify_output_json_structure(simple_job_output) == simple_job_structure
    assert _simplify_output_json_structure(complex_job_output) == complex_job_structure
    assert _simplify_output_json_structure(deep_job_output) == deep_job_structure
