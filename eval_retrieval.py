"""
eval_retreival.py

This file calls the different retrieval setups in this repo and evaluates the retrieval
in terms of speed and accuracy.
"""


def get_retrieval(inputs: list):
    pass


def evaluate_chroma(test_case_dict: dict, output_dict: dict):
    pass


if __name__ == "__main__":
    import json

    test_case_dict = {}
    with open("json_outs/eval_retrieval.json", "r") as f:
        test_case_dict = json.load(f)

    print(json.dumps(test_case_dict, indent=2))
