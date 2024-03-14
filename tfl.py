import difflib
from pathlib import Path

from tflwrapper import line as LineEndpoint
from tqdm import tqdm
from utils_python import dump_data, read_dict_from_file, serialize_data

from create_mappings import DEFAULT_MAPPING_DIR
from utils import DEFAULT_DATA_ROOT

app_key = None

TFL_DATA_ROOT = Path(DEFAULT_DATA_ROOT, "tfl")

from argparse import ArgumentParser, Namespace


class ArgsNamespace(Namespace):
    input_dir: Path
    output_dir: Path
    mode: str
    line: str | None


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-dir",
        default=DEFAULT_MAPPING_DIR,
        help="default: '%(default)s'",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=TFL_DATA_ROOT,
        help="default: '%(default)s'",
    )
    parser.add_argument("mode", nargs="?")
    parser.add_argument("line", nargs="?")
    return parser.parse_args(namespace=ArgsNamespace())


assert app_key is not None
LINE_ENDPOINT = LineEndpoint(app_key)


def get_modes():
    mode_list = LINE_ENDPOINT.getModes()
    return {mode["modeName"]: mode for mode in mode_list}


def get_lines(mode: str):
    line_list = LINE_ENDPOINT.getAllByModes([mode])
    return {line["id"]: line for line in line_list}


WORDS_TO_REMOVE: set[str] = {" Underground Station", " Rail Station", " DLR Station"}
# WORDS_TO_APPEND = {"Station"}
WORDS_TO_APPEND: set[str] = set()

inexact_matches: dict[str, dict[str, dict[str, dict[str, int|float] ]]] = {}


def get_identifier_from_stoppoint(stoppoint: dict, line, mode):
    rm_mappings = RM_STATION_MAPPINGS_MODIFIED
    candidate_names_base = {
        "stoppoint_commonName": stoppoint["commonName"],
    }
    for i, child in enumerate(stoppoint["children"]):
        if (commonName := child["commonName"]) not in candidate_names_base.values():
            candidate_names_base[f"child-{i}_commonName"] = commonName
    candidate_names = {**candidate_names_base}
    # print(candidate_names)
    scores: dict[str, int|float] = {}

    for word in WORDS_TO_REMOVE:
        for method, candidate_name in candidate_names_base.items():
            if word not in candidate_name:
                continue
            candidate_modified = candidate_name.replace(word, "")
            if candidate_modified not in candidate_names.values():
                candidate_names[f"{method}_removed-{word}"] = candidate_modified

    for word in WORDS_TO_APPEND:
        for method, candidate_name in candidate_names_base.items():
            if candidate_name.endswith(word):
                continue
            candidate_modified = candidate_name + (f" {word}")
            if candidate_modified not in candidate_names.values():
                candidate_names[f"{method}_added-{word}"] = candidate_modified

    exact_match = False
    # todo:
    # extract words like "Rail", "Station", "Tram" from both name types
    # then get a subset of RM stations which have any words in common with TFL stations
    # then evaluate similarity with those.
    # This should help prevent situations like:
    # {'child-2_commonName': 'Woolwich Station',
    # 'child-3_commonName': 'Woolwich Crossrail Station',
    # 'stoppoint_commonName': 'Woolwich'}
    # Woolwich -> {
    #     "Norwich Station": 0.8387096774193549,
    #     "Bloxwich Station": 0.8125,
    #     "North Dulwich Station": 0.41379310344827586,
    #     "Northwich Station": 0.7878787878787878,
    #     "New Cross Station": 0.6976744186046512,
    #     "Crosshill Station": 0.6976744186046512,
    #     "Bromley Cross Station": 0.6808510638297872
    # }
    for method, name in candidate_names.items():
        if name in rm_mappings:
            scores[name] = 1
            exact_match = True
            continue
        matches_list: list[str] = difflib.get_close_matches(name, rm_mappings.keys(), cutoff=0)
        for match in matches_list:
            score = difflib.SequenceMatcher(None, name, match).ratio()
            scores[match] = max(scores.get(match, 0), score)

    if exact_match:
        exact_matches = {name: score for name, score in scores.items() if score == 1}
        if len(exact_matches) != 1:
            breakpoint()
            # raise NotImplementedError(
            # f"Got unexpected number of exact_matches {exact_matches} for stopPoint {stoppoint['commonName']}"
            # )
        station = list(exact_matches.keys())[0]
        station_data = rm_mappings[station]
        # print(f"TFL:{stoppoint['commonName']} -> RM:{station}")#={station_data}
        #
    else:
        # todo: save non-exact messages for later examination to eventually create a manual correspondence?
        # pprint(candidate_names)
        inexact_matches.setdefault(mode, {}).setdefault(line, {})[
            stoppoint["commonName"]
        ] = scores
        # print(f"{stoppoint['commonName']} ->", serialize_data(scores))
        # print()
    # exit()


def get_stoppoints(line: str, mode: str):
    # getAllStopPoints is broken
    # you must edit it to change `line` to `line_name`, except in the `super()` call
    stoppoint_list = LINE_ENDPOINT.getAllStopPoints(line)
    for stoppoint in (pbar3 := tqdm(stoppoint_list, leave=False)):
        pbar3.set_description(stoppoint["commonName"])
        get_identifier_from_stoppoint(stoppoint, line, mode)


def main():
    args = parse_args()
    global RM_STATION_MAPPINGS
    RM_STATION_MAPPINGS = read_dict_from_file(
        Path(args.input_dir, "STATION-all-mappings-single.json"),
        # Path(args.input_dir, "STATION-all-mappings-multiple.json"),
        optional=False,
    )
    global RM_STATION_MAPPINGS_MODIFIED
    RM_STATION_MAPPINGS_MODIFIED = {
        k.replace(" Tram Stop", "").replace(" Station", ""): v
        for k, v in RM_STATION_MAPPINGS.items()
    }
    # print(RM_STATION_MAPPINGS_MODIFIED)
    # exit()

    # for station_name, station_info in RM_STATION_MAPPINGS.items():
    #     if not station_name.endswith("Station") and not station_name.endswith("Tram Stop"):
    #         print(station_name)
    #         pprint(station_info)
    #         print()
    #         # breakpoint()
    #         # pass
    # exit()

    # dump_data({mode: get_lines(mode) for mode in get_modes()})
    modes = get_modes()
    if args.mode is not None and args.mode not in modes:
        raise ValueError(f"mode '{args.mode}' not in {modes.keys()}")

    for mode_id, _mode_data in (pbar1 := tqdm(modes.items())):
        pbar1.set_description(mode_id)
        if args.mode is not None and mode_id != args.mode:
            continue
        lines = get_lines(mode_id)
        if args.line is not None and args.line not in lines:
            raise ValueError(f"line '{args.line}' not in {lines.keys()}")
        for line_id, _line_data in (pbar2 := tqdm(lines.items(), leave=False)):
            pbar2.set_description(line_id)
            if args.line is not None and line_id != args.line:
                continue
            get_stoppoints(line_id, args.mode)
    inexact_matches_file = "inexact_matches.json"
    combined_inexact_matches = read_dict_from_file(inexact_matches_file, optional=True)
    combined_inexact_matches.update(inexact_matches)
    dump_data(combined_inexact_matches, inexact_matches_file)


if __name__ == "__main__":
    main()
