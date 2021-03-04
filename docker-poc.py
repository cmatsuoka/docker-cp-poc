#!/usr/bin/env python3

import argparse
import json
import os
import shutil
import sys
import tarfile
import textwrap
from io import BytesIO
from pathlib import Path
from typing import List

import docker  # type: ignore
import yaml

import craft_parts
import craft_parts.callbacks
import craft_parts.errors
from craft_parts import ActionType, Part, ProjectInfo, Step

_LAYER_DIR = Path("layer")


def main():
    options = _parse_arguments()

    try:
        process_parts(options)
    except OSError as err:
        print(f"Error: {err.strerror}.", file=sys.stderr)
        sys.exit(1)
    except craft_parts.errors.SchemaValidationError:
        print("Error: invalid parts specification.", file=sys.stderr)
        sys.exit(2)
    except craft_parts.errors.InvalidPartName as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(3)
    except ValueError as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(4)


def process_parts(options: argparse.Namespace) -> None:
    craft_parts.callbacks.register_prologue(build_stage_layer)
    craft_parts.callbacks.register_epilogue(create_final_image)

    with open(options.file) as f:
        part_data = yaml.safe_load(f)

    lf = craft_parts.LifecycleManager(
        part_data,
        application_name="docker-poc",
        disable_stage_packages=True,
        # custom arguments
        series="20.04",
        image_name="demo-image",
    )

    # Check if stage packages changed, if so clean the stage step
    stage_packages = lf.get_stage_packages()
    stage_deps = lf.resolve_package_dependencies(stage_packages)
    state_stage_deps = lf.get_state_assets("stage-packages", step=Step.PULL)

    if set(stage_deps) != set(state_stage_deps):
        lf.clean(Step.PULL)  # to update the pull state containing the stage packages
        lf.reload_state()
        if _LAYER_DIR.is_dir():
            shutil.rmtree(_LAYER_DIR)

    command = options.command if options.command else "prime"
    if command == "clean":
        _do_clean(lf, options)
        sys.exit()

    _do_step(lf, options)


def build_stage_layer(project_info: ProjectInfo, part_list: List[Part]) -> None:
    if not _LAYER_DIR.is_dir():
        print("Create stage layer")
        stage_packages = craft_parts.stage_packages_from_parts(part_list)

        # TODO: inject apt lists from host to prevent a race in cache updating

        dockerfile = textwrap.dedent(
            f"""\
            FROM ubuntu:{project_info.series}
            RUN apt update && apt install {" ".join(list(stage_packages))}
            """
        ).encode()

        client = docker.from_env()
        image, logs = client.images.build(
            tag="craft-parts-stage", fileobj=BytesIO(dockerfile)
        )
        image_data = image.save(named=True)

        with open("image_data.tar", "wb") as f:
            for chunk in image_data:
                f.write(chunk)

        print("Unpack stage layer")
        extract_stage_layer(Path("image_data.tar"), _LAYER_DIR)

        shutil.copytree(_LAYER_DIR, project_info.stage_dir, copy_function=os.link)
        shutil.copytree(_LAYER_DIR, project_info.prime_dir, copy_function=os.link)
    else:
        print("Using existing stage layer")


def create_final_image(project_info: ProjectInfo, part_list: List[Part]) -> None:
    print("Create final docker image")

    prime_dir = os.path.relpath(project_info.prime_dir)
    dockerfile = textwrap.dedent(
        f"""\
        FROM ubuntu:{project_info.series}
        COPY {prime_dir} /
        """
    )

    Path("Dockerfile").write_text(dockerfile)
    client = docker.from_env()
    client.images.build(tag=project_info.image_name, path=".")


def extract_stage_layer(image_file: Path, stage_dir: Path):
    image = tarfile.open(image_file)

    manifest_file = image.extractfile("manifest.json")
    if not manifest_file:
        raise RuntimeError("docker image missing manifest file")

    manifest = json.loads(manifest_file.read())
    layer = manifest[0]["Layers"][-1]
    layer_tar = tarfile.open(fileobj=image.extractfile(layer))
    for entry in layer_tar:
        layer_tar.extract(entry, path=stage_dir)


def _do_step(lf: craft_parts.LifecycleManager, options: argparse.Namespace) -> None:
    target_step = _parse_step(options.command) if options.command else Step.PRIME
    part_names = vars(options).get("parts", [])

    if options.update:
        lf.update()

    actions = lf.plan(target_step, part_names)

    if options.plan_only:
        printed = False
        for a in actions:
            if a.type != ActionType.SKIP:
                print(_action_message(a))
                printed = True
        if not printed:
            print("No actions to execute.")
        sys.exit()

    with lf.execution_context() as ctx:
        for a in actions:
            if a.type != ActionType.SKIP:
                print(f"Execute: {_action_message(a)}")
                ctx.execute(a)


def _do_clean(lf: craft_parts.LifecycleManager, options: argparse.Namespace) -> None:
    if options.plan_only:
        raise ValueError("Clean operations cannot be planned.")

    if not options.parts:
        print("Clean all parts.")

    lf.clean(None, options.parts)
    if _LAYER_DIR.is_dir():
        shutil.rmtree(_LAYER_DIR)


def _action_message(a: craft_parts.Action) -> str:
    msg = {
        Step.PULL: {
            ActionType.RUN: "Pull",
            ActionType.RERUN: "Repull",
            ActionType.SKIP: "Skip pull",
            ActionType.UPDATE: "Update sources for",
        },
        Step.BUILD: {
            ActionType.RUN: "Build",
            ActionType.RERUN: "Rebuild",
            ActionType.SKIP: "Skip build",
            ActionType.UPDATE: "Update build for",
        },
        Step.STAGE: {
            ActionType.RUN: "Stage",
            ActionType.RERUN: "Restage",
            ActionType.SKIP: "Skip stage",
        },
        Step.PRIME: {
            ActionType.RUN: "Prime",
            ActionType.RERUN: "Re-prime",
            ActionType.SKIP: "Skip prime",
        },
    }

    if a.reason:
        return f"{msg[a.step][a.type]} {a.part_name} ({a.reason})"

    return f"{msg[a.step][a.type]} {a.part_name}"


def _parse_step(name: str) -> Step:
    step_map = {
        "pull": Step.PULL,
        "build": Step.BUILD,
        "stage": Step.STAGE,
        "prime": Step.PRIME,
    }

    return step_map.get(name, Step.PRIME)


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        metavar="filename",
        default="parts.yaml",
        help="The parts specification file (default: parts.yaml)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Refresh the stage packages list before procceeding",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Show planned actions to be executed and exit",
    )

    subparsers = parser.add_subparsers(dest="command")

    pull_parser = subparsers.add_parser("pull", help="Pull the specified parts")
    pull_parser.add_argument("parts", nargs="*", help="The list of parts to pull")

    build_parser = subparsers.add_parser("build", help="Build the specified parts")
    build_parser.add_argument("parts", nargs="*", help="The list of parts to build")

    stage_parser = subparsers.add_parser("stage", help="Stage the specified parts")
    stage_parser.add_argument("parts", nargs="*", help="The list of parts to stage")

    prime_parser = subparsers.add_parser("prime", help="Prime the specified parts")
    prime_parser.add_argument("parts", nargs="*", help="The list of parts to prime")

    clean_parser = subparsers.add_parser(
        "clean", help="Clean the specified steps and parts"
    )
    clean_parser.add_argument(
        "parts", nargs="*", help="The list of parts whose this step should be cleaned"
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
