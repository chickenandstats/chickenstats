"""Check that a commit message has a one-line summary, a blank line and a body.

Used as a commit-msg hook (the path of the message file is the only argument) and in CI
(``--range main..HEAD`` checks every commit in the range). The summary's type prefix is
checked separately by the conventional-pre-commit hook.
"""

import re
import subprocess
import sys

MAX_SUMMARY_LENGTH = 72

# Messages git writes by itself
EXEMPT_PREFIXES = ("Merge ", "Revert ", "fixup! ", "squash! ", "amend! ")

TRAILER = re.compile(r"^[A-Za-z][A-Za-z-]*: \S")


def problems(message: str) -> list[str]:
    """Return what is wrong with a commit message, or an empty list."""
    lines = []
    for line in message.splitlines():
        # Everything below the scissors line is the diff shown by `git commit -v`
        if line.startswith("# ------------------------ >8"):
            break
        if not line.startswith("#"):
            lines.append(line.rstrip())

    while lines and not lines[-1]:
        lines.pop()

    # Commits made by CI (version sync, changelog) carry [skip ci] and are not checked
    if not lines or lines[0].startswith(EXEMPT_PREFIXES) or "[skip ci]" in lines[0]:
        return []

    found = []
    summary = lines[0]

    if len(summary) > MAX_SUMMARY_LENGTH:
        found.append(f"the summary is {len(summary)} characters; the limit is {MAX_SUMMARY_LENGTH}")
    if summary.endswith("."):
        found.append("the summary ends with a period")
    if len(lines) > 1 and lines[1]:
        found.append("the second line must be blank")

    body = [line for line in lines[2:] if line and not TRAILER.match(line)]
    if not body:
        found.append("there is no body; say what changed and why in a sentence or more")

    return found


def main(argv: list[str]) -> int:
    """Check one message file, or every commit in a range."""
    if len(argv) == 2 and argv[0] == "--range":
        hashes = subprocess.run(
            ["git", "rev-list", "--no-merges", argv[1]], capture_output=True, text=True, check=True
        ).stdout.split()
        messages = {
            commit[:8]: subprocess.run(
                ["git", "log", "-1", "--format=%B", commit], capture_output=True, text=True, check=True
            ).stdout
            for commit in hashes
        }
    elif len(argv) == 1:
        with open(argv[0], encoding="utf-8") as f:
            messages = {"commit message": f.read()}
    else:
        print("usage: check_commit_message.py <message file> | --range <revision range>")
        return 2

    failed = False
    for name, message in messages.items():
        for problem in problems(message):
            failed = True
            print(f"{name}: {problem}")

    if failed:
        print("See the commit format section of CONTRIBUTING.md.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
