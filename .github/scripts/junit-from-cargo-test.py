#!/usr/bin/env python3
"""Turn `cargo test`'s human-readable output into JUnit XML, one file per test binary.

**Why a parser and not a test runner that emits JUnit itself.** `cargo-nextest` writes JUnit
natively and would be the obvious answer, but it runs every test in its own process, and this
suite's harness shares one database container across the tests that overlap in a *single*
process — a `static` holding a `Weak`, with each test holding an `Arc` (see
`crates/test-support/src/lib.rs`). Under a process-per-test runner that reference count can never
be greater than one, so every test would start and remove its own container: the harness's own
documented pathological case. libtest on stable has no JUnit or JSON formatter, so the output it
does produce is what there is to read.

Nothing new is captured to make this work. CI already tees the run to a log so the timings step
can read per-binary wall clock back out of it; this reads the same file.

Usage: junit-from-cargo-test.py <test-output.log> <output-directory>
"""

import os
import re
import sys
import xml.etree.ElementTree as ET

# `CARGO_TERM_COLOR: always` in CI, so the log is coloured even though nothing reading it is a
# terminal. Every pattern below would miss on a line wrapped in escapes.
ANSI = re.compile(r"\x1b\[[0-9;]*m")

# `Running unittests src/lib.rs (target/debug/deps/dbos-6370fae9bebcecf1)`, and `Doc-tests dbos`,
# which has no path at all. Both start a binary's section. The part before the parenthesis is
# one word for an integration test (`tests/queues.rs`) and two for a unit-test binary
# (`unittests src/lib.rs`), so it is matched loosely and only the path is kept.
RUNNING = re.compile(r"^\s+Running\s+.+\((?P<path>[^)]+)\)\s*$")
DOCTESTS = re.compile(r"^\s+Doc-tests\s+(?P<crate>\S+)\s*$")

# `test some::name ... ok`. Non-greedy, because a doc-test's name contains spaces:
# `test src/lib.rs - DBOS::launch (line 88) ... ok`. The outcome runs to end of line since
# `ignored` carries an optional reason after a comma.
TEST = re.compile(r"^test (?P<name>.+?) \.\.\. (?P<outcome>.+?)\s*$")
RESULT = re.compile(r"^test result: .*?finished in (?P<secs>[0-9.]+)s\s*$")

# `---- some::name stdout ----` opens the captured output of one failed test, in the block that
# follows the summary line. Both streams appear; either may carry the panic.
FAILURE_HEAD = re.compile(r"^---- (?P<name>.+?) (?:stdout|stderr) ----\s*$")


class Suite:
    """One test binary's worth of results."""

    def __init__(self, name):
        self.name = name
        self.cases = []          # (name, outcome) in the order libtest reported them
        self.failures = {}       # test name -> captured output
        self.seconds = 0.0

    def add_failure_text(self, name, text):
        # Two blocks per test when it wrote to both streams; keep both rather than the last.
        joined = "\n".join(text).strip()
        if not joined:
            return
        self.failures[name] = (self.failures.get(name, "") + "\n" + joined).strip()


def parse(lines):
    suites, suite = [], None
    failing_name, failing_text = None, []

    def close_failure():
        nonlocal failing_name, failing_text
        if suite is not None and failing_name is not None:
            suite.add_failure_text(failing_name, failing_text)
        failing_name, failing_text = None, []

    for raw in lines:
        line = ANSI.sub("", raw.rstrip("\n"))

        running = RUNNING.match(line)
        doctests = DOCTESTS.match(line)
        if running or doctests:
            close_failure()
            if running:
                # `target/debug/deps/queues-618214f6e36173f5` -> `queues`. The hash changes on
                # every rebuild, so a suite named with it would never match run to run.
                name = re.sub(r"-[0-9a-f]{6,}$", "", os.path.basename(running.group("path")))
            else:
                name = f"doc-tests-{doctests.group('crate')}"
            suite = Suite(name)
            suites.append(suite)
            continue

        if suite is None:
            continue

        head = FAILURE_HEAD.match(line)
        if head:
            close_failure()
            failing_name = head.group("name")
            continue

        if failing_name is not None:
            # The captured block ends at the second `failures:` — the list of names libtest
            # prints after the detail — or at the result line.
            if line.strip() == "failures:" or RESULT.match(line):
                close_failure()
            else:
                failing_text.append(line)
                continue

        test = TEST.match(line)
        if test:
            suite.cases.append((test.group("name"), test.group("outcome")))
            continue

        result = RESULT.match(line)
        if result:
            close_failure()
            suite.seconds = float(result.group("secs"))
            suite = None

    close_failure()
    return [s for s in suites if s.cases]


def to_xml(suite):
    failed = sum(1 for _, outcome in suite.cases if outcome.startswith("FAILED"))
    skipped = sum(1 for _, outcome in suite.cases if outcome.startswith("ignored"))
    suites = ET.Element("testsuites")
    element = ET.SubElement(
        suites,
        "testsuite",
        name=suite.name,
        tests=str(len(suite.cases)),
        failures=str(failed),
        errors="0",
        skipped=str(skipped),
        time=f"{suite.seconds:.3f}",
    )
    for name, outcome in suite.cases:
        # libtest reports no per-test duration, so every case carries the suite's total rather
        # than a fabricated share of it. The timings step is where time is actually accounted.
        case = ET.SubElement(element, "testcase", name=name, classname=suite.name, time="0")
        if outcome.startswith("FAILED"):
            failure = ET.SubElement(case, "failure", message="test failed")
            failure.text = suite.failures.get(name, "no captured output")
        elif outcome.startswith("ignored"):
            _, _, reason = outcome.partition(", ")
            ET.SubElement(case, "skipped", message=reason or "ignored")
    return ET.ElementTree(suites)


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} <test-output.log> <output-directory>")
    log, out_dir = sys.argv[1], sys.argv[2]
    if not os.path.exists(log) or os.path.getsize(log) == 0:
        print("No test output to convert.")
        return
    with open(log, encoding="utf-8", errors="replace") as handle:
        suites = parse(handle)
    os.makedirs(out_dir, exist_ok=True)
    for suite in suites:
        to_xml(suite).write(
            os.path.join(out_dir, f"{suite.name}.xml"), encoding="unicode", xml_declaration=True
        )
    total = sum(len(s.cases) for s in suites)
    failed = sum(1 for s in suites for _, o in s.cases if o.startswith("FAILED"))
    print(f"Wrote {len(suites)} suite(s), {total} test(s), {failed} failure(s) to {out_dir}.")


if __name__ == "__main__":
    main()
