from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from codebase_rag.graph_updater import FunctionRegistryTrie, GraphUpdater
from codebase_rag.parser_loader import load_parsers
from codebase_rag.parsers.call_processor import CallProcessor


class InMemoryIngestor:
    """Minimal MemgraphIngestor replacement for integration-style tests."""

    def __init__(self) -> None:
        self.nodes: list[tuple[str, dict[str, object]]] = []
        self.relationships: list[tuple[tuple[str, str, str], str, tuple[str, str, str], dict | None]] = []
        self.pending_calls: list[dict[str, object]] = []
        self.fetch_queries: list[tuple[str, dict[str, object] | None]] = []

    def ensure_node_batch(self, label: str, properties: dict[str, object]) -> None:
        self.nodes.append((label, properties))

    def ensure_relationship_batch(
        self,
        from_spec: tuple[str, str, str],
        rel_type: str,
        to_spec: tuple[str, str, str],
        properties: dict | None = None,
    ) -> None:
        self.relationships.append((from_spec, rel_type, to_spec, properties))

    def flush_all(self) -> None:  # pragma: no cover - no-op for tests
        return

    def fetch_all(self, query: str, params: dict | None = None) -> list[dict[str, object]]:
        self.fetch_queries.append((query, params))
        allowed = set(params.get("allowed_labels", [])) if params else set()
        results: list[dict[str, object]] = []

        qualified_names: set[str] | None = None
        suffixes: set[str] | None = None

        if params:
            if "qualified_name" in params and isinstance(params["qualified_name"], str):
                qualified_names = {params["qualified_name"]}
            elif "qualified_names" in params and isinstance(
                params["qualified_names"], (list, tuple, set)
            ):
                qualified_names = {
                    name for name in params["qualified_names"] if isinstance(name, str)
                }

            if "suffix" in params and isinstance(params["suffix"], str):
                suffixes = {params["suffix"]}
            elif "suffixes" in params and isinstance(
                params["suffixes"], (list, tuple, set)
            ):
                suffixes = {
                    suffix for suffix in params["suffixes"] if isinstance(suffix, str)
                }

        for label, props in self.nodes:
            if allowed and label not in allowed:
                continue
            qualified_name = props.get("qualified_name")
            if not isinstance(qualified_name, str):
                continue
            if qualified_names is not None and qualified_name not in qualified_names:
                continue
            if suffixes is not None and not any(
                qualified_name.endswith(suffix) for suffix in suffixes
            ):
                continue
            results.append({"qualified_name": qualified_name, "labels": [label]})
        return results

    def execute_write(self, query: str, params: dict | None = None) -> None:  # pragma: no cover - unused
        return

    def record_pending_call(self, pending: dict[str, object]) -> None:
        if pending not in self.pending_calls:
            self.pending_calls.append(pending)

    def get_pending_calls(self) -> list[dict[str, object]]:
        return list(self.pending_calls)

    def replace_pending_calls(self, pending_calls: list[dict[str, object]]) -> None:
        self.pending_calls = list(pending_calls)


class DummyImportProcessor:
    """Simple stand-in for ImportProcessor used in call processor unit tests."""

    def __init__(self) -> None:
        self.import_mapping: dict[str, dict[str, str]] = {}


class DummyTypeInference:
    """Simple type inference stub that returns empty maps."""

    def build_local_variable_type_map(
        self, caller_node, module_qn: str, language: str
    ) -> dict[str, str]:  # pragma: no cover - trivial stub
        return {}


def make_call_processor(ingestor: InMemoryIngestor) -> CallProcessor:
    """Create a CallProcessor configured for unit tests."""

    function_registry = FunctionRegistryTrie()
    function_registry[
        "consumer.src.main.java.com.microsoft.app.App.App.run"
    ] = "Method"
    return CallProcessor(
        ingestor=ingestor,
        repo_path=Path("."),
        project_name="consumer",
        function_registry=function_registry,
        import_processor=DummyImportProcessor(),
        type_inference=DummyTypeInference(),
        class_inheritance={},
    )


def load_java_parsers_or_skip():
    """Load Java parsers or skip tests when unavailable."""

    try:
        parsers, queries = load_parsers()
    except RuntimeError as exc:
        pytest.skip(str(exc))

    if "java" not in parsers:
        pytest.skip("Java parser not available in this environment")

    return parsers, queries


def test_cross_project_calls_create_edges(temp_repo: Path) -> None:
    """Cross-project method calls should resolve to definitions from other projects."""

    parsers, queries = load_java_parsers_or_skip()
    ingestor = InMemoryIngestor()

    library_project = temp_repo / "library"
    library_src = library_project / "src/main/java/com/example/lib"
    library_src.mkdir(parents=True, exist_ok=True)
    (library_src / "LibraryClass.java").write_text(
        textwrap.dedent(
            """
            package com.example.lib;

            public class LibraryClass {
                public static String greet() {
                    return "hello";
                }
            }
            """
        ).strip()
    )

    library_updater = GraphUpdater(ingestor, library_project, parsers, queries)
    library_updater.run()

    consumer_project = temp_repo / "consumer"
    consumer_src = consumer_project / "src/main/java/com/microsoft/app"
    consumer_src.mkdir(parents=True, exist_ok=True)
    (consumer_src / "App.java").write_text(
        textwrap.dedent(
            """
            package com.example.app;

            import com.example.lib.LibraryClass;

            public class App {
                public String run() {
                    return LibraryClass.greet();
                }
            }
            """
        ).strip()
    )

    consumer_updater = GraphUpdater(ingestor, consumer_project, parsers, queries)
    consumer_updater.run()

    expected_caller = (
        "Method",
        "qualified_name",
        f"{consumer_project.name}.src.main.java.com.example.app.App.App.run",
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert any(
        rel[0] == expected_caller and rel[2][0] == "Method"
        and rel[2][2].startswith(
            f"{library_project.name}.src.main.java.com.example.lib.LibraryClass.LibraryClass.greet"
        )
        for rel in call_relationships
    ), "Expected CALLS relationship between consumer run() and library greet()"

def test_cross_project_calls_resolve_after_dependency(temp_repo: Path) -> None:
    """Cross-project calls should be created even if dependency is ingested later."""

    parsers, queries = load_java_parsers_or_skip()

    ingestor = InMemoryIngestor()

    consumer_project = temp_repo / "consumer"
    consumer_src = consumer_project / "src/main/java/com/microsoft/app"
    consumer_src.mkdir(parents=True, exist_ok=True)
    (consumer_src / "App.java").write_text(
        textwrap.dedent(
            """
            package com.microsoft.app;

            import com.microsoft.telemetry.TelemetryProvider;

            public class App {
                private final TelemetryProvider telemetryProvider;

                public App(TelemetryProvider telemetryProvider) {
                    this.telemetryProvider = telemetryProvider;
                }

                public void run() {
                    telemetryProvider.resolveCoordinate(null, 1, 2);
                }
            }
            """
        ).strip()
    )

    consumer_updater = GraphUpdater(ingestor, consumer_project, parsers, queries)
    consumer_updater.run()

    library_project = temp_repo / "microsoft-telemetry"
    library_src = library_project / "src/main/java/com/microsoft/telemetry"
    library_src.mkdir(parents=True, exist_ok=True)
    (library_src / "TelemetryProvider.java").write_text(
        textwrap.dedent(
            """
            package com.microsoft.telemetry;

            import com.microsoft.telemetry.dto.LocationDTO;

            public interface TelemetryProvider {
                LocationDTO resolveCoordinate(
                    LocationDTO locationDTO,
                    int observerSiteId,
                    int observerUnitId
                );
            }
            """
        ).strip()
    )

    dto_src = library_project / "src/main/java/com/microsoft/telemetry/dto"
    dto_src.mkdir(parents=True, exist_ok=True)
    (dto_src / "LocationDTO.java").write_text(
        textwrap.dedent(
            """
            package com.microsoft.telemetry.dto;

            public record LocationDTO(double lat, double lon) {}
            """
        ).strip()
    )

    library_updater = GraphUpdater(ingestor, library_project, parsers, queries)
    library_updater.run()

    expected_caller = (
        "Method",
        "qualified_name",
        f"{consumer_project.name}.src.main.java.com.microsoft.app.App.App.run",
    )

    telemetry_interface_suffix = (
        "microsoft.telemetry.TelemetryProvider.TelemetryProvider.resolveCoordinate"
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert any(
        rel[0] == expected_caller
        and rel[2][0] == "Method"
        and telemetry_interface_suffix in rel[2][2]
        for rel in call_relationships
    ), "Expected CALLS relationship after dependency ingestion"


def test_cross_project_calls_ignore_third_party(temp_repo: Path) -> None:
    """Cross-project Java calls should be ignored for non-first-party packages."""

    parsers, queries = load_java_parsers_or_skip()

    ingestor = InMemoryIngestor()

    consumer_project = temp_repo / "consumer"
    consumer_src = consumer_project / "src/main/java/com/microsoft/app"
    consumer_src.mkdir(parents=True, exist_ok=True)
    (consumer_src / "App.java").write_text(
        textwrap.dedent(
            """
            package com.microsoft.app;

            import io.fjord.telemetry.TelemetryProvider;

            public class App {
                private final TelemetryProvider telemetryProvider;

                public App(TelemetryProvider telemetryProvider) {
                    this.telemetryProvider = telemetryProvider;
                }

                public void run() {
                    telemetryProvider.resolveCoordinate(null, 1, 2);
                }
            }
            """
        ).strip()
    )

    GraphUpdater(ingestor, consumer_project, parsers, queries).run()

    library_project = temp_repo / "fjord-telemetry-adapter"
    library_src = library_project / "src/main/java/io/fjord/telemetry"
    library_src.mkdir(parents=True, exist_ok=True)
    (library_src / "TelemetryProvider.java").write_text(
        textwrap.dedent(
            """
            package io.fjord.telemetry;

            import io.fjord.telemetry.dto.LocationDTO;

            public interface TelemetryProvider {
                LocationDTO resolveCoordinate(
                    LocationDTO locationDTO,
                    int observerSiteId,
                    int observerUnitId
                );
            }
            """
        ).strip()
    )

    GraphUpdater(ingestor, library_project, parsers, queries).run()

    expected_caller = (
        "Method",
        "qualified_name",
        f"{consumer_project.name}.src.main.java.com.microsoft.app.App.App.run",
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert not any(
        rel[0] == expected_caller and rel[2][0] == "Method"
        for rel in call_relationships
    ), "Did not expect CALLS relationship for third-party package"


def test_cross_project_calls_with_fully_qualified_name(temp_repo: Path) -> None:
    """Calls using fully qualified class names should resolve across projects."""

    parsers, queries = load_java_parsers_or_skip()

    ingestor = InMemoryIngestor()

    library_project = temp_repo / "fq-library"
    library_src = library_project / "src/main/java/com/example/lib"
    library_src.mkdir(parents=True, exist_ok=True)
    (library_src / "LibraryClass.java").write_text(
        textwrap.dedent(
            """
            package com.example.lib;

            public class LibraryClass {
                public static String greet() {
                    return "hello";
                }
            }
            """
        ).strip()
    )

    GraphUpdater(ingestor, library_project, parsers, queries).run()

    consumer_project = temp_repo / "fq-consumer"
    consumer_src = consumer_project / "src/main/java/com/example/app"
    consumer_src.mkdir(parents=True, exist_ok=True)
    (consumer_src / "App.java").write_text(
        textwrap.dedent(
            """
            package com.example.app;

            public class App {
                public String run() {
                    return com.example.lib.LibraryClass.greet();
                }
            }
            """
        ).strip()
    )

    GraphUpdater(ingestor, consumer_project, parsers, queries).run()

    expected_caller = (
        "Method",
        "qualified_name",
        f"{consumer_project.name}.src.main.java.com.example.app.App.App.run",
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert any(
        rel[0] == expected_caller
        and rel[2][0] == "Method"
        and rel[2][2].startswith(
            f"{library_project.name}.src.main.java.com.example.lib.LibraryClass.LibraryClass.greet"
        )
        for rel in call_relationships
    ), "Expected CALLS relationship for fully qualified cross-project call"


def test_cross_project_lookup_batches_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exact and suffix lookups should batch candidates into a single DB call each."""

    ingestor = InMemoryIngestor()
    target_qn = (
        "library.src.main.java.com.microsoft.lib.LibraryClass"
        ".LibraryClass.greet"
    )
    helper_qn = (
        "library.src.main.java.com.microsoft.lib.LibraryHelper"
        ".LibraryHelper.help"
    )
    ingestor.ensure_node_batch("Method", {"qualified_name": target_qn})
    ingestor.ensure_node_batch("Method", {"qualified_name": helper_qn})

    call_processor = make_call_processor(ingestor)
    candidates = [
        "com.microsoft.lib.LibraryClass.LibraryClass.greet",
        "com.microsoft.lib.LibraryHelper.LibraryHelper.help",
    ]

    monkeypatch.setattr(
        call_processor,
        "_generate_cross_project_candidates",
        lambda _: list(candidates),
    )

    resolved = call_processor._lookup_cross_project_definition(  # pylint: disable=protected-access
        "com.microsoft.lib.LibraryClass.greet",
        "consumer.src.main.java.com.microsoft.app.App",
    )

    assert resolved == ("Method", target_qn)
    assert len(ingestor.fetch_queries) == 2
    _, first_params = ingestor.fetch_queries[0]
    assert "qualified_name" not in first_params
    assert sorted(first_params["qualified_names"]) == sorted(candidates)
    _, second_params = ingestor.fetch_queries[1]
    expected_suffixes = {
        f".{candidate}" if not candidate.startswith(".") else candidate
        for candidate in candidates
    }
    assert set(second_params["suffixes"]) == expected_suffixes


def test_cross_project_lookup_skips_mismatched_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-project lookups should avoid querying for mismatched package prefixes."""

    ingestor = InMemoryIngestor()
    call_processor = make_call_processor(ingestor)

    monkeypatch.setattr(
        call_processor,
        "_generate_cross_project_candidates",
        lambda _: [
            "io.fjord.telemetry.TelemetryProvider.TelemetryProvider.resolveCoordinate"
        ],
    )

    resolved = call_processor._lookup_cross_project_definition(  # pylint: disable=protected-access
        "io.fjord.telemetry.TelemetryProvider.resolveCoordinate",
        "consumer.src.main.java.com.microsoft.app.App",
    )

    assert resolved is None
    assert ingestor.fetch_queries == []


def test_pending_cross_project_skips_unparsed_callers(temp_repo: Path) -> None:
    """Pending cross-project calls are ignored when the caller was not parsed."""

    parsers, queries = load_java_parsers_or_skip()

    ingestor = InMemoryIngestor()

    library_project = temp_repo / "telemetry-lib"
    library_src = library_project / "src/main/java/com/microsoft/telemetry"
    library_src.mkdir(parents=True, exist_ok=True)
    (library_src / "TelemetryProvider.java").write_text(
        textwrap.dedent(
            """
            package com.microsoft.telemetry;

            public interface TelemetryProvider {
                void resolveCoordinate();
            }
            """
        ).strip()
    )

    ingestor.record_pending_call(
        {
            "caller_type": "Method",
            "caller_qn": "consumer.src.main.java.com.microsoft.app.App.App.run",
            "module_qn": "consumer.src.main.java.com.microsoft.app.App",
            "project_name": "consumer",
            "call_name": "TelemetryProvider.resolveCoordinate",
            "candidates": [
                "com.microsoft.telemetry.TelemetryProvider.TelemetryProvider.resolveCoordinate"
            ],
            "language": "java",
            "caller_was_parsed": False,
        }
    )

    GraphUpdater(ingestor, library_project, parsers, queries).run()

    expected_caller = (
        "Method",
        "qualified_name",
        "consumer.src.main.java.com.microsoft.app.App.App.run",
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert not any(rel[0] == expected_caller for rel in call_relationships)
    assert ingestor.pending_calls and ingestor.pending_calls[0]["caller_was_parsed"] is False

