from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from codebase_rag.graph_updater import GraphUpdater
from codebase_rag.parser_loader import load_parsers


class InMemoryIngestor:
    """Minimal MemgraphIngestor replacement for integration-style tests."""

    def __init__(self) -> None:
        self.nodes: list[tuple[str, dict[str, object]]] = []
        self.relationships: list[tuple[tuple[str, str, str], str, tuple[str, str, str], dict | None]] = []
        self.pending_calls: list[dict[str, object]] = []

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
        allowed = set(params.get("allowed_labels", [])) if params else set()
        results: list[dict[str, object]] = []
        for label, props in self.nodes:
            if allowed and label not in allowed:
                continue
            qualified_name = props.get("qualified_name")
            if isinstance(qualified_name, str):
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


def test_cross_project_calls_create_edges(temp_repo: Path) -> None:
    """Cross-project method calls should resolve to definitions from other projects."""

    parsers, queries = load_parsers()
    if "java" not in parsers:
        pytest.skip("Java parser not available in this environment")
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

    parsers, queries = load_parsers()
    if "java" not in parsers:
        pytest.skip("Java parser not available in this environment")

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

    parsers, queries = load_parsers()
    if "java" not in parsers:
        pytest.skip("Java parser not available in this environment")

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

    parsers, queries = load_parsers()
    if "java" not in parsers:
        pytest.skip("Java parser not available in this environment")

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

