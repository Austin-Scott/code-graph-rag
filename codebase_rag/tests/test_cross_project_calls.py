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
    consumer_src = consumer_project / "src/main/java/com/example/app"
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
        f"{consumer_project.name}.src.main.java.com.example.app.App.App.run()",
    )
    expected_callee = (
        "Method",
        "qualified_name",
        f"{library_project.name}.src.main.java.com.example.lib.LibraryClass.LibraryClass.greet()",
    )

    call_relationships = [
        rel for rel in ingestor.relationships if rel[1] == "CALLS"
    ]

    assert any(
        rel[0] == expected_caller and rel[2] == expected_callee
        for rel in call_relationships
    ), "Expected CALLS relationship between consumer run() and library greet()"
