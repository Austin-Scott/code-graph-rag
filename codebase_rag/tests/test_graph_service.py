from codebase_rag.services.graph_service import MemgraphIngestor


def test_memgraph_ingestor_defines_enum_and_interface_constraints() -> None:
    ingestor = MemgraphIngestor(host="localhost", port=7687)

    assert ingestor.unique_constraints["Enum"] == "qualified_name"
    assert ingestor.unique_constraints["Interface"] == "qualified_name"


def test_flush_nodes_uses_constraints_for_enum() -> None:
    ingestor = MemgraphIngestor(host="localhost", port=7687)
    executed: list[tuple[str, list[dict[str, str]]]] = []
    ingestor._execute_batch = lambda query, params_list: executed.append((query, params_list))  # type: ignore[attr-defined]

    ingestor.ensure_node_batch(
        "Enum",
        {"qualified_name": "project.module.MyEnum", "docstring": "Example"},
    )

    ingestor.flush_nodes()

    assert executed, "Expected flush_nodes to execute a batch for Enum nodes"
    query, params_list = executed[0]
    assert "MERGE (n:Enum" in query
    assert params_list == [
        {"qualified_name": "project.module.MyEnum", "docstring": "Example"}
    ]


def test_flush_nodes_uses_constraints_for_interface() -> None:
    ingestor = MemgraphIngestor(host="localhost", port=7687)
    executed: list[tuple[str, list[dict[str, str]]]] = []
    ingestor._execute_batch = lambda query, params_list: executed.append((query, params_list))  # type: ignore[attr-defined]

    ingestor.ensure_node_batch(
        "Interface",
        {"qualified_name": "project.module.MyInterface", "docstring": "Example"},
    )

    ingestor.flush_nodes()

    assert executed, "Expected flush_nodes to execute a batch for Interface nodes"
    query, params_list = executed[0]
    assert "MERGE (n:Interface" in query
    assert params_list == [
        {"qualified_name": "project.module.MyInterface", "docstring": "Example"}
    ]
