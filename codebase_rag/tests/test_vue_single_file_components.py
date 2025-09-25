from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

from codebase_rag.graph_updater import GraphUpdater
from codebase_rag.parser_loader import load_parsers


def test_vue_single_file_component_scripts(
    temp_repo: Path, mock_ingestor: MagicMock
) -> None:
    """Ensure Vue SFC <script> contents are parsed using JS/TS logic."""

    project_path = temp_repo / "vue_project"
    project_path.mkdir()

    (project_path / "ComponentJs.vue").write_text(
        """
<template>
  <div>{{ message }}</div>
</template>

<script>
import { ref } from 'vue'

export function greet(name) {
  return `Hello, ${name}!`
}

export default {
  setup() {
    const message = ref(greet('World'))
    return { message }
  }
}
</script>
""".strip()
    )

    (project_path / "ComponentTs.vue").write_text(
        """
<template>
  <div>{{ total }}</div>
</template>

<script lang="ts">
export function add(a: number, b: number): number {
  return a + b
}
</script>

<script setup lang="ts">
import { computed } from 'vue'

const total = computed(() => add(2, 3))
</script>
""".strip()
    )

    parsers, queries = load_parsers()
    updater = GraphUpdater(
        ingestor=mock_ingestor,
        repo_path=project_path,
        parsers=parsers,
        queries=queries,
    )

    updater.run()

    project_name = project_path.name

    function_nodes = [
        call
        for call in mock_ingestor.ensure_node_batch.call_args_list
        if call.args and call.args[0] == "Function"
    ]

    qualified_names = {
        cast(dict[str, object], call.args[1])["qualified_name"]
        for call in function_nodes
    }

    assert f"{project_name}.ComponentJs.greet" in qualified_names
    assert f"{project_name}.ComponentTs.add" in qualified_names

    js_root, js_language = updater.ast_cache[project_path / "ComponentJs.vue"]
    ts_root, ts_language = updater.ast_cache[project_path / "ComponentTs.vue"]

    assert js_language == "javascript"
    assert ts_language == "typescript"
    assert js_root is not None
    assert ts_root is not None
