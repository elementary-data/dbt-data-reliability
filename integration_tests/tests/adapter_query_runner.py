"""Direct database query execution via dbt adapter connection.

Bypasses ``run_operation`` log-parsing entirely so that query results are
never lost due to intermittent log-capture issues in the CLI / fusion
runners.
"""

import json
import multiprocessing
import os
import re
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt.adapters.base import BaseAdapter
from logger import get_logger

logger = get_logger(__name__)


class UnsupportedJinjaError(Exception):
    """Raised when a query contains Jinja expressions beyond ref()/source()."""

    def __init__(self, query: str) -> None:
        self.query = query
        super().__init__(
            "Query contains Jinja expressions beyond {{ ref() }} / {{ source() }} "
            "which cannot be executed via the direct adapter path. "
            "Use the run_operation fallback instead."
        )


# Pattern that matches {{ ref('name') }} or {{ ref("name") }} with optional whitespace
_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")

# Pattern that matches {{ source('source_name', 'table_name') }}
_SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
)

# Pattern that matches any Jinja expression {{ ... }}
_JINJA_EXPR_PATTERN = re.compile(r"\{\{.*?\}\}")


def _serialize_value(val: Any) -> Any:
    """Mimic elementary's ``agate_to_dicts`` serialisation.

    * ``Decimal`` → ``int`` (no fractional part) or ``float``
    * ``datetime`` / ``date`` / ``time`` → ISO-format string
    * Everything else is returned unchanged.
    """
    if isinstance(val, Decimal):
        # Match the Jinja macro: normalize, then int or float
        normalized = val.normalize()
        if normalized.as_tuple().exponent >= 0:
            return int(normalized)
        return float(normalized)
    if isinstance(val, (datetime, date, time)):
        return val.isoformat()
    return val


class AdapterQueryRunner:
    """Execute SQL directly through a dbt adapter connection.

    Parameters
    ----------
    project_dir : str
        Path to the dbt project directory.
    target : str
        Name of the dbt target / profile output to use.
    """

    def __init__(self, project_dir: str, target: str) -> None:
        self._project_dir = project_dir
        self._target = target
        self._adapter: BaseAdapter = self._create_adapter(project_dir, target)
        self._ref_map: Optional[Dict[str, str]] = None
        self._source_map: Optional[Dict[tuple, str]] = None

    # ------------------------------------------------------------------
    # Adapter bootstrap
    # ------------------------------------------------------------------

    @staticmethod
    def _create_adapter(project_dir: str, target: str) -> BaseAdapter:
        from argparse import Namespace

        from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
        from dbt.config.runtime import RuntimeConfig
        from dbt.flags import set_from_args

        profiles_dir = os.environ.get("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt"))
        args = Namespace(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target,
            threads=1,
            vars={},
            profile=None,
            PROFILES_DIR=profiles_dir,
            PROJECT_DIR=project_dir,
        )
        set_from_args(args, None)
        config = RuntimeConfig.from_args(args)

        reset_adapters()
        mp_context = multiprocessing.get_context("spawn")
        register_adapter(config, mp_context)
        return get_adapter(config)

    # ------------------------------------------------------------------
    # Ref resolution
    # ------------------------------------------------------------------

    def _load_manifest_maps(self) -> None:
        """Load ref and source maps from the dbt manifest."""
        manifest_path = Path(self._project_dir) / "target" / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Manifest not found at {manifest_path}.  "
                "Run `dbt run` or `dbt compile` first."
            )
        with open(manifest_path) as fh:
            manifest = json.load(fh)

        ref_map: Dict[str, str] = {}
        for node in manifest.get("nodes", {}).values():
            relation_name = node.get("relation_name")
            name = node.get("name")
            if relation_name and name:
                ref_map[name] = relation_name

        source_map: Dict[tuple, str] = {}
        for source in manifest.get("sources", {}).values():
            relation_name = source.get("relation_name")
            name = source.get("name")
            source_name = source.get("source_name")
            if relation_name and source_name and name:
                source_map[(source_name, name)] = relation_name
                # Also register source tables by name for simple ref() lookups
                ref_map.setdefault(name, relation_name)

        self._ref_map = ref_map
        self._source_map = source_map

    def _ensure_maps_loaded(self) -> None:
        """Lazily load manifest maps on first use."""
        if self._ref_map is None:
            self._load_manifest_maps()

    def resolve_refs(self, query: str) -> str:
        """Replace ``{{ ref('name') }}`` and ``{{ source('x','y') }}`` with relation names."""
        self._ensure_maps_loaded()
        assert self._ref_map is not None
        assert self._source_map is not None

        def _replace_ref(match: re.Match) -> str:  # type: ignore[type-arg]
            name = match.group(1)
            if name not in self._ref_map:
                # Manifest may have changed (temp models/seeds); reload once.
                self._load_manifest_maps()
                assert self._ref_map is not None
                if name not in self._ref_map:
                    raise ValueError(
                        f"Cannot resolve ref('{name}'): not found in dbt manifest."
                    )
            return self._ref_map[name]

        def _replace_source(match: re.Match) -> str:  # type: ignore[type-arg]
            source_name, table_name = match.group(1), match.group(2)
            key = (source_name, table_name)
            if self._source_map is None or key not in self._source_map:
                self._load_manifest_maps()
                assert self._source_map is not None
                if key not in self._source_map:
                    raise ValueError(
                        f"Cannot resolve source('{source_name}', '{table_name}'): "
                        "not found in dbt manifest."
                    )
            return self._source_map[key]

        query = _REF_PATTERN.sub(_replace_ref, query)
        query = _SOURCE_PATTERN.sub(_replace_source, query)
        return query

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    @staticmethod
    def has_non_ref_jinja(query: str) -> bool:
        """Return True if *query* contains Jinja beyond ``{{ ref() }}`` / ``{{ source() }}``."""
        stripped = _REF_PATTERN.sub("", query)
        stripped = _SOURCE_PATTERN.sub("", stripped)
        return bool(_JINJA_EXPR_PATTERN.search(stripped))

    def run_query(self, prerendered_query: str) -> List[Dict[str, Any]]:
        """Render Jinja refs/sources and execute a query, returning rows as dicts.

        Column names are lower-cased and values are serialised to match the
        behaviour of ``elementary.agate_to_dicts``.

        Only ``{{ ref() }}`` and ``{{ source() }}`` Jinja expressions are
        supported.  Raises ``UnsupportedJinjaError`` if the query contains
        other Jinja expressions.
        """
        if self.has_non_ref_jinja(prerendered_query):
            raise UnsupportedJinjaError(prerendered_query)
        sql = self.resolve_refs(prerendered_query)
        with self._adapter.connection_named("run_query"):
            _response, table = self._adapter.execute(sql, fetch=True)

        # Convert agate Table → list[dict] matching agate_to_dicts behaviour
        columns = [c.lower() for c in table.column_names]
        return [
            {col: _serialize_value(val) for col, val in zip(columns, row)}
            for row in table
        ]
