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

from logger import get_logger

logger = get_logger(__name__)

# Pattern that matches {{ ref('name') }} or {{ ref("name") }} with optional whitespace
_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")

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
        self._adapter = self._create_adapter(project_dir, target)
        self._ref_map: Optional[Dict[str, str]] = None

    # ------------------------------------------------------------------
    # Adapter bootstrap
    # ------------------------------------------------------------------

    @staticmethod
    def _create_adapter(project_dir: str, target: str) -> Any:
        from argparse import Namespace

        from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
        from dbt.config.runtime import RuntimeConfig
        from dbt.flags import set_from_args

        args = Namespace(
            project_dir=project_dir,
            profiles_dir=os.path.expanduser("~/.dbt"),
            target=target,
            threads=1,
            vars={},
            profile=None,
            PROFILES_DIR=os.path.expanduser("~/.dbt"),
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

    def _load_ref_map(self) -> Dict[str, str]:
        """Build a ``{model_name: relation_name}`` map from the dbt manifest."""
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

        # Also include sources (some queries reference source tables)
        for source in manifest.get("sources", {}).values():
            relation_name = source.get("relation_name")
            name = source.get("name")
            if relation_name and name:
                ref_map[name] = relation_name

        return ref_map

    def resolve_refs(self, query: str) -> str:
        """Replace ``{{ ref('name') }}`` with the fully-qualified relation name."""
        if self._ref_map is None:
            self._ref_map = self._load_ref_map()

        def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
            name = match.group(1)
            if name not in self._ref_map:
                raise ValueError(
                    f"Cannot resolve ref('{name}'): not found in dbt manifest.  "
                    f"Known models: {sorted(self._ref_map)!r}"
                )
            return self._ref_map[name]

        return _REF_PATTERN.sub(_replace, query)

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    @staticmethod
    def has_non_ref_jinja(query: str) -> bool:
        """Return True if *query* contains Jinja expressions other than ``{{ ref(...) }}``."""
        stripped = _REF_PATTERN.sub("", query)
        return bool(_JINJA_EXPR_PATTERN.search(stripped))

    def run_query(self, prerendered_query: str) -> List[Dict[str, Any]]:
        """Render Jinja refs and execute a query, returning rows as dicts.

        Column names are lower-cased and values are serialised to match the
        behaviour of ``elementary.agate_to_dicts``.

        If the query contains Jinja expressions beyond simple ``{{ ref() }}``
        calls (e.g. ``{{ elementary.missing_count(...) }}``), this method
        raises ``ValueError`` so the caller can fall back to
        ``run_operation`` which handles full Jinja rendering.
        """
        if self.has_non_ref_jinja(prerendered_query):
            raise ValueError(
                "Query contains Jinja expressions that cannot be resolved "
                "from the manifest alone (only {{ ref() }} is supported)."
            )

        sql = self.resolve_refs(prerendered_query)
        with self._adapter.connection_named("run_query"):
            _response, table = self._adapter.execute(sql, fetch=True)

        # Convert agate Table → list[dict] matching agate_to_dicts behaviour
        columns = [c.lower() for c in table.column_names]
        return [
            {col: _serialize_value(val) for col, val in zip(columns, row)}
            for row in table
        ]
