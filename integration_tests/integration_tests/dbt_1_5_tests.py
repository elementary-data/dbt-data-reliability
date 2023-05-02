from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest

# use 'parse' command to load a Manifest
res: dbtRunnerResult = dbtRunner().invoke(["parse", "--project-dir", dbt_project_dir, "--target", dbt_target])
manifest: Manifest = res.result
