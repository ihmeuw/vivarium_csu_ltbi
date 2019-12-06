import click
from loguru import logger

from vivarium.framework.utilities import handle_exceptions

from vivarium_csu_ltbi.tools import builder
from vivarium_csu_ltbi import globals as ltbi_globals
from vivarium_csu_ltbi import paths as ltbi_paths


@click.command()
@click.option('-l', '--location',
              required=True,
              type=click.Choice(ltbi_globals.LOCATIONS),
              help='The location for which to build an artifact')
@click.option('-o', '--output-dir',
              default=str(ltbi_paths.ARTIFACT_ROOT),
              show_default=True,
              type=click.Path(exists=True, dir_okay=True),
              help='Specify an output directory. Directory must exist.')
def build_artifact(location: str, output_dir: str) -> None:
    """Build an artifact for the provided location
    """
    main = handle_exceptions(builder.build_ltbi_artifact, logger, with_debugger=True)
    main(location, output_dir)

