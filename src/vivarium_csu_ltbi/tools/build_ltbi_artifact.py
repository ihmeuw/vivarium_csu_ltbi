import click
from loguru import logger
from vivarium.framework.utilities import handle_exceptions

from . import builder

PROJECT_NAME = 'vivarium_csu_ltbi'


@click.command()
@click.option('-l', '--location',
              required=True,
              type=click.Path(dir_okay=False),
              help=('The location for which to build an artifact'))
@click.option('-o', '--output-dir',
              default=f'/share/costeffectiveness/artifacts/{PROJECT_NAME}/',
              show_default=True,
              type=click.Path(exists=True, dir_okay=True),
              help='Specify an output directory. Directory must exist.')
def build_artifact(location: str, output_dir: str) -> None:
    """Build an artifact for the provided location
    """
    main = handle_exceptions(builder.build_ltbi_artifact, logger, with_debugger=True)
    main(location, output_dir)

