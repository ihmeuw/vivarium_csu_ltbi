"""
Make model specifications

click application that takes a template model specification file
and locations for which to create model specs and uses jinja2 to
render model specs with the correct location parameters plugged in.

It will look for the model spec template in "model_spec.in" in the directory
``src/vivarium_csu_ltbi/model_specifications``.
Specify multiple locations in a file called "locations.txt" in the same
directory. You can create a single location using the "-s" option. Using the
single location option will override an existing locations file.

The application will look for the model spec and locations files based
on the python environment that is active and these files don't need
to be specified if the default names and location are used.
"""
from collections import namedtuple
from pathlib import Path
import re
from typing import List, Optional, Iterable

import click
from jinja2 import Template
from loguru import logger

from vivarium_csu_ltbi.tools import results
from vivarium_csu_ltbi import globals as project_globals, paths


MODEL_SPEC_DIR = (Path(__file__).parent.parent / 'model_specifications').resolve()
Location = namedtuple('Location', ['proper', 'sanitized'])


def sanitize(*locations: str) -> Iterable[Location]:
    """Processes locations into tuples of proper and sanitized names.

    Sanitized location strings are all lower case, have spaces replaced
    by underscores, and have apostrophes replaced by dashes.

    Parameters
    ----------
    locations
        The locations to process formatted as proper location names.
        Proper location names should come from GBD location set 1, the
        location reporting hierarchy wherever possible. If using sub-national
        locations, they may be found in GBD location set 35, the model
        results location hierarchy.

    Yields
    -------
        Named tuples with both the proper and sanitized location names

    Examples
    --------
    >>> sanitize("Nigeria")
    Location(proper="Nigeria", sanitized="nigeria")

    >>> sanitize("Burkina Faso")
    Location(proper="Burkina Faso", sanitized="burkina_faso")

    >>> sanitize("Cote d'Ivoire")
    Location(proper="Cote d'Ivoire", sanitized="cote_d-ivoire")

    """
    # TODO: Check that they're in a call to get_location_id from
    #    vivarium_gbd_access.gbd.  Not doing this now because it's not
    #    specified as a proper dependency in the setup.py and would be a
    #    bit of a pain to do now.
    for location in locations:
        proper = location.strip()
        sanitized = re.sub("[- ]", '_', proper).lower()
        sanitized = sanitized.replace("'", '-')
        yield Location(proper, sanitized)


def parse_locations(locations_file: Optional[str], single_location: Optional[str]) -> List[str]:
    """Parses location inputs into a list of location strings.

    If no arguments are provided, this will default to the ``locations.txt``
    file located in the repository model_specifications directory.

    Parameters
    ----------
    locations_file
        Path to a file containing a list of locations to generate model
        specifications for.
    single_location
        Optional single location to generate a model specification for.

    Returns
    -------
        A list of location strings.

    Raises
    ------
    ValueError
        If both ``locations_file`` and ``single_loc`` are provided or if
        a ``locations_file`` with no locations is provided.

    """
    if locations_file and single_location:
        raise ValueError('You provided both a locations file and a single location to make_specs.')

    if single_location:
        return [single_location]

    locations_file = Path(locations_file) if locations_file else MODEL_SPEC_DIR / 'locations.txt'
    with locations_file.open() as f:
        # Interpret each line that doesn't start with a '#' as a single location.
        locations = [l for l in f.readlines() if not l.strip().startswith('#') and l.strip()]
    if not locations:
        raise ValueError(f'No locations provided in location file {str(locations_file)}.')

    return locations


@click.command()
@click.option('-l', '--locations-file',
              type=click.Path(dir_okay=False),
              help=('The file with the location parameters for the template. If no locations file is provided '
                    'and no single location is provided with the ``-s`` flag, this will default to the locations '
                    f'file located at {str(MODEL_SPEC_DIR / "locations.txt")}.'))
@click.option('-t', '--template',
              default=str(MODEL_SPEC_DIR / 'model_spec.in'),
              show_default=True,
              type=click.Path(exists=True, dir_okay=False),
              help='The model specification template file.')
@click.option('-s', '--single-location',
              default='',
              help='Specify a single location name.')
@click.option('-o', '--output-dir',
              default=str(MODEL_SPEC_DIR),
              show_default=True,
              type=click.Path(exists=True, dir_okay=True),
              help='Specify an output directory. Directory must exist.')
def make_specs(template: str, locations_file: str, single_location: str, output_dir: str) -> None:
    """Generate model specifications based on a template.

    The default template lives here:

    ``vivarium_csu_ltbi/src/vivarium_csu_ltbi/model_specification/model_spec.in``

    Supply the locations for which you want a model spec generated by filling
    in the empty 'locations.txt' file. A template for this file can be found at

    ``vivarium_csu_ltbi/src/vivarium_csu_ltbi/model_specification/locations.txt``

    with instructions for it's use.

    """
    template = Path(template)
    output_dir = Path(output_dir)
    locations = parse_locations(locations_file, single_location)

    with template.open('r') as infile:
        jinja_temp = Template(infile.read())

    logger.info(f'Writing model spec(s) to "{output_dir}"')

    for location in sanitize(*locations):
        filespec = output_dir / f'{location.sanitized}.yaml'
        with filespec.open('w+') as outfile:
            logger.info(f'   Writing {filespec.name}')
            outfile.write(jinja_temp.render(
                location_proper=location.proper,
                location_sanitized=location.sanitized,
                artifact_root=paths.ARTIFACT_ROOT
            ))


@click.command()
@click.argument('model_versions', nargs=-1, type=click.STRING, required=True)
@click.argument('location', type=click.Choice(project_globals.LOCATIONS), required=True)
@click.option('-p', '--preceding-results', type=click.INT, default=0)
@click.option('-o', '--output-path', type=click.Path(exists=True, dir_okay=True))
def make_results(model_versions, location, preceding_results, output_path):
    """Generate count-space measure information and final outputs tables in
    *.hdf and *.csv format. In the event of unfinished results, draws deficient
    in random seeds or scenarios are excluded from the analysis.

    The results to be processed are the most recent outputs from the run defined
    by MODEL_VERSIONS and LOCATION. MODEL_VERSIONS is 1 or 2 arguments which are
    names of model runs as found in the root results directory. If two are
    passed, the model results are summed before processing. The option
    PRECEDING_RESULTS defines the results to be processed counting backwards
    from the most recent results. The processed results are saved in OUTPUT_PATH
    if specified, otherwise the current working directory.

    """
    results.process_latest_results(model_versions, location, preceding_results,
                                   output_path)


@click.command()
@click.argument('model_output_paths', nargs=-1, type=click.Path(exists=True))
@click.option('-o', '--output-path', type=click.Path(exists=True, dir_okay=True))
def make_specific_results(model_output_paths, output_path=None):
    """Generate count-space measure information and final outputs tables in
    *.hdf and *.csv format. In the event of unfinished results, draws deficient
    in random seeds or scenarios are excluded from the analysis.

    The results to be processed are those found in MODEL_OUTPUT_PATHS. This
    should be one or two paths to model outputs, e.g. the directory containing
    output.hdf If two are passed, the model results are summed before
    processing.The processed results are saved in OUTPUT_PATH if specified,
    otherwise the current working directory.

    This is not currently implemented.
    """
    raise NotImplementedError
