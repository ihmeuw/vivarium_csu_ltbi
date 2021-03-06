from loguru import logger

import gbd_mapping
import vivarium_inputs

from vivarium_csu_ltbi import globals as ltbi_globals

"""
Outstanding issues to discuss with yongquan et al at the next meeting:
LTBI remission -- need a value. currently placeholding one.
All-form active TB remission -- need to confirm value. Tried using a dismod id
                                yongquan dug up.
"""



def build_cache():
    """Ensure all data is pull-able and build a cache"""

    # Prevalence
    for id in [300, 934, 946, 947, 948, 949, 950, 954]:
        logger.info(f"Pulling prevalence data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in ltbi_globals.LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'prevalence', loc)

    # Cause-specific mortaltity rate
    for id in [300, 934, 946, 947, 948, 949, 950]:
        logger.info(f"Pulling CSMR data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in ltbi_globals.LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'cause_specific_mortality_rate', loc)

    # Incidence
    # NOTE: ID 954 fails for all locations -- no non-zero data!
    for id in [300, 934, 946, 947, 948, 949, 950]:
        logger.info(f"Pulling incidence data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in ltbi_globals.LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'incidence_rate', loc)

    # Remission
    from vivarium_gbd_access import gbd
    location_ids = gbd.get_location_ids()
    for loc in ltbi_globals.LOCATIONS:
        loc_id = location_ids.loc[location_ids.location_name == loc, 'location_id']
        logger.info(f"Pulling special remission data for dismod id=9422 for {loc}")
        gbd.get_modelable_entity_draws(9422, loc_id)


if __name__ == "__main__":
    build_cache()
