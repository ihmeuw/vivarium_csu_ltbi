from loguru import logger

import gbd_mapping
import vivarium_inputs
import vivarium_gbd_access.gbd as gbd

"""
Outstanding issues to discuss with yongquan et al at the next meeting:
LTBI remission -- need a value. currently placeholding one.
All-form active TB remission -- need to confirm value. Tried using a dismod id
                                yongquan dug up.
"""

LOCATIONS = ["India", "South Africa", "Philippines", "Ethiopia", "Brazil"]


def build_cache():
    """Ensure all data is pull-able and build a cache"""

    # Prevalence
    for id in [300, 934, 946, 947, 948, 949, 950, 954]:
        logger.info(f"Pulling prevalence data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'prevalence', loc)

    # Cause-specific mortaltity rate
    for id in [300, 934, 946, 947, 948, 949, 950]:
        logger.info(f"Pulling CSMR data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'cause_specific_mortality_rate', loc)

    # Incidence
    # NOTE: ID 954 fails for all countries -- no non-zero data!
    for id in [300, 934, 946, 947, 948, 949, 950]:
        logger.info(f"Pulling incidence data for id={id}")
        entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
        logger.info(f"Entity found for id={id}: {entity.name}")
        for loc in LOCATIONS:
            logger.info(f"Pulling for {loc}")
            vivarium_inputs.get_measure(entity, 'incidence', loc)

    # Remission
    location_ids = gbd.get_location_ids()
    for loc in LOCATIONS:
        loc_id = location_ids.loc[location_ids.location_name == loc, 'location_id']
        logger.info(f"Pulling special remission data for dismod id=9422 for {loc}")
        gbd.get_modelable_entity_draws(9422, loc_id)


if __name__ == "__main__":
    build_cache()
