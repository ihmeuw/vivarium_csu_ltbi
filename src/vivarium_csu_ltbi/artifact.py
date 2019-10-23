from loguru import logger

import gbd_mapping
import vivarium_inputs


LOCATIONS = ["India", "South Africa", "Philippines", "Ethiopia", "Brazil"]


def build_cache():
    """Ensure all data is pull-able and build a cache"""

    # Prevalence
    for loc in LOCATIONS:
        for id in [300, 634, 946, 947, 948, 949, 950, 954]:
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling prevalence data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'prevalence', loc)

    # Cause-specific mortaltity rate
    for loc in LOCATIONS:
        for id in [300, 934, 946, 947, 948, 949, 950]:
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling CSMR data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'cause_specific_mortality', loc)

    # Incidence
    for loc in LOCATIONS:
        for id in [300, 934, 946, 947, 948, 949, 950, 954]:
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling incidence data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'incidence', loc)

    # Remission
    for loc in LOCATIONS:
        for id in [954]:
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling remission data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'remission', loc)


if __name__ == "__main__":
    build_cache()
