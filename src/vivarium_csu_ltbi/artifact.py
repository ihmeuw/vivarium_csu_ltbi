from loguru import logger

import gbd_mapping
import vivarium_inputs

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
    for loc in LOCATIONS:
        for id in [300, 934, 946, 947, 948, 949, 950, 954]:
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
    # NOTE: ID 954 fails for all countries -- no non-zero data!
    for loc in LOCATIONS:
        for id in [300, 934, 946, 947, 948, 949, 950]:
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling incidence data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'incidence', loc)

    # Remission
    for loc in LOCATIONS:
        for id in [9422]:  # Note this is a dismod id.
            entity = [c for c in gbd_mapping.causes if c.gbd_id == id][0]
            logger.info(f"Pulling remission data for id={id}, {entity.name} "
                        f"for {loc}")
            vivarium_inputs.get_measure(entity, 'remission', loc)


if __name__ == "__main__":
    build_cache()
