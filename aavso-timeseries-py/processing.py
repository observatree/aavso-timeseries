
import collections


Observation = collections.namedtuple("Observation", "unique_id julian_date_string julian_date")


def convert_julian_date_string(julian_date_string):
    return float(julian_date_string)


def validated(observation):
    if not observation.unique_id or not observation.julian_date_string:
        return None

    julian_date = convert_julian_date_string(observation.julian_date_string)

    if not julian_date:
        return None

    return Observation(unique_id=observation.unique_id,
                       julian_date_string=None,
                       julian_date=julian_date)


# It is presumed that identify_timeseries will be called with a list of records that are
# all from the same observer and the same star. Therefore all that identify_timeseries has
# to do is examine the Julian dates and look for proximity.

def identify_timeseries(observations: [Observation]):

    print("{} observations".format(len(observations)))

    validated_observations = []

    for observation in observations:

        validated_observation = validated(observation)

        if not validated_observation:
            print("Invalid observation {}".format(observation))

        validated_observations.append(validated_observation)

    validated_observations.sort(key=lambda x: x.julian_date)

    return
