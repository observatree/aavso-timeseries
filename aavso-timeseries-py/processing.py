
import collections

TIMESERIES_THRESHOLD = 0.04166666666  # This is 1/24 (e.g., one hour)

Observation = collections.namedtuple("Observation", "unique_id julian_date_string julian_date")


def convert_julian_date_string(julian_date_string):
    return float(julian_date_string)  # almost all platforms map Python floats to IEEE-754 "double precision"

N0 = N1 = N2 = N3 = N4 = 0


def validated(observation):
    global N0, N1, N2, N3, N4
    N0 += 1

    if not observation.unique_id:
        N1 += 1
        return None

    if not observation.julian_date_string:
        N2 += 1
        return None

    julian_date = convert_julian_date_string(observation.julian_date_string)

    if not julian_date:
        N3 += 1
        return None

    return Observation(unique_id=observation.unique_id,
                       julian_date_string=None,
                       julian_date=julian_date)


def proximity_test(observation, last_observation):
    global N4
    if observation.julian_date == last_observation.julian_date:
        # This is just a duplicate observation! It is not our charge to flag duplicates as timeseries
        N4 += 1
        return False
    else:
        return observation.julian_date - last_observation.julian_date < TIMESERIES_THRESHOLD


# This routine does the actual work of identifying proximate observations.
# It presumes that the julian_date field is set within the observations and
# that the observations are already sorted ascending by julian_date.
def __identify_timeseries(observations: [Observation]):
    timeseries_dict = dict()
    last_observation = None

    proximate_observations = None

    for observation in observations:
        if last_observation:

            if proximity_test(observation, last_observation):
                if not proximate_observations:
                    proximate_observations = [last_observation, observation]
                else:
                    proximate_observations.append(observation)
            else:
                if proximate_observations:
                    timeseries = proximate_observations[0].unique_id
                    timeseries_dict[timeseries] = proximate_observations
                    proximate_observations = None

        last_observation = observation

    # Perhaps on the last observation a timeseries was still being added.
    if proximate_observations:
        timeseries = proximate_observations[0].unique_id
        timeseries_dict[timeseries] = proximate_observations

    return timeseries_dict


# It is presumed that identify_timeseries will be called with a list of records that are
# all from the same observer and the same star. Therefore all that identify_timeseries has
# to do is examine the Julian dates and look for proximity.
def identify_timeseries(observations: [Observation]):

    validated_observations = []

    for observation in observations:

        validated_observation = validated(observation)

        if not validated_observation:
            print("Invalid observation {}".format(observation))

        validated_observations.append(validated_observation)

    validated_observations.sort(key=lambda x: x.julian_date)

    return __identify_timeseries(validated_observations)
