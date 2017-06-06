
import collections

# A time series must have at least 2 observations at a cadence period of at most 12 hours.

EXCLUDED_TIMESERIES_LENGTH = 1  # Strict comparison. 1 means a timeseries of only 1 frame isn't a timeseries.
EXCLUDED_TIMESERIES_DURATION_DAYS = 0.0  # Strict comparison. 0.0 means a timeseries must have some duration.
EXCLUDED_CADENCE_MINUTES = 720.0  # Strict comparison. 720.0 means the cadence must be less than 12 hours.

Observation = collections.namedtuple("Observation", "unique_id name obscode julian_date_string julian_date")


def convert_julian_date_string(julian_date_string):
    return float(julian_date_string)  # almost all platforms map Python floats to IEEE-754 "double precision"


def increment_invalid(reason, validation_dict):
    if reason not in validation_dict:
        validation_dict[reason] = 1
    else:
        validation_dict[reason] = validation_dict[reason] + 1


def validated(observation, validation_dict):

    if not observation.unique_id:
        increment_invalid("invalid-missing_unique_id", validation_dict)
        return None

    if not observation.julian_date_string:
        increment_invalid("invalid-missing_jd_string", validation_dict)
        return None

    julian_date = convert_julian_date_string(observation.julian_date_string)

    if not julian_date:
        increment_invalid("invalid-failed_jd_conversion", validation_dict)
        return None

    return Observation(unique_id=observation.unique_id,
                       name=observation.name,
                       obscode=observation.obscode,
                       julian_date_string=None,
                       julian_date=julian_date)


def proximity_test(observation, last_observation, validation_dict):
    if observation.julian_date == last_observation.julian_date:
        increment_invalid("invalid-duplicate_jd", validation_dict)
        # return True anyway
        return True

    return 1440.0 * (observation.julian_date - last_observation.julian_date) < EXCLUDED_CADENCE_MINUTES


def duration_days(observations: [Observation]):
    first_observation = observations[0]
    last_observation = observations[-1]
    return last_observation.julian_date - first_observation.julian_date


def __conditionally_add(proximate_observations: [Observation], timeseries_dict):
    if proximate_observations:
        timeseries_length = len(proximate_observations)
        if timeseries_length > EXCLUDED_TIMESERIES_LENGTH:
            if duration_days(proximate_observations) > EXCLUDED_TIMESERIES_DURATION_DAYS:
                timeseries = proximate_observations[0].unique_id
                timeseries_dict[timeseries] = proximate_observations


# This routine does the actual work of identifying proximate observations.
# It presumes that the julian_date field is set within the observations and
# that the observations are already sorted ascending by julian_date.
def __identify_timeseries(observations: [Observation], validation_dict):
    timeseries_dict = dict()
    last_observation = None

    proximate_observations = None

    for observation in observations:
        if last_observation:

            if proximity_test(observation, last_observation, validation_dict):
                if not proximate_observations:
                    proximate_observations = [last_observation, observation]
                else:
                    proximate_observations.append(observation)
            else:
                __conditionally_add(proximate_observations, timeseries_dict)
                proximate_observations = None

        last_observation = observation

    # Perhaps on the last observation a timeseries was still being added.
    __conditionally_add(proximate_observations, timeseries_dict)

    return timeseries_dict


# It is presumed that identify_timeseries will be called with a list of records that
# are all from the same observer, the same star. Therefore all that identify_timeseries
# has to do is examine the Julian dates and look for proximity.
def identify_timeseries(observations: [Observation], validation_dict):

    validated_observations = []

    for observation in observations:

        validated_observation = validated(observation, validation_dict)

        if validated_observation:
            validated_observations.append(validated_observation)

    validated_observations.sort(key=lambda x: x.julian_date)

    return __identify_timeseries(validated_observations, validation_dict)
