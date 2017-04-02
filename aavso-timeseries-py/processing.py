
import collections


Observation = collections.namedtuple("Observation", "unique_id julian_date_string julian_date timeseries")


# noinspection PyUnusedLocal


# It is presumed that identify_timeseries will be called with a list of records that are
# all from the same observer and the same star. Therefore all that identify_timeseries has
# to do is examine the Julian

def identify_timeseries(observations: [Observation]):
    print("{} observations".format(len(observations)))
    return
