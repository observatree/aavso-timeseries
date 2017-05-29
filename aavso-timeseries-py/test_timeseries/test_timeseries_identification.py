from unittest import TestCase

from timeseries_identification import identify_timeseries, Observation

# Observations 1, 5 and 9 are not part of any timeseries.
# Observations 2, 3 and 4 should be grouped into timeseries 1.
# Observations 7, 8 and 9 should be grouped into timeseries 2.

TEST_OBSERVATIONS = [
    # FAIL DUE TO GAP FROM 112 TO 113
    Observation(unique_id=101, julian_date=None, julian_date_string="15.001"),
    Observation(unique_id=102, julian_date=None, julian_date_string="15.002"),
    Observation(unique_id=103, julian_date=None, julian_date_string="15.003"),
    Observation(unique_id=104, julian_date=None, julian_date_string="15.004"),
    Observation(unique_id=105, julian_date=None, julian_date_string="15.005"),
    Observation(unique_id=106, julian_date=None, julian_date_string="15.006"),
    Observation(unique_id=107, julian_date=None, julian_date_string="15.007"),
    Observation(unique_id=108, julian_date=None, julian_date_string="15.008"),
    Observation(unique_id=109, julian_date=None, julian_date_string="15.009"),
    Observation(unique_id=110, julian_date=None, julian_date_string="15.010"),
    Observation(unique_id=111, julian_date=None, julian_date_string="15.011"),
    Observation(unique_id=112, julian_date=None, julian_date_string="15.012"),
    Observation(unique_id=113, julian_date=None, julian_date_string="16.013"),
    Observation(unique_id=114, julian_date=None, julian_date_string="16.014"),
    Observation(unique_id=115, julian_date=None, julian_date_string="16.015"),
    Observation(unique_id=116, julian_date=None, julian_date_string="16.016"),
    Observation(unique_id=117, julian_date=None, julian_date_string="16.017"),
    Observation(unique_id=118, julian_date=None, julian_date_string="16.018"),
    Observation(unique_id=119, julian_date=None, julian_date_string="16.019"),
    Observation(unique_id=120, julian_date=None, julian_date_string="16.020"),
    Observation(unique_id=121, julian_date=None, julian_date_string="16.021"),
    Observation(unique_id=122, julian_date=None, julian_date_string="16.022"),
    Observation(unique_id=123, julian_date=None, julian_date_string="16.023"),
    Observation(unique_id=124, julian_date=None, julian_date_string="16.024"),
    # VALID TIME SERIES
    Observation(unique_id=201, julian_date=None, julian_date_string="17.001"),
    Observation(unique_id=202, julian_date=None, julian_date_string="17.002"),
    Observation(unique_id=203, julian_date=None, julian_date_string="17.003"),
    Observation(unique_id=204, julian_date=None, julian_date_string="17.004"),
    Observation(unique_id=205, julian_date=None, julian_date_string="17.005"),
    Observation(unique_id=206, julian_date=None, julian_date_string="17.006"),
    Observation(unique_id=207, julian_date=None, julian_date_string="17.007"),
    Observation(unique_id=208, julian_date=None, julian_date_string="17.008"),
    Observation(unique_id=209, julian_date=None, julian_date_string="17.009"),
    Observation(unique_id=210, julian_date=None, julian_date_string="17.010"),
    Observation(unique_id=211, julian_date=None, julian_date_string="17.011"),
    Observation(unique_id=212, julian_date=None, julian_date_string="17.012"),
    Observation(unique_id=213, julian_date=None, julian_date_string="17.013"),
    Observation(unique_id=214, julian_date=None, julian_date_string="17.014"),
    Observation(unique_id=215, julian_date=None, julian_date_string="17.015"),
    Observation(unique_id=216, julian_date=None, julian_date_string="17.016"),
    Observation(unique_id=217, julian_date=None, julian_date_string="17.017"),
    Observation(unique_id=218, julian_date=None, julian_date_string="17.018"),
    Observation(unique_id=219, julian_date=None, julian_date_string="17.019"),
    Observation(unique_id=220, julian_date=None, julian_date_string="17.020"),
    Observation(unique_id=221, julian_date=None, julian_date_string="17.021"),
    Observation(unique_id=222, julian_date=None, julian_date_string="17.022"),
    Observation(unique_id=223, julian_date=None, julian_date_string="17.023"),
    Observation(unique_id=224, julian_date=None, julian_date_string="17.024")
]


class TestIdentify(TestCase):
    def test_identify_timeseries(self):
        validation_dict = dict()
        timeseries = identify_timeseries(TEST_OBSERVATIONS, validation_dict)
        self.assertEqual(len(timeseries), 1, "There should be one time series in the dictionary.")
        first_timeseries = timeseries[201]
        self.assertEqual(len(first_timeseries), 24, "There should be 24 observations in the time series.")
        # Spot check the time series
        self.assertEqual(17.003, first_timeseries[2].julian_date, "Test the third observation in the time series.")
