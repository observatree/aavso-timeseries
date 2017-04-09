from unittest import TestCase

from processing import identify_timeseries, Observation

# Observations 1, 5 and 9 are not part of any timeseries.
# Observations 2, 3 and 4 should be grouped into timeseries 1.
# Observations 7, 8 and 9 should be grouped into timeseries 2.

TEST_OBSERVATIONS = [
    Observation(unique_id=1, julian_date=None, julian_date_string="15.00"),
    Observation(unique_id=2, julian_date=None, julian_date_string="16.05"),
    Observation(unique_id=3, julian_date=None, julian_date_string="16.06"),
    Observation(unique_id=4, julian_date=None, julian_date_string="16.07"),
    Observation(unique_id=5, julian_date=None, julian_date_string="17.00"),
    Observation(unique_id=6, julian_date=None, julian_date_string="18.85"),
    Observation(unique_id=7, julian_date=None, julian_date_string="18.86"),
    Observation(unique_id=8, julian_date=None, julian_date_string="18.87"),
    Observation(unique_id=9, julian_date=None, julian_date_string="19.00"),
    Observation(unique_id=981, julian_date=None, julian_date_string="20.00"),
    Observation(unique_id=982, julian_date=None, julian_date_string="20.00"),
]


class TestIdentifyTimeseries(TestCase):
    def test_identify_timeseries(self):
        timeseries = identify_timeseries(TEST_OBSERVATIONS)
        self.assertEquals(len(timeseries), 2, "There should be two time series in the dictionary.")
        first_timeseries = timeseries[2]
        self.assertEquals(len(first_timeseries), 3, "There should be three observations in the first time series.")
        second_timeseries = timeseries[6]
        self.assertEquals(len(second_timeseries), 3, "There should be three observations in the second time series.")
        # Spot check the time series
        self.assertEquals(16.07, first_timeseries[2].julian_date, "Test the third observation in the first series.")
        self.assertEquals(18.86, second_timeseries[1].julian_date, "Test the second observation in the second series.")
