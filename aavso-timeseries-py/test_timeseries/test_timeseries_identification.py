from unittest import TestCase

from timeseries_identification import identify_timeseries, Observation

TEST_OBSERVATIONS = [
    # FAIL DUE TO GAP FROM 112 TO 113
    Observation(unique_id=101, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.001"),
    Observation(unique_id=102, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.002"),
    Observation(unique_id=103, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.003"),
    Observation(unique_id=104, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.004"),
    Observation(unique_id=105, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.005"),
    Observation(unique_id=106, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.006"),
    Observation(unique_id=107, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.007"),
    Observation(unique_id=108, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.008"),
    Observation(unique_id=109, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.009"),
    Observation(unique_id=110, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.010"),
    Observation(unique_id=111, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.011"),
    Observation(unique_id=112, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="15.012"),
    Observation(unique_id=113, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.013"),
    Observation(unique_id=114, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.014"),
    Observation(unique_id=115, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.015"),
    Observation(unique_id=116, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.016"),
    Observation(unique_id=117, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.017"),
    Observation(unique_id=118, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.018"),
    Observation(unique_id=119, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.019"),
    Observation(unique_id=120, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.020"),
    Observation(unique_id=121, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.021"),
    Observation(unique_id=122, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.022"),
    Observation(unique_id=123, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.023"),
    Observation(unique_id=124, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="16.024"),
    # VALID TIME SERIES
    Observation(unique_id=201, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.001"),
    Observation(unique_id=202, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.002"),
    Observation(unique_id=203, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.003"),
    Observation(unique_id=204, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.004"),
    Observation(unique_id=205, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.005"),
    Observation(unique_id=206, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.006"),
    Observation(unique_id=207, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.007"),
    Observation(unique_id=208, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.008"),
    Observation(unique_id=209, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.009"),
    Observation(unique_id=210, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.010"),
    Observation(unique_id=211, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.011"),
    Observation(unique_id=212, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.012"),
    Observation(unique_id=213, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.013"),
    Observation(unique_id=214, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.014"),
    Observation(unique_id=215, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.015"),
    Observation(unique_id=216, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.016"),
    Observation(unique_id=217, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.017"),
    Observation(unique_id=218, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.018"),
    Observation(unique_id=219, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.019"),
    Observation(unique_id=220, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.020"),
    Observation(unique_id=221, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.021"),
    Observation(unique_id=222, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.022"),
    Observation(unique_id=223, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.023"),
    Observation(unique_id=224, name="TZ BOO", obscode="HBRB", julian_date=None, julian_date_string="17.024")
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
