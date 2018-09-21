"""
Copyright 2018 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import logging
import unittest

import numpy as np

from ikats.algo.rollmean.rollmean_computation import rollmean, Alignment, rollmean_tsuid, rollmean_ts_list, \
    rollmean_ds
from ikats.core.resource.api import IkatsApi

LOGGER = logging.getLogger()
# Log format
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')
# Create another handler that will redirect log entries to STDOUT
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setLevel(logging.DEBUG)
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)

# TS used for calculation
TS1_DATA = np.array([[np.float64(14879031000), 1.0],
                     [np.float64(14879032000), 2.0],
                     [np.float64(14879033000), 10.0],
                     [np.float64(14879034000), 3.0],
                     [np.float64(14879035000), 4.0],
                     [np.float64(14879036000), 5.0],
                     [np.float64(14879037000), 6.0],
                     [np.float64(14879038000), 7.0],
                     [np.float64(14879039000), 8.0],
                     [np.float64(14879040000), 9.0],
                     [np.float64(14879041000), 10.0]])


def gen_ts(ts_id):
    """
    Generate a TS in database used for test bench where id is defined

    :param ts_id: Identifier of the TS to generate (see content below for the structure)
    :type ts_id: int

    :return: the TSUID and funcId
    :rtype: dict
    """

    # Build TS identifier
    fid = "UNIT_TEST_Rollmean_%s" % ts_id

    if ts_id == 1:
        ts_content = TS1_DATA
    elif ts_id == 2:
        ts_content = [
            [14879030000, 5.0],
            [14879031000, 6.0],
            [14879032000, 8.0],
            [14879033000, -15.0],
            [14879034000, 2.0],
            [14879035000, 6.0],
            [14879036000, 3.0],
            [14879037000, 2.0],
            [14879038000, 42.0],
            [14879039000, 8.0]
        ]
    elif ts_id == 3:
        ts_content = [
            [14879030500, 5.0],
            [14879031000, 6.0],
            [14879032000, 8.0],
            [14879033000, 7.0],
            [14879034000, 9.0],
            [14879037000, 9.0],
            [14879039000, 9.0],
            [14879041000, 9.0],
            [14879043000, 9.0],
            [14879044500, 10.0]
        ]
    else:
        raise NotImplementedError

    # Create the time series
    result = IkatsApi.ts.create(fid=fid, data=np.array(ts_content))
    IkatsApi.md.create(tsuid=result['tsuid'], name="qual_ref_period", value=1000, force_update=True)
    IkatsApi.md.create(tsuid=result['tsuid'], name="qual_nb_points", value=len(ts_content), force_update=True)
    IkatsApi.md.create(tsuid=result['tsuid'], name="ikats_start_date", value=int(ts_content[0][0]), force_update=True)
    IkatsApi.md.create(tsuid=result['tsuid'], name="ikats_end_date", value=int(ts_content[-1][0]), force_update=True)
    IkatsApi.md.create(tsuid=result['tsuid'], name="funcId", value="fid_%s" % ts_id, force_update=True)
    if not result['status']:
        raise SystemError("Error while creating TS %s" % ts_id)

    return {"tsuid": result['tsuid'], "funcId": fid}


class TestRollmean(unittest.TestCase):
    """
    Test the rollmean operator
    """

    def test_rollmean_value(self):
        """
        Testing the result values of the rollmean operator
        """
        window_size = 2
        results = rollmean(TS1_DATA, window_size=window_size, alignment=Alignment.left)
        result = results.data[:, 1]
        self.assertEqual(result[0], 1.5)
        self.assertEqual(result[1], 6.0)
        self.assertEqual(result[2], 6.5)
        self.assertEqual(result[3], 3.5)
        self.assertEqual(result[4], 4.5)
        self.assertEqual(result[5], 5.5)
        self.assertEqual(result[6], 6.5)
        self.assertEqual(result[7], 7.5)
        self.assertEqual(result[8], 8.5)
        self.assertEqual(result[9], 9.5)

    def test_rollmean_window(self):
        """
        Testing the window size and period options of the rollmean operator
        """

        ts_info = gen_ts(1)
        ts_to_delete = []

        try:

            window_size = 6
            results_1 = rollmean_tsuid(tsuid=ts_info['tsuid'], window_size=window_size,
                                       alignment=Alignment.left)
            ts_to_delete.append({'tsuid': results_1[0]})
            results_1_data = IkatsApi.ts.read(results_1[0])[0]
            self.assertEqual(len(results_1_data), len(TS1_DATA) - window_size + 1)

            window_size = 3
            results_2 = rollmean_tsuid(tsuid=ts_info['tsuid'], window_size=window_size,
                                       alignment=Alignment.left)
            ts_to_delete.append({'tsuid': results_2[0]})
            results_2_data = IkatsApi.ts.read(results_2[0])[0]
            self.assertEqual(len(results_2_data), len(TS1_DATA) - window_size + 1)

            window_period = 3000
            results_3 = rollmean_tsuid(tsuid=ts_info['tsuid'], window_period=window_period,
                                       alignment=Alignment.left)
            ts_to_delete.append({'tsuid': results_3[0]})
            results_3_data = IkatsApi.ts.read(results_3[0])[0]
            self.assertEqual(len(results_3_data), len(TS1_DATA) - window_period / 1000 + 1)

            window_period = 6000
            results_4 = rollmean_tsuid(tsuid=ts_info['tsuid'], window_period=window_period,
                                       alignment=Alignment.left)
            ts_to_delete.append({'tsuid': results_4[0]})
            results_4_data = IkatsApi.ts.read(results_4[0])[0]
            self.assertEqual(len(results_4_data), len(TS1_DATA) - window_period / 1000 + 1)

            self.assertTrue(np.array_equal(results_1_data, results_4_data))

        finally:
            # Clean up database
            self.clean_up_db([ts_info])
            self.clean_up_db(ts_to_delete)

    @staticmethod
    def clean_up_db(ts_info):
        """
        Clean up the database by removing created TS
        :param ts_info: list of TS to remove
        """
        for ts_item in ts_info:
            # Delete created TS
            IkatsApi.ts.delete(tsuid=ts_item['tsuid'], no_exception=True)

    # noinspection PyTypeChecker
    def test_rollmean_robustness(self):
        """
        Testing the robustness cases of the rollmean operator
        """

        ts_info = gen_ts(1)

        try:

            # Period and size set
            with self.assertRaises(ValueError):
                rollmean_tsuid(tsuid=ts_info['tsuid'], window_size=5, window_period=5, alignment=Alignment.left)

            # size = 0
            with self.assertRaises(ValueError):
                rollmean(TS1_DATA, window_size=0, alignment=Alignment.left)

            # size too big
            with self.assertRaises(ValueError):
                rollmean(TS1_DATA, window_size=len(TS1_DATA), alignment=Alignment.left)

            # size too big
            with self.assertRaises(ValueError):
                rollmean_tsuid(tsuid=ts_info['tsuid'], window_period=(TS1_DATA[-1][0] - TS1_DATA[0][0]),
                               alignment=Alignment.left)

            # wrong type
            with self.assertRaises(TypeError):
                rollmean_tsuid(tsuid=ts_info['tsuid'], window_size='wrong_type', alignment=Alignment.left)

            # wrong type
            with self.assertRaises(TypeError):
                rollmean_tsuid(tsuid=ts_info['tsuid'], window_period='wrong_type', alignment=Alignment.left)

            # wrong type
            with self.assertRaises(TypeError):
                rollmean_tsuid(tsuid=ts_info['tsuid'], window_period=500, alignment="wrong_type")

            # wrong value
            with self.assertRaises(TypeError):
                rollmean(TS1_DATA, window_size=5, alignment=0)

            # wrong value
            with self.assertRaises(TypeError):
                rollmean(TS1_DATA, window_size=5, alignment=4)

            # window_size > ts size
            with self.assertRaises(ValueError):
                rollmean(TS1_DATA, window_size=50, alignment=1)

        finally:
            # Clean up database
            self.clean_up_db([ts_info])

    def test_rollmean_ts_list(self):
        """
        Testing the rollmean operator from ts list
        """

        ts_info = gen_ts(1)
        try:
            window_size = 10
            results = rollmean_ts_list(ts_list=[ts_info['tsuid']], window_size=window_size,
                                       alignment=Alignment.left)
            self.assertEqual(len(results), 1)

        finally:
            # Clean up database
            self.clean_up_db(results)
            self.clean_up_db([ts_info])

    def test_rollmean_ds_spark(self):
        """
        Testing the rollmean operator from dataset
        """
        # TS used for calculation
        ts_info = gen_ts(1)
        tsuid = ts_info['tsuid']
        IkatsApi.ds.create("ds_test", "", [tsuid])

        results = None
        try:
            window_size = 2
            results = rollmean_ds(ds_name='ds_test', window_size=window_size,
                                  alignment=Alignment.left, nb_points_by_chunk=5)
            result = IkatsApi.ts.read(results[0]['tsuid'])[0][:, 1]

            self.assertEqual(result[0], 1.5)
            self.assertEqual(result[1], 6.0)
            self.assertEqual(result[2], 6.5)
            self.assertEqual(result[3], 3.5)
            self.assertEqual(result[4], 4.5)
            self.assertEqual(result[5], 5.5)
            self.assertEqual(result[6], 6.5)
            self.assertEqual(result[7], 7.5)
            self.assertEqual(result[8], 8.5)
            self.assertEqual(result[9], 9.5)

        finally:
            # Clean up database
            IkatsApi.ds.delete("ds_test", True)
            if results:
                self.clean_up_db(results)

    def test_rollmean_ds_alignment(self):
        """
        Testing the rollmean operator from dataset / alignment functionality
        """

        # TS used for calculation
        ts_info = gen_ts(1)
        tsuid = ts_info['tsuid']
        IkatsApi.ds.create("ds_test", "", [tsuid])
        ts_to_delete = []

        try:
            for number_of_points_in_a_chunk in [2, 15]:
                for window_size in [1, 2, 10]:
                    # Alignment.left
                    results_1 = rollmean_ds(ds_name='ds_test', window_size=window_size,
                                            alignment=Alignment.left, nb_points_by_chunk=number_of_points_in_a_chunk)
                    ts_to_delete.append({'tsuid': results_1[0]})
                    results_1_data = IkatsApi.ts.read(results_1[0]['tsuid'])[0]
                    self.assertEqual(len(results_1_data), len(TS1_DATA) - window_size + 1)
                    self.assertEqual(results_1_data[0][0], TS1_DATA[0][0])

                    # Alignment.right
                    results_2 = rollmean_ds(ds_name='ds_test', window_size=window_size,
                                            alignment=Alignment.right, nb_points_by_chunk=number_of_points_in_a_chunk)
                    ts_to_delete.append({'tsuid': results_2[0]})
                    results_2_data = IkatsApi.ts.read(results_2[0]['tsuid'])[0]
                    self.assertEqual(len(results_2_data), len(TS1_DATA) - window_size + 1)
                    self.assertEqual(results_2_data[-1][0], TS1_DATA[-1][0])

                    # Alignment.center
                    results_3 = rollmean_ds(ds_name='ds_test', window_size=window_size,
                                            alignment=Alignment.center, nb_points_by_chunk=number_of_points_in_a_chunk)
                    ts_to_delete.append({'tsuid': results_3[0]})
                    results_3_data = IkatsApi.ts.read(results_3[0]['tsuid'])[0]
                    self.assertEqual(len(results_3_data), len(TS1_DATA) - window_size + 1)
                    self.assertEqual(results_3_data[0][0], TS1_DATA[int((window_size - 1) / 2)][0])

                    # alignment = 2 (center)
                    results_4 = rollmean_ds(ds_name='ds_test', window_size=window_size,
                                            alignment=2, nb_points_by_chunk=number_of_points_in_a_chunk)
                    ts_to_delete.append({'tsuid': results_4[0]})
                    results_4_data = IkatsApi.ts.read(results_4[0]['tsuid'])[0]
                    self.assertEqual(len(results_4_data), len(TS1_DATA) - window_size + 1)
                    self.assertEqual(results_4_data[0][0], TS1_DATA[int((window_size - 1) / 2)][0])

                    # check values
                    self.assertTrue(np.array_equal(results_1_data[:, 1], results_2_data[:, 1]))
                    self.assertTrue(np.array_equal(results_1_data[:, 1], results_3_data[:, 1]))
                    self.assertTrue(np.array_equal(results_3_data[:, 1], results_4_data[:, 1]))
                    # check timestamps
                    self.assertTrue(np.array_equal(results_3_data[:, 0], results_4_data[:, 0]))

        finally:
            # Clean up database
            IkatsApi.ds.delete("ds_test", True)
            self.clean_up_db(ts_to_delete)

    # noinspection PyTypeChecker
    def test_spark_rollmean_robustness(self):
        """
        Testing the robustness cases of the rollmean operator
        """

        # TS used for calculation
        ts_info = gen_ts(1)
        tsuid = ts_info['tsuid']
        IkatsApi.ds.create("ds_test", "", [tsuid])

        try:
            # window_size > ts size
            with self.assertRaises(ValueError):
                rollmean_ds(ds_name='ds_test', window_size=12,
                            alignment=Alignment.left, nb_points_by_chunk=5)

        finally:
            # Clean up database
            IkatsApi.ds.delete("ds_test", True)
