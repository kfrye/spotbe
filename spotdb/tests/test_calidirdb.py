from spotdb.calidirdb import SpotCaliperDirectoryDB

import unittest

class CaliDirDBTest(unittest.TestCase):
    def setUp(self):
        self.db = SpotCaliperDirectoryDB("spotdb/tests/data/lulesh_timeseries")
    
        runs = self.db.get_all_run_ids()
        self.db.get_global_data(runs)


    def test_calidirdb_metadata(self):        
        g = self.db.get_global_attribute_metadata()

        self.assertEqual(g["launchdate"     ]["type"], "date")
        self.assertEqual(g["problem_size"   ]["type"], "int")
        self.assertEqual(g["figure_of_merit"]["type"], "double")

        m = self.db.get_metric_attribute_metadata()

        self.assertEqual(m["avg#inclusive#sum#time.duration"]["type"], "double")
        self.assertEqual(m["avg#inclusive#sum#time.duration"]["unit"], "sec")
        self.assertEqual(m["avg#loop.iterations/time.duration"]["type"],  "double")
        self.assertEqual(m["avg#loop.iterations/time.duration"]["alias"], "Iter/s")


    def test_calidirdb_runs(self):
        runs = self.db.get_all_run_ids()

        self.assertTrue(len(runs) > 4)
        runs = runs[0:4]

        g = self.db.get_global_data(runs)

        self.assertSetEqual(set(runs), set(g.keys()))

        for r in runs:
            self.assertIn("launchdate", g[r])
            self.assertIn("figure_of_merit", g[r])
            self.assertEqual(g[r]["cluster"], "quartz")
        
        p = self.db.get_regionprofiles(runs)

        self.assertSetEqual(set(runs), set(p.keys()))

        for r in runs:
            self.assertIn("main/lulesh.cycle/LagrangeLeapFrog", p[r])
            self.assertIn("avg#inclusive#sum#time.duration", p[r]["main/lulesh.cycle/LagrangeLeapFrog"])


    def test_calidirdb_timeseries(self):
        runs = self.db.get_all_run_ids()

        self.assertTrue(len(runs) > 4)
        runs = runs[0:4]

        g = self.db.get_global_data(runs)
        t = self.db.get_channel_data("timeseries", runs)

        self.assertSetEqual(set(runs), set(t.keys()))

        for r in runs:
            self.assertEqual(g[r]["timeseries"], 1)
            self.assertTrue(len(t[r]) > 0)
            self.assertIn("block", t[r][0])
            self.assertIn("avg#loop.iterations/time.duration", t[r][0])


if __name__ == "__main__":
    unittest.main()
