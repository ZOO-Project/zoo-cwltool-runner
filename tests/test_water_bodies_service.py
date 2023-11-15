import unittest

from water_bodies.service import water_bodies  # noqa


class TestWaterBodiesService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class ZooStub(object):
            def __init__(self):
                self.SERVICE_SUCCEEDED = 3
                self.SERVICE_FAILED = 4

            def update_status(self, conf, progress):
                print(f"Status {progress}")

            def _(self, message):
                print(f"invoked _ with {message}")

        try:
            import zoo  # noqa
        except ImportError:
            print("Not running in zoo instance")

            zoo = ZooStub()

        cls.zoo = zoo

        conf = {}
        conf["lenv"] = {"message": ""}
        conf["lenv"] = {"Identifier": "water_bodies"}
        conf["tmpPath"] = "/tmp"

        cls.conf = conf

        inputs = {
            "aoi": {"value": "-121.399,39.834,-120.74,40.472"},
            "bands": {"value": ["green", "nir"]},
            "epsg": {"value": "EPSG:4326"},
            "stac_items": {
                "value": [
                    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2A_10TFK_20210708_0_L2A",  # noqa
                ]
            },
        }

        cls.inputs = inputs

        outputs = {"Result": {"value": ""}}

        cls.outputs = outputs

    def test_execution(self):
        exit_code = water_bodies(conf=self.conf, inputs=self.inputs, outputs=self.outputs)

        self.assertEqual(exit_code, self.zoo.SERVICE_SUCCEEDED)
