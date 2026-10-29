import unittest

import flatbuffers
from fx_object_api import BBox2D, Detection, UBytePair


class ObjectApiAcronymStructTest(unittest.TestCase):
    def test_pack_round_trips_structs_with_consecutive_capitals(self):
        detection = Detection.DetectionT()
        detection.bbox = BBox2D.BBox2DT()
        detection.bbox.centerX, detection.bbox.centerY = 1.5, 2.5
        detection.bbox.width, detection.bbox.height = 3.0, 4.0
        detection.pair = UBytePair.UBytePairT()
        detection.pair.first, detection.pair.second = 7, 9
        second = BBox2D.BBox2DT()
        second.width = 8.0
        detection.boxes = [detection.bbox, second]

        builder = flatbuffers.Builder(0)
        builder.Finish(detection.Pack(builder))
        decoded = Detection.DetectionT.InitFromPackedBuf(builder.Output(), 0)

        self.assertEqual(
            (decoded.bbox.centerX, decoded.bbox.centerY, decoded.bbox.width, decoded.bbox.height),
            (1.5, 2.5, 3.0, 4.0),
        )
        self.assertEqual((decoded.pair.first, decoded.pair.second), (7, 9))
        self.assertEqual([box.width for box in decoded.boxes], [3.0, 8.0])

    def test_struct_creator_name_matches_object_api_call_site(self):
        self.assertTrue(callable(BBox2D.CreateBbox2D))
        self.assertTrue(callable(UBytePair.CreateUbytePair))


if __name__ == "__main__":
    unittest.main()
