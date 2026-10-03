import unittest
import ab


class CanonicalAnswersTest(unittest.TestCase):
    def test_unordered_enumeration_is_equal(self):
        a = b"{\"values\":[{\"value\":\"_time\",\"hits\":160},{\"value\":\"_msg\",\"hits\":160}]}"
        b = b"{\"values\":[{\"value\":\"_msg\",\"hits\":160},{\"value\":\"_time\",\"hits\":160}]}"
        self.assertEqual(ab.canon(a, True), ab.canon(b, True))
        self.assertNotEqual(ab.canon(a), ab.canon(b))

    def test_changed_counts_are_different(self):
        self.assertNotEqual(ab.canon(b"{\"values\":[{\"value\":\"_msg\",\"hits\":160}]}", True),
                            ab.canon(b"{\"values\":[{\"value\":\"_msg\",\"hits\":159}]}", True))

    def test_series_order_remains_significant(self):
        self.assertNotEqual(ab.canon(b"{\"timestamps\":[1,2]}"), ab.canon(b"{\"timestamps\":[2,1]}"))


if __name__ == "__main__":
    unittest.main()
