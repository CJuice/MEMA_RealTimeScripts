"""
Trying to implement some testing for these scripts but since I have them in main() calls accessing functions is an issue
"""
import unittest
import doit_HospitalStatus  # Says there is an error importing this but it actually gets used in the test runs so ??


class TestHospitalStatus(unittest.TestCase):
    """"""
    def test_add(self):
        """
        just made a quick add() function in doit_HospitalStatus to see if test my unittest use, since it was new to me.
        it worked as long as add() was outside/same level as main(). can't access inner functions of main() so can't
        write tests against module with if __name__ == "__main__": design.
        Would have to write tests inside of main().
        :return:
        """
        result = doit_HospitalStatus.add(10, 5)
        self.assertEqual(result, 15)


if __name__ == "__main__":
    unittest.main()
