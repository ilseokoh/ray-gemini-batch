import unittest
import os
from gemini import call_gemini_with_attachment, PIData

class TestGeminiIntegration(unittest.TestCase):

    def test_extract_pii_from_csv(self):
        """
        Tests the extraction of PII from a specific CSV file on GCS.
        This is an integration test and makes a real API call.
        """
        # --- ARRANGE ---
        url = "gs://kevin-step1-bucket/expense.csv"
        content_type = "text/csv"

        expected_pii = {
            "phone_number": "010-8723-0993",
            "name": "김택진",
            "email": "tkkim@gmail.com"
        }

        # --- ACT ---
        results = call_gemini_with_attachment(url, content_type)

        # --- ASSERT ---
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0, "Expected to find at least one PII record.")

        found_match = False
        for person in results:
            self.assertIsInstance(person, PIData)
            if (person.name == expected_pii["name"] and
                person.phone_number == expected_pii["phone_number"] and
                person.email == expected_pii["email"]):
                found_match = True
                break
        
        self.assertTrue(found_match, f"Expected PII record not found in results: {results}")

if __name__ == '__main__':
    unittest.main()
