import unittest
import os
import pandas as pd
from gemini import call_gemini_with_attachment, call_gemini_with_csv, PIData

class TestGeminiIntegration(unittest.TestCase):

    def test_extract_pii_from_csv(self):
        """
        Tests the extraction of PII from a specific CSV file on GCS.
        This is an integration test and makes a real API call.
        """
        # --- ARRANGE ---
        url = "gs://test-bucket22222222/expense.csv"
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

    def test_call_gemini_with_csv(self):
        """
        Tests the call_gemini_with_csv function with a local CSV file.
        """
        # --- ARRANGE ---
        csv_path = './shared/mes.csv'
        self.assertTrue(os.path.exists(csv_path), f"CSV file not found at {csv_path}")
        
        df = pd.read_csv(csv_path)

        expected_pii = {
            "phone_number": "010-9887-2211",
            "name": "오일석",
            "social_security_number": "870929-1231717"
        }

        # --- ACT ---
        results = call_gemini_with_csv(df)

        # --- ASSERT ---
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0, "Expected to find at least one PII record.")
        
        found_match = False
        for person in results:
            self.assertIsInstance(person, PIData)
            if (person.name == expected_pii["name"] and
                person.phone_number == expected_pii["phone_number"] and
                person.social_security_number == expected_pii["social_security_number"]):
                found_match = True
                break
        
        self.assertTrue(found_match, f"Expected PII record not found in results: {results}")

if __name__ == '__main__':
    unittest.main()
