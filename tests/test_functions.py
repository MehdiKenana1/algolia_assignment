import logging
import unittest
from unittest.mock import MagicMock
from io import StringIO

import pandas as pd
from airflow.exceptions import AirflowSkipException

from dags.etl_dag import extract_csv_from_s3, transform_data


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_DB = "algolia_wh"
POSTGRES_USER = "alglia_user"
POSTGRES_PASSWORD = "algolia_pwd"
POSTGRES_HOST = "warehouse"
POSTGRES_PORT = "5432"


class MyTestCase(unittest.TestCase):
    def test_extract_csv_from_s3_success(self):
        """
        Test successffl extract of CSV file
        """
        logger.info("Testing successful extraction from S3")
        mock_context = {"ti": MagicMock()}
        ds = "2019-04-01"

        try:
            extract_csv_from_s3(ds, **mock_context)
            extracted_csv = mock_context["ti"].xcom_push.call_args[1]["value"]
            exact_df = pd.read_csv(filepath_or_buffer="tests/2019-04-01.csv")
            extracted_df = pd.read_csv(StringIO(extracted_csv))

            pd.testing.assert_frame_equal(extracted_df, exact_df)
            logger.info("Successful extraction test passed")
        except Exception as e:
            logger.error(f"Error during test_extract_csv_from_s3_success: {str(e)}")
            raise


    def test_extract_csv_from_s3_no_such_key(self):
        """
        Test error handling with missing key in S3
        """
        logger.info("Testing extraction from S3 with missing key")
        mock_context = {"ti": MagicMock()}
        ds = "2019-04-03"

        with self.assertRaises(AirflowSkipException):
            extract_csv_from_s3(ds, **mock_context)
        logger.info("Successfully handled missing key with AirflowSkipException")


    def test_transform_data(self):
        """
        Test successufl transformation of data
        """
        logger.info("Testing data transformation")
        mock_data = pd.DataFrame(
            {
                "application_id": ["1", "2", None],
                "index_prefix": ["shopify_", "other_prefix", "shopify_"],
            }
        )

        expected_data = pd.DataFrame(
            {
                "application_id": ["1", "2"],
                "index_prefix": ["shopify_", "other_prefix"],
                "has_specific_prefix": [False, True],
            }
        )

        context = {"ti": MagicMock()}
        context["ti"].xcom_pull.return_value = mock_data.to_csv(index=False)

        try:
            transform_data(**context)
            pushed_args = context["ti"].xcom_push.call_args
            assert pushed_args is not None

            transformed_data_csv = pushed_args[1]["value"]
            transformed_data = pd.read_csv(StringIO(transformed_data_csv))

            pd.testing.assert_frame_equal(transformed_data, expected_data)
            logger.info("Data transformation test passed")
        except Exception as e:
            logger.error(f"Error during test_transform_data: {str(e)}")
            raise


if __name__ == "__main__":
    unittest.main()
